"""
Gold Layer - Fact Table de Órdenes.

=== GRAIN ===
Una fila por order_item (la unidad mínima de una venta).

=== POR QUÉ ESTE GRAIN? ===
El grain define la granularidad de la tabla.
- Grain = orden      → perdemos info de qué productos/sellers estuvieron en la orden
- Grain = order_item → mantenemos todo el detalle, se puede agregar hacia arriba
                       (GROUP BY order_id para análisis a nivel orden)

=== JOINS Y SUS TIPOS ===
- INNER con orders:              solo items de órdenes entregadas y válidas
- INNER con customers:           toda orden tiene cliente (si no → dato corrupto)
- LEFT  con products:            producto puede haberse eliminado del catálogo
- LEFT  con sellers:             seller puede haberse dado de baja
- LEFT  con payments (agregado): precaución ante datos faltantes
- LEFT  con reviews  (agregado): review es opcional (no toda orden tiene review)
- LEFT  con geolocation:         no todos los zip codes tienen coordenadas

=== PRE-AGREGACIÓN DE PAYMENTS Y REVIEWS ===
Payments y reviews tienen múltiples filas por order_id.
Si se joinean directamente explotan los registros:
  1 orden con 3 items × 2 métodos de pago = 6 filas (INCORRECTO)

Solución: agregarlos a 1 fila por order_id ANTES del join.
  - payments → total_payment_value, tipo de pago dominante, cuotas
  - reviews  → review_score promedio, cantidad de reviews

=== MÉTRICAS DERIVADAS ===
No copiamos datos de Silver, los transformamos en métricas:
  - delivery_days:     días entre compra y entrega real
  - delay_days:        días respecto al plazo estimado (+ = tarde, - = anticipado)
  - is_late_delivery:  bool para análisis de calidad
  - year/month/day_of_week: features temporales para ML y series de tiempo
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    avg,
    coalesce,
    lit,
    current_timestamp,
    when,
    round as spark_round,
    datediff,
    year,
    month,
    dayofweek,
    row_number,
)
from pyspark.sql.window import Window
from delta import DeltaTable
from typing import Optional, Dict
import time

from spark_jobs.config.settings import config
from spark_jobs.utils.loggins_utils import get_logger

logger = get_logger(__name__)


class FactOrdersTable:
    """
    Construye la fact table de órdenes en la capa Gold.

    Grain: una fila por order_item.
    """

    def __init__(
        self,
        spark: SparkSession,
        silver_orders_path: Optional[str] = None,
        silver_order_items_path: Optional[str] = None,
        silver_products_path: Optional[str] = None,
        silver_payments_path: Optional[str] = None,
        silver_reviews_path: Optional[str] = None,
        silver_sellers_path: Optional[str] = None,
        silver_geolocation_path: Optional[str] = None,
        silver_customers_path: Optional[str] = None,
        gold_path: Optional[str] = None,
    ):
        self.spark = spark
        self.silver_orders_path = silver_orders_path or str(
            config.paths.delta_silver / "orders"
        )
        self.silver_order_items_path = silver_order_items_path or str(
            config.paths.delta_silver / "order_items"
        )
        self.silver_products_path = silver_products_path or str(
            config.paths.delta_silver / "products"
        )
        self.payments_path = silver_payments_path or str(
            config.paths.delta_silver / "orders_payments"
        )
        self.customers_path = silver_customers_path or str(
            config.paths.delta_silver / "customers"
        )
        self.reviews_path = silver_reviews_path or str(
            config.paths.delta_silver / "orders_reviews"
        )
        self.sellers_path = silver_sellers_path or str(
            config.paths.delta_silver / "sellers"
        )
        self.geolocation_path = silver_geolocation_path or str(
            config.paths.delta_silver / "geolocation"
        )
        self.gold_path = gold_path or str(config.paths.delta_gold / "fact_orders")

    # -------------------------------------------------------------------------
    # LECTURA
    # -------------------------------------------------------------------------

    def read_silver_tables(self) -> Dict[str, DataFrame]:
        """Lee las tablas Silver filtrando solo registros válidos."""
        logger.info("Leyendo tablas de Silver")

        # Solo órdenes entregadas y válidas
        orders = (
            self.spark.read.format("delta")
            .load(self.silver_orders_path)
            .filter(col("_is_valid") == True)
            .filter(col("order_status") == "delivered")
            .select(
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            )
        )

        items = (
            self.spark.read.format("delta")
            .load(self.silver_order_items_path)
            .filter(col("_is_valid") == True)
            .select(
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "shipping_limit_date",
                "price",
                "freight_value",
            )
        )

        customers = (
            self.spark.read.format("delta")
            .load(self.customers_path)
            .filter(col("_is_valid") == True)
            .select(
                "customer_id",
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
            )
        )

        products = (
            self.spark.read.format("delta")
            .load(self.silver_products_path)
            .filter(col("_is_valid") == True)
            .select(
                "product_id",
                "product_category_name",
                "product_weight_g",
            )
        )

        # Geolocation: una fila por zip code (ya deduplicada en Silver)
        geolocation = (
            self.spark.read.format("delta")
            .load(self.geolocation_path)
            .filter(col("_is_valid") == True)
            .select(
                col("geolocation_zip_code_prefix"),
                col("geolocation_lat").alias("customer_lat"),
                col("geolocation_lng").alias("customer_lng"),
            )
        )

        # Payments: múltiples filas por order_id → hay que agregar antes del join
        payments_raw = (
            self.spark.read.format("delta")
            .load(self.payments_path)
            .filter(col("_is_valid") == True)
            .select("order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value")
        )

        # Reviews: múltiples filas por order_id → hay que agregar antes del join
        reviews_raw = (
            self.spark.read.format("delta")
            .load(self.reviews_path)
            .filter(col("_is_valid") == True)
            .select("review_id", "order_id", "review_score", "review_comment_message")
        )

        sellers = (
            self.spark.read.format("delta")
            .load(self.sellers_path)
            .filter(col("_is_valid") == True)
            .select(
                "seller_id",
                "seller_city",
                "seller_state",
            )
        )

        return {
            "orders": orders,
            "items": items,
            "customers": customers,
            "products": products,
            "geolocation": geolocation,
            "payments_raw": payments_raw,
            "reviews_raw": reviews_raw,
            "sellers": sellers,
        }

    # -------------------------------------------------------------------------
    # PRE-AGREGACIONES
    # -------------------------------------------------------------------------

    def _aggregate_payments(self, payments: DataFrame) -> DataFrame:
        """
        Agrega payments a 1 fila por order_id.

        Por qué: payments tiene una fila por método de pago por orden.
        Si una orden se pagó con tarjeta + voucher, tiene 2 filas.
        Si se joinea directamente, se duplican los order_items.

        Resultado:
          - total_payment_value: suma de todos los pagos de la orden
          - payment_type: tipo de pago con mayor valor (dominante)
          - payment_installments: cuotas del pago dominante
        """
        # Pago dominante = el que tiene mayor valor en la orden
        w = Window.partitionBy("order_id").orderBy(col("payment_value").desc())
        dominant = (
            payments
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .select("order_id", "payment_type", "payment_installments")
        )

        totals = payments.groupBy("order_id").agg(
            spark_round(_sum("payment_value"), 2).alias("total_payment_value"),
        )

        return totals.join(dominant, on="order_id", how="inner")

    def _aggregate_reviews(self, reviews: DataFrame) -> DataFrame:
        """
        Agrega reviews a 1 fila por order_id.

        Por qué: una orden puede tener múltiples reviews.
        Si se joinea directamente, se duplican los order_items.

        Resultado:
          - review_score: promedio de scores (redondeado a 1 decimal)
          - review_count: cuántas reviews dejó el cliente
          - has_review_comment: si dejó algún comentario escrito
        """
        return reviews.groupBy("order_id").agg(
            spark_round(avg("review_score"), 1).alias("review_score"),
            count("review_id").alias("review_count"),
            (
                _sum(when(col("review_comment_message").isNotNull(), lit(1)).otherwise(lit(0))) > 0
            ).cast("boolean").alias("has_review_comment"),
        )

    # -------------------------------------------------------------------------
    # CONSTRUCCIÓN DE LA FACT TABLE
    # -------------------------------------------------------------------------

    def _build_fact(self, tables: Dict[str, DataFrame]) -> DataFrame:
        """
        Construye la fact table realizando todos los joins y métricas derivadas.

        Orden de joins:
        1. items + orders (INNER): base de la fact, solo ítems de órdenes entregadas
        2. + customers (INNER): enriquece con datos del comprador
        3. + products (LEFT): enriquece con categoría; LEFT porque producto puede no existir
        4. + sellers (LEFT): enriquece con ubicación del seller
        5. + payments agregado (LEFT): monto total y tipo de pago
        6. + reviews agregado (LEFT): score de satisfacción
        7. + geolocation (LEFT): coordenadas del cliente via zip code
        """
        payments = self._aggregate_payments(tables["payments_raw"])
        reviews = self._aggregate_reviews(tables["reviews_raw"])

        fact = (
            # 1. items + orders: grain = order_item de una orden entregada
            tables["items"]
            .join(tables["orders"], on="order_id", how="inner")

            # 2. + customers: customer_id está en orders
            .join(tables["customers"], on="customer_id", how="inner")

            # 3. + products: product_id está en items
            .join(tables["products"], on="product_id", how="left")

            # 4. + sellers: seller_id está en items
            .join(tables["sellers"], on="seller_id", how="left")

            # 5. + payments agregados: 1 fila por order_id
            .join(payments, on="order_id", how="left")

            # 6. + reviews agregados: 1 fila por order_id
            .join(reviews, on="order_id", how="left")

            # 7. + geolocation: el zip de customers se cruza con el zip de geolocation
            # No se puede usar on="columna" porque tienen nombres distintos
            .join(
                tables["geolocation"],
                on=col("customer_zip_code_prefix") == col("geolocation_zip_code_prefix"),
                how="left",
            )
            .drop("geolocation_zip_code_prefix")  # ya queda customer_zip_code_prefix
        )

        # Métricas derivadas
        fact = fact.withColumns({
            # Días entre compra y entrega real
            "delivery_days": datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp"),
            ),
            # Días de atraso respecto al plazo prometido
            # Positivo = llegó tarde, negativo = llegó antes
            "delay_days": datediff(
                col("order_delivered_customer_date"),
                col("order_estimated_delivery_date"),
            ),
            # Features temporales para series de tiempo y ML
            "purchase_year": year(col("order_purchase_timestamp")),
            "purchase_month": month(col("order_purchase_timestamp")),
            "purchase_day_of_week": dayofweek(col("order_purchase_timestamp")),
            # Metadata
            "_created_at": current_timestamp(),
        })

        # is_late se calcula después porque usa delay_days
        fact = fact.withColumn(
            "is_late_delivery",
            when(col("delay_days") > 0, True).otherwise(False),
        )

        return fact.select(
            # Claves
            "order_id",
            "order_item_id",
            "customer_id",
            "customer_unique_id",
            "product_id",
            "seller_id",
            # Fechas
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            # Financiero
            "price",
            "freight_value",
            "total_payment_value",
            "payment_type",
            "payment_installments",
            # Logística
            "delivery_days",
            "delay_days",
            "is_late_delivery",
            # Temporales
            "purchase_year",
            "purchase_month",
            "purchase_day_of_week",
            # Dimensiones denormalizadas
            "product_category_name",
            "product_weight_g",
            "customer_city",
            "customer_state",
            "customer_zip_code_prefix",
            "customer_lat",
            "customer_lng",
            "seller_city",
            "seller_state",
            # Satisfacción
            "review_score",
            "review_count",
            "has_review_comment",
            # Metadata
            "_created_at",
        )

    # -------------------------------------------------------------------------
    # ESCRITURA
    # -------------------------------------------------------------------------

    def _write_gold(self, df: DataFrame) -> int:
        """
        Escribe la fact table en Gold con overwrite.

        Por qué overwrite y no MERGE:
        La fact table se reconstruye completa desde Silver en cada ejecución.
        No tiene sentido hacer MERGE fila por fila en 100k+ registros.
        Overwrite es más simple, más rápido y el resultado es idempotente.
        """
        logger.info(f"Escribiendo fact table en {self.gold_path}")
        count = df.count()
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(self.gold_path)
        )
        logger.info(f"Escritos {count:,} registros en Gold")
        return count

    # -------------------------------------------------------------------------
    # EJECUCIÓN
    # -------------------------------------------------------------------------

    def run(self) -> dict:
        """Ejecuta el pipeline completo y devuelve métricas."""
        start = time.time()
        logger.info("=" * 50)
        logger.info("Iniciando construcción de fact_orders (Gold)")

        tables = self.read_silver_tables()
        fact = self._build_fact(tables)
        total = self._write_gold(fact)

        duration = round(time.time() - start, 2)
        logger.info(f"Completado en {duration}s")

        return {
            "status": "success",
            "total_records": total,
            "duration_seconds": duration,
            "gold_path": self.gold_path,
        }


# =============================================================================
# SCRIPT DE EJECUCIÓN
# =============================================================================

if __name__ == "__main__":
    from spark_jobs.utils.spark_utils import get_spark_session

    print("=" * 60)
    print("GOLD LAYER - FACT TABLE DE ÓRDENES")
    print("=" * 60)

    spark = get_spark_session("GoldFactOrders")

    try:
        fact_table = FactOrdersTable(spark)
        result = fact_table.run()

        print(f"\n  status:   {result['status']}")
        print(f"  records:  {result['total_records']:,}")
        print(f"  duration: {result['duration_seconds']}s")
        print(f"  path:     {result['gold_path']}")
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
    finally:
        spark.stop()
