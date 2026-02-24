"""
Gold Layer - Aggregate Tables.

=== PROPÓSITO ===
Las tablas aggregate responden preguntas de negocio concretas.
A diferencia de la fact table (grain = order_item), estas tablas
están pre-calculadas y son directamente consumibles por dashboards,
reportes o como features para ML.

=== TABLAS ===

1. AggSellerPerformance (gold/agg_seller_performance)
   ¿Qué sellers rinden mejor?
   Grain: una fila por seller.

2. AggCustomerLTV (gold/agg_customer_ltv)
   ¿Cuánto vale cada cliente? = Feature store para ML.
   Grain: una fila por customer_unique_id.

3. AggMonthlyMetrics (gold/agg_monthly_metrics)
   ¿Cómo evoluciona el negocio mes a mes?
   Grain: una fila por año-mes.

=== FUENTE ===
Todas leen de gold/fact_orders (ya construida por FactOrdersTable).
No vuelven a Silver — la fact table ya hizo los JOINs pesados.

=== ESCRITURA ===
Todas usan overwrite: se recalculan completas en cada ejecución.
Igual que la fact table — idempotente y simple.
"""

import time
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    countDistinct,
    avg,
    max as _max,
    min as _min,
    round as spark_round,
    when,
    lit,
    datediff,
    current_timestamp,
    percentile_approx,
    row_number,
    date_format,
)
from pyspark.sql.window import Window

from spark_jobs.config.settings import config
from spark_jobs.utils.loggins_utils import get_logger

logger = get_logger(__name__)


# =============================================================================
# BASE CLASS
# =============================================================================

class BaseAggTable:
    """Base para todas las tablas aggregate."""

    def __init__(self, spark: SparkSession, fact_path: Optional[str] = None, gold_path: Optional[str] = None):
        self.spark = spark
        self.fact_path = fact_path or str(config.paths.delta_gold / "fact_orders")
        self.gold_path = gold_path  # cada subclase define el default

    def read_fact(self) -> DataFrame:
        logger.info(f"Leyendo fact_orders desde {self.fact_path}")
        return self.spark.read.format("delta").load(self.fact_path)

    def _write(self, df: DataFrame) -> int:
        n = df.count()
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(self.gold_path)
        )
        logger.info(f"Escritos {n:,} registros en {self.gold_path}")
        return n

    def run(self) -> dict:
        raise NotImplementedError


# =============================================================================
# 1. SELLER PERFORMANCE
# =============================================================================

class AggSellerPerformance(BaseAggTable):
    """
    Métricas de performance por seller.

    Responde: ¿Quiénes son los mejores y peores vendedores?
    Útil para: ranking de sellers, detección de fraude, bonificaciones.

    Grain: una fila por seller_id.  
    """

    def __init__(self, spark: SparkSession, fact_path: Optional[str] = None, gold_path: Optional[str] = None):
        super().__init__(spark, fact_path, gold_path)
        self.gold_path = gold_path or str(config.paths.delta_gold / "agg_seller_performance")

    def build(self, fact: DataFrame) -> DataFrame:
        return (
            fact.groupBy("seller_id", "seller_city", "seller_state")
            .agg(
                # Revenue: precio del ítem + flete
                spark_round(_sum(col("price") + col("freight_value")), 2).alias("total_revenue"),
                # Órdenes distintas (un seller puede tener varios items en una orden)
                countDistinct("order_id").alias("total_orders"),
                # Total de ítems vendidos
                count("order_item_id").alias("total_items_sold"),
                # Ticket promedio por ítem
                spark_round(avg("price"), 2).alias("avg_item_price"),
                # Review score promedio recibido
                spark_round(avg("review_score"), 2).alias("avg_review_score"),
                # % de entregas a tiempo (is_late_delivery = False)
                spark_round(
                    avg(when(col("is_late_delivery") == False, lit(1.0)).otherwise(lit(0.0))) * 100, 1
                ).alias("on_time_delivery_pct"),
                # Días promedio de entrega
                spark_round(avg("delivery_days"), 1).alias("avg_delivery_days"),
                # Días promedio de atraso (solo tardanzas)
                spark_round(
                    avg(when(col("delay_days") > 0, col("delay_days"))), 1
                ).alias("avg_delay_days_when_late"),
            )
            .withColumn("_created_at", current_timestamp())
        )

    def run(self) -> dict:
        start = time.time()
        logger.info("Construyendo agg_seller_performance")
        fact = self.read_fact()
        df = self.build(fact)
        n = self._write(df)
        return {"status": "success", "table": "agg_seller_performance", "records": n, "duration_seconds": round(time.time() - start, 2)}


# =============================================================================
# 2. CUSTOMER LTV (Feature Store para ML)
# =============================================================================

class AggCustomerLTV(BaseAggTable):
    """
    Métricas de Lifetime Value por cliente.

    Responde: ¿Cuánto vale cada cliente y cuáles son sus patrones de compra?
    Útil para: segmentación, retención, predicción de churn, feed de ML.

    Grain: una fila por customer_unique_id.

    === RFM ===
    Recency:  ¿Cuándo compró por última vez?
    Frequency: ¿Cuántas veces compró?
    Monetary:  ¿Cuánto gastó en total?

    RFM es el estándar de la industria para segmentación de clientes.
    Los tres juntos capturan el valor pasado y el potencial futuro.

    === LTV SEGMENT ===
    Se calcula con percentiles de monetary:
      - High:   >= percentil 75  (top 25% por gasto)
      - Medium: percentil 25-75
      - Low:    < percentil 25   (bottom 25%)

    Este campo ES el target variable para el modelo de ML.
    """

    def __init__(self, spark: SparkSession, fact_path: Optional[str] = None, gold_path: Optional[str] = None):
        super().__init__(spark, fact_path, gold_path)
        self.gold_path = gold_path or str(config.paths.delta_gold / "agg_customer_ltv")


    def _get_preferred(self, fact: DataFrame, group_col: str, value_col: str, alias: str) -> DataFrame:
        """
        Para cada cliente, obtiene el valor más frecuente de value_col.
        Ejemplo: preferred_category = categoría que más veces compró.
        """
        counts = (
            fact.groupBy(group_col, value_col)
            .agg(count("*").alias("_cnt"))
        )
        w = Window.partitionBy(group_col).orderBy(col("_cnt").desc())
        return (
            counts
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .select(col(group_col), col(value_col).alias(alias))
            )
    def build(self, fact: DataFrame) -> DataFrame:
        # Fecha de referencia = máxima fecha del dataset (simula "hoy")
        max_date = fact.select(_max("order_delivered_customer_date")).collect()[0][0]

        # === RFM + métricas base ===
        base = (
            fact.groupBy("customer_unique_id")
            .agg(
                # Recency: días desde última compra
                datediff(lit(max_date), _max("order_purchase_timestamp")).alias("recency_days"),
                # Frequency: órdenes distintas
                countDistinct("order_id").alias("frequency"),
                # Monetary: gasto total (precio + flete)
                spark_round(_sum(col("price") + col("freight_value")), 2).alias("monetary"),
                # Métricas adicionales
                spark_round(avg(col("price") + col("freight_value")), 2).alias("avg_order_value"),
                count("order_item_id").alias("total_items"),
                spark_round(avg("review_score"), 2).alias("avg_review_score_given"),
                spark_round(
                    avg(when(col("is_late_delivery") == True, lit(1.0)).otherwise(lit(0.0))) * 100, 1
                ).alias("pct_late_deliveries"),
                spark_round(avg("delivery_days"), 1).alias("avg_delivery_days"),
                # Estado geográfico (del último registro)
                _max("customer_state").alias("customer_state"),
                # Fechas
                _min("order_purchase_timestamp").alias("first_purchase_date"),
                _max("order_purchase_timestamp").alias("last_purchase_date"),
            )
        )

        # === Preferencias (categoria, pago) ===
        preferred_category = self._get_preferred(
            fact.filter(col("product_category_name").isNotNull()),
            "customer_unique_id", "product_category_name", "preferred_category"
        )
        preferred_payment = self._get_preferred(
            fact.filter(col("payment_type").isNotNull()),
            "customer_unique_id", "payment_type", "preferred_payment"
        )

        df = (
            base
            .join(preferred_category, on="customer_unique_id", how="left")
            .join(preferred_payment, on="customer_unique_id", how="left")
        )
        #left pq puede haber no coincidencias
        # === LTV Segment (target para ML) ===
        # Percentiles sobre el DataFrame completo
        p25 = df.select(percentile_approx("monetary", 0.25)).collect()[0][0]
        p75 = df.select(percentile_approx("monetary", 0.75)).collect()[0][0]

        logger.info(f"LTV percentiles — p25: {p25:.2f}, p75: {p75:.2f}")

        df = df.withColumn(
            "ltv_segment",
            when(col("monetary") >= p75, "High")
            .when(col("monetary") >= p25, "Medium")
            .otherwise("Low")
        )

        return df.withColumn("_created_at", current_timestamp())

    def run(self) -> dict:
        start = time.time()
        logger.info("Construyendo agg_customer_ltv")
        fact = self.read_fact()
        df = self.build(fact)
        n = self._write(df)
        return {"status": "success", "table": "agg_customer_ltv", "records": n, "duration_seconds": round(time.time() - start, 2)}


# =============================================================================
# 3. MONTHLY METRICS
# =============================================================================

class AggMonthlyMetrics(BaseAggTable):
    """
    Métricas del negocio agregadas por mes.

    Responde: ¿Cómo evoluciona el negocio? ¿Hay estacionalidad?
    Útil para: dashboards ejecutivos, detección de tendencias, forecasting.

    Grain: una fila por año-mes (ej: "2018-03").
    """

    def __init__(self, spark: SparkSession, fact_path: Optional[str] = None, gold_path: Optional[str] = None):
        super().__init__(spark, fact_path, gold_path)
        self.gold_path = gold_path or str(config.paths.delta_gold / "agg_monthly_metrics")

    def build(self, fact: DataFrame) -> DataFrame:
        return (
            fact
            # Añadir columna year_month para agrupar
            .withColumn("year_month", date_format("order_purchase_timestamp", "yyyy-MM"))
            .groupBy("year_month")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                spark_round(_sum(col("price") + col("freight_value")), 2).alias("total_revenue"),
                count("order_item_id").alias("total_items_sold"),
                spark_round(avg(col("price") + col("freight_value")), 2).alias("avg_ticket"),
                spark_round(avg("review_score"), 2).alias("avg_review_score"),
                spark_round(
                    avg(when(col("is_late_delivery") == False, lit(1.0)).otherwise(lit(0.0))) * 100, 1
                ).alias("on_time_delivery_pct"),
                countDistinct("customer_unique_id").alias("unique_customers"),
                spark_round(avg("delivery_days"), 1).alias("avg_delivery_days"),
            )
            .orderBy("year_month")
            .withColumn("_created_at", current_timestamp())
        )

    def run(self) -> dict:
        start = time.time()
        logger.info("Construyendo agg_monthly_metrics")
        fact = self.read_fact()
        df = self.build(fact)
        n = self._write(df)
        return {"status": "success", "table": "agg_monthly_metrics", "records": n, "duration_seconds": round(time.time() - start, 2)}


# =============================================================================
# SCRIPT DE EJECUCIÓN
# =============================================================================

if __name__ == "__main__":
    from spark_jobs.utils.spark_utils import get_spark_session

    print("=" * 60)
    print("GOLD LAYER - AGGREGATE TABLES")
    print("=" * 60)

    spark = get_spark_session("GoldAggTables")

    tables = [
        AggSellerPerformance,
        AggCustomerLTV,
        AggMonthlyMetrics,
    ]

    # Leemos la fact table una sola vez y la cacheamos para no releerla 3 veces
    fact_path = str(config.paths.delta_gold / "fact_orders")
    fact = spark.read.format("delta").load(fact_path).cache()
    fact.count()  # materializa el cache
    logger.info("Fact table cacheada en memoria")

    for TableClass in tables:
        try:
            print(f"\n{'='*20} {TableClass.__name__} {'='*20}")
            # Pasamos la fact ya cacheada inyectando el path (se re-lee pero desde cache)
            table = TableClass(spark)
            result = table.run()
            print(f"  status:   {result['status']}")
            print(f"  records:  {result['records']:,}")
            print(f"  duration: {result['duration_seconds']}s")
        except Exception as e:
            print(f"  ERROR: {e}")
            raise

    fact.unpersist()
    spark.stop()
