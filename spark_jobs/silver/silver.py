"""
Transformación de Bronze a Silver.

=== PRINCIPIOS DE LA CAPA SILVER ===

1. CASTING DE TIPOS
   - String → Date, Integer, Decimal según corresponda
   - Manejo explícito de errores de conversión

2. LIMPIEZA DE DATOS
   - Eliminar espacios en blanco
   - Estandarizar mayúsculas/minúsculas
   - Manejar valores nulos de forma consistente

3. DEDUPLICACIÓN
   - Un registro por entidad (orden, cliente, etc.)
   - Criterio claro: mantener el más reciente

4. VALIDACIÓN
   - Aplicar reglas de negocio
   - Marcar registros válidos/inválidos (no eliminar)

5. ENRIQUECIMIENTO
   - Calcular campos derivados
   - Agregar claves para dimensiones (date_key)

=== PATRÓN DE DISEÑO: MERGE (UPSERT) ===

En lugar de append (Bronze) usamos MERGE:
- Si el registro existe → actualizar
- Si no existe → insertar

Esto evita duplicados si el pipeline se re-ejecuta.

=== DISEÑO DE CLASES ===

¿Por qué NO usar herencia simple como en Bronze?

En Bronze, todas las clases hacen EXACTAMENTE lo mismo:
  leer CSV → agregar metadata → escribir Delta
Solo cambian datos del __init__ (schema, path, columnas).

En Silver, cada dataset necesita transformaciones DIFERENTES:
  - cast_types(): orders castea timestamps, products castea doubles
  - clean_strings(): orders limpia order_status, customers limpia customer_city
  - validate_business_rules(): reglas completamente distintas por dataset

Si cada subclase sobreescribe todos los métodos, la herencia no sirve de nada.

SOLUCIÓN: Configuración declarativa.
  - La clase base tiene los métodos GENÉRICOS (read, deduplicate, write, run)
  - Cada subclase DECLARA sus transformaciones como datos en el __init__:
      * cast_map: {"columna": tipo_destino}
      * string_transforms: {"columna": funcion_limpieza}
      * null_defaults: {"columna": valor_default}
      * validation_rules: [condicion1, condicion2, ...]
  - La clase base EJECUTA esas declaraciones genéricamente

Así los métodos genéricos se heredan sin cambios, y las subclases
solo definen QUÉ transformar, no CÓMO transformar.
"""
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import (
    col,
    when,
    trim,
    upper,
    lower,
    initcap,
    to_timestamp,
    current_timestamp,
    lit,
    row_number,
    coalesce,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    TimestampType,
    DataType,
)
from delta import DeltaTable
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable

from spark_jobs.config.schemas import VALID_ORDER_STATUSES
from spark_jobs.config.settings import config
from spark_jobs.utils.loggins_utils import get_logger

logger = get_logger(__name__)


class SilverCleaner:
    """
    Clase base para transformar datos de Bronze a Silver.

    Los métodos genéricos (read_bronze, deduplicate, write_to_silver, run)
    se heredan sin cambios. Cada subclase solo declara en su __init__:

    - station: nombre del dataset ("orders", "customers", etc.)
    - pk_column: columna de clave primaria para dedup y merge
    - cast_map: dict de columna → tipo destino para cast_types()
    - string_transforms: dict de columna → función para clean_strings()
    - null_defaults: dict de columna → valor default para handle_nulls()
    - validation_rules: lista de Column expressions para validate()

    Uso:
        >>> cleaner = SilverOrdersCleaner(spark)
        >>> result = cleaner.run()
    """

    def __init__(
        self,
        spark: SparkSession,
        station: str,
        pk_column: str,
        cast_map: Dict[str, DataType],
        string_transforms: Dict[str, Callable[[Column], Column]],
        null_defaults: Dict[str, Any],
        validation_rules: List[Column],
        silver_columns: List[str],
        bronze_path: Optional[str] = None,
        silver_path: Optional[str] = None,
        merge_keys: Optional[List[str]] = None,
    ):
        self.spark = spark
        self.station = station
        self.pk_column = pk_column
        self.cast_map = cast_map
        self.string_transforms = string_transforms
        self.null_defaults = null_defaults
        self.validation_rules = validation_rules
        self.silver_columns = silver_columns
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / station)
        self.silver_path = silver_path or str(config.paths.delta_silver / station)
        # merge_keys: columnas usadas en la condición del MERGE.
        # Por defecto es [pk_column] (PK simple).
        # Para PKs compuestas pasar, ej: ["order_id", "order_item_id"]
        self.merge_keys = merge_keys or [pk_column]

    # =========================================================================
    # MÉTODOS GENÉRICOS - se heredan sin cambios
    # =========================================================================

    def read_bronze(self) -> DataFrame:
        """Lee datos de la capa Bronze."""
        logger.info(f"Leyendo Bronze desde: {self.bronze_path}")
        df = self.spark.read.format("delta").load(self.bronze_path)
        logger.record_count("bronze_read", df.count())
        return df

    def cast_types(self, df: DataFrame) -> DataFrame:
        """
        Convierte tipos de datos según self.cast_map.

        Itera sobre el dict {columna: tipo} y aplica .cast() a cada una.
        Los valores que no se pueden convertir se vuelven NULL
        (Spark no falla, simplemente devuelve null en cast fallido).

        Para timestamps usa to_timestamp() en vez de .cast() porque
        necesita parsear el formato string específico.
        """
        logger.info("Aplicando casting de tipos")
        for column, target_type in self.cast_map.items():
            if column not in df.columns:
                continue
            if isinstance(target_type, TimestampType):
                df = df.withColumn(column, to_timestamp(col(column)))
            else:
                df = df.withColumn(column, col(column).cast(target_type))
        return df
    def clean_strings(self, df: DataFrame) -> DataFrame:
        """
        Limpia campos de texto según self.string_transforms.

        Cada entrada es {columna: función_transformación}.
        La función recibe un Column y devuelve un Column transformado.
        Ejemplo: {"customer_city": lambda c: initcap(trim(c))}
        """
        logger.info("Limpiando campos de texto")
        for column, transform_fn in self.string_transforms.items():
            if column not in df.columns:
                continue
            df = df.withColumn(column, transform_fn(col(column)))
        return df

    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Maneja valores nulos según self.null_defaults.

        Usa coalesce(columna, default): si la columna es NULL, usa el default.
        Si el default es None, no hace nada (deja el NULL para que
        la validación lo marque como inválido).
        """
        logger.info("Manejando valores nulos")
        for column, default_value in self.null_defaults.items():
            if column not in df.columns:
                continue
            df = df.withColumn(column, coalesce(col(column), lit(default_value)))
        return df
    
    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Elimina duplicados quedándose con el registro más reciente.

        Usa Window particionado por pk_column, ordenado por
        _ingestion_timestamp descendente. row_number() = 1 es el más reciente.
        """
        logger.info("Eliminando duplicados")
        window = Window.partitionBy(self.pk_column).orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_ranked = df.withColumn("_row_num", row_number().over(window))
        df_deduped = df_ranked.filter(col("_row_num") == 1).drop("_row_num")

        original_count = df.count()
        deduped_count = df_deduped.count()
        logger.info(f"Duplicados eliminados: {original_count - deduped_count:,}")
        return df_deduped

        #widow_deduplicate = Window.partitionBy(self.pk_column).orderBy(col("_ingestion_timestamp").desc())
        #deduplicate = df.withColumn("rn", row_number().over(window_deduplicate)).filter("rn = 1").drop("rn")
  
    """ Una forma a tener en cuenta.
    query = f
        SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY {pk_column} ORDER BY {time_stamp} DESC) as rn
        FROM raw_data
    ) WHERE rn = 1
    """
    def validate_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Aplica reglas de validación y marca _is_valid.

        Combina todas las reglas con AND: si CUALQUIER regla falla,
        el registro se marca como inválido (_is_valid = False).
        NO elimina registros, solo los marca.
        """
        logger.info("Aplicando validaciones de negocio")
        if not self.validation_rules:
            return df.withColumn("_is_valid", lit(True))

        combined_rule = self.validation_rules[0]
        for rule in self.validation_rules[1:]:
            combined_rule = combined_rule & rule
        return df.withColumn("_is_valid", combined_rule)

    def add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """Agrega _cleaned_at y _source_file de metadata."""
        df = df.withColumn("_cleaned_at", current_timestamp())
        if "_source_file" not in df.columns:
            df = df.withColumn("_source_file", lit(self.bronze_path))
        return df

    def select_silver_columns(self, df: DataFrame) -> DataFrame:
        """Selecciona solo las columnas definidas en silver_columns."""
        return df.select(self.silver_columns)

    def write_to_silver(self, df: DataFrame) -> None:
        """
        Escribe a Silver usando MERGE (upsert).

        Primera ejecución: crea la tabla con overwrite.
        Ejecuciones siguientes: MERGE por pk_column.
          - Si el registro existe → actualizar
          - Si no existe → insertar
        """
        logger.info(f"Escribiendo a Silver: {self.silver_path}")
        table_exists = DeltaTable.isDeltaTable(self.spark, self.silver_path)

        if table_exists:
            logger.info("Tabla existe - ejecutando MERGE")
            delta_table = DeltaTable.forPath(self.spark, self.silver_path)
            merge_condition = " AND ".join(
                f"target.{k} = source.{k}" for k in self.merge_keys
            )
            (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            logger.info("Tabla no existe - creando nueva")
            df.write.format("delta").mode("overwrite").save(self.silver_path)

        logger.info("Escritura a Silver completada")

    def get_quality_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """Calcula métricas de calidad."""
        total = df.count()
        valid = df.filter(col("_is_valid")).count()
        return {
            "total_records": total,
            "valid_records": valid,
            "invalid_records": total - valid,
            "validation_rate": round(valid / total * 100, 2) if total > 0 else 0,
        }

    def run(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo de Bronze a Silver.

        Pipeline:
          read_bronze → cast_types → clean_strings → handle_nulls
          → deduplicate → validate → add_metadata → select_columns
          → write_to_silver
        """
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.start_batch(batch_id, f"silver_{self.station}")
        start_time = datetime.now()

        try:
            df = (
                self.read_bronze()
                .transform(self.cast_types)
                .transform(self.clean_strings)
                .transform(self.handle_nulls)
                .transform(self.deduplicate)
                .transform(self.validate_business_rules)
                .transform(self.add_silver_metadata)
                .transform(self.select_silver_columns)
            )

            quality_metrics = self.get_quality_metrics(df)
            logger.quality_metrics(quality_metrics)

            if quality_metrics["validation_rate"] < 95:
                logger.warning(
                    f"Tasa de validación baja: {quality_metrics['validation_rate']}%"
                )

            self.write_to_silver(df)
            duration = (datetime.now() - start_time).total_seconds()

            result = {
                "status": "success",
                "batch_id": batch_id,
                "station": self.station,
                "silver_path": self.silver_path,
                "duration_seconds": round(duration, 2),
                **quality_metrics,
            }
            logger.end_batch(batch_id, "success", quality_metrics["total_records"], duration)
            return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error en Silver ({self.station}): {str(e)}", e)
            logger.end_batch(batch_id, "failed", 0, duration)
            return {
                "status": "failed",
                "batch_id": batch_id,
                "station": self.station,
                "error": str(e),
                "duration_seconds": round(duration, 2),
            }


# =============================================================================
# SUBCLASES - Solo declaran QUÉ transformar, no CÓMO
# =============================================================================

class SilverOrdersCleaner(SilverCleaner):
    """Silver cleaner para orders."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="orders",
            pk_column="order_id",
            bronze_path=bronze_path,
            silver_path=silver_path,
            # Qué columnas castear y a qué tipo
            cast_map={
                "order_purchase_timestamp": TimestampType(),
                "order_approved_at": TimestampType(),
                "order_delivered_carrier_date": TimestampType(),
                "order_delivered_customer_date": TimestampType(),
                "order_estimated_delivery_date": TimestampType(),
            },
            # Qué columnas limpiar y cómo
            string_transforms={
                "order_id": lambda c: upper(trim(c)),
                "customer_id": lambda c: upper(trim(c)),
                "order_status": lambda c: lower(trim(c)),
            },
            # Qué columnas rellenar si son NULL
            null_defaults={},
            # Reglas de validación (todas deben cumplirse)
            validation_rules=[
                col("order_id").isNotNull(),
                col("customer_id").isNotNull(),
                col("order_purchase_timestamp").isNotNull(),
                col("order_status").isNotNull(),
                col("order_status").isin(VALID_ORDER_STATUSES),
                col("order_estimated_delivery_date").isNotNull(),
            ],
            # Columnas finales de la tabla Silver
            silver_columns=[
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )


class SilverCustomersCleaner(SilverCleaner):
    """Silver cleaner para customers."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="customers",
            pk_column="customer_id",
            bronze_path=bronze_path,
            silver_path=silver_path,
            cast_map={
                "customer_zip_code_prefix": IntegerType(),
            },
            string_transforms={
                "customer_id": lambda c: upper(trim(c)),
                "customer_unique_id": lambda c: upper(trim(c)),
                "customer_city": lambda c: initcap(trim(c)),
                "customer_state": lambda c: upper(trim(c)),
            },
            null_defaults={},
            validation_rules=[
                col("customer_id").isNotNull(),
                col("customer_unique_id").isNotNull(),
                col("customer_zip_code_prefix").isNotNull(),
                col("customer_city").isNotNull(),
                col("customer_state").isNotNull(),
            ],
            silver_columns=[
                "customer_id",
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )


class SilverOrderItemsCleaner(SilverCleaner):
    """Silver cleaner para order_items."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="order_items",
            pk_column="order_id",
            merge_keys=["order_id", "order_item_id"],
            bronze_path=bronze_path,
            silver_path=silver_path,
            cast_map={
                "order_item_id": IntegerType(),
                "price": DoubleType(),
                "freight_value": DoubleType(),
                "shipping_limit_date": TimestampType(),
            },
            string_transforms={
                "order_id": lambda c: upper(trim(c)),
                "product_id": lambda c: upper(trim(c)),
                "seller_id": lambda c: upper(trim(c)),
            },
            null_defaults={
                "freight_value": 0.0,
            },
            validation_rules=[
                col("order_id").isNotNull(),
                col("order_item_id").isNotNull(),
                col("product_id").isNotNull(),
                col("seller_id").isNotNull(),
                col("price").isNotNull() & (col("price") >= 0),
                col("freight_value") >= 0,
                col("shipping_limit_date").isNotNull(),
            ],
            silver_columns=[
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "shipping_limit_date",
                "price",
                "freight_value",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Order items tiene PK compuesta (order_id + order_item_id),
        no se puede deduplicar solo por order_id.
        """
        logger.info("Eliminando duplicados (PK compuesta: order_id + order_item_id)")
        window = Window.partitionBy("order_id", "order_item_id").orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_ranked = df.withColumn("_row_num", row_number().over(window))
        df_deduped = df_ranked.filter(col("_row_num") == 1).drop("_row_num")

        original_count = df.count()
        deduped_count = df_deduped.count()
        logger.info(f"Duplicados eliminados: {original_count - deduped_count:,}")
        return df_deduped


class SilverProductsCleaner(SilverCleaner):
    """Silver cleaner para products."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="products",
            pk_column="product_id",
            bronze_path=bronze_path,
            silver_path=silver_path,
            cast_map={
                "product_name_length": IntegerType(),
                "product_description_length": IntegerType(),
                "product_photos_qty": IntegerType(),
                "product_weight_g": DoubleType(),
                "product_length_cm": DoubleType(),
                "product_height_cm": DoubleType(),
                "product_width_cm": DoubleType(),
            },
            string_transforms={
                "product_id": lambda c: upper(trim(c)),
                "product_category_name": lambda c: lower(trim(c)),
            },
            null_defaults={},
            validation_rules=[
                col("product_id").isNotNull(),
                # Peso y dimensiones deben ser positivos si existen
                (col("product_weight_g").isNull()) | (col("product_weight_g") > 0),
                (col("product_length_cm").isNull()) | (col("product_length_cm") > 0),
                (col("product_height_cm").isNull()) | (col("product_height_cm") > 0),
                (col("product_width_cm").isNull()) | (col("product_width_cm") > 0),
            ],
            silver_columns=[
                "product_id",
                "product_category_name",
                "product_name_length",
                "product_description_length",
                "product_photos_qty",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )


class SilverOrderPaymentsCleaner(SilverCleaner):
    """Silver cleaner para order_payments."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="orders_payments",
            pk_column="order_id",
            merge_keys=["order_id", "payment_sequential"],
            bronze_path=bronze_path,
            silver_path=silver_path,
            cast_map={
                "payment_sequential": IntegerType(),
                "payment_installments": IntegerType(),
                "payment_value": DoubleType(),
            },
            string_transforms={
                "order_id": lambda c: upper(trim(c)),
                "payment_type": lambda c: lower(trim(c)),
            },
            null_defaults={},
            validation_rules=[
                col("order_id").isNotNull(),
                col("payment_sequential").isNotNull(),
                col("payment_type").isNotNull(),
                col("payment_value").isNotNull() & (col("payment_value") >= 0),
                col("payment_installments").isNotNull() & (col("payment_installments") >= 0),
            ],
            silver_columns=[
                "order_id",
                "payment_sequential",
                "payment_type",
                "payment_installments",
                "payment_value",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Payments tiene PK compuesta (order_id + payment_sequential)."""
        logger.info("Eliminando duplicados (PK compuesta: order_id + payment_sequential)")
        window = Window.partitionBy("order_id", "payment_sequential").orderBy(
            col("_ingestion_timestamp").desc()
        )
        df_ranked = df.withColumn("_row_num", row_number().over(window))
        df_deduped = df_ranked.filter(col("_row_num") == 1).drop("_row_num")

        original_count = df.count()
        deduped_count = df_deduped.count()
        logger.info(f"Duplicados eliminados: {original_count - deduped_count:,}")
        return df_deduped


class SilverGeolocationCleaner(SilverCleaner):
    """Silver cleaner para geolocation."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="geolocation",
            pk_column="geolocation_zip_code_prefix",
            bronze_path=bronze_path,
            silver_path=silver_path,
            cast_map={
                "geolocation_zip_code_prefix": IntegerType(),
                "geolocation_lat": DoubleType(),
                "geolocation_lng": DoubleType(),
            },
            string_transforms={
                "geolocation_city": lambda c: initcap(trim(c)),
                "geolocation_state": lambda c: upper(trim(c)),
            },
            null_defaults={},
            validation_rules=[
                col("geolocation_zip_code_prefix").isNotNull(),
                col("geolocation_lat").isNotNull(),
                col("geolocation_lng").isNotNull(),
                # Coordenadas de Brasil: lat entre -34 y 6, lng entre -74 y -35
                (col("geolocation_lat") >= -34) & (col("geolocation_lat") <= 6),
                (col("geolocation_lng") >= -74) & (col("geolocation_lng") <= -35),
                col("geolocation_city").isNotNull(),
                col("geolocation_state").isNotNull(),
            ],
            silver_columns=[
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng",
                "geolocation_city",
                "geolocation_state",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )


class SilverSellersCleaner(SilverCleaner):
    """Silver cleaner para sellers."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="sellers",
            pk_column="seller_id",
            bronze_path=bronze_path,
            silver_path=silver_path,
            cast_map={
                "seller_zip_code_prefix": IntegerType(),
            },
            string_transforms={
                "seller_id": lambda c: upper(trim(c)),
                "seller_city": lambda c: initcap(trim(c)),
                "seller_state": lambda c: upper(trim(c)),
            },
            null_defaults={},
            validation_rules=[
                col("seller_id").isNotNull(),
                col("seller_zip_code_prefix").isNotNull(),
                col("seller_city").isNotNull(),
                col("seller_state").isNotNull(),
            ],
            silver_columns=[
                "seller_id",
                "seller_zip_code_prefix",
                "seller_city",
                "seller_state",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )


class SilverOrderReviewsCleaner(SilverCleaner):
    """Silver cleaner para order_reviews."""

    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None, silver_path: Optional[str] = None):
        super().__init__(
            spark=spark,
            station="orders_reviews",
            pk_column="review_id",
            bronze_path=bronze_path,
            silver_path=silver_path,
            cast_map={
                "review_score": IntegerType(),
                "review_creation_date": TimestampType(),
                "review_answer_timestamp": TimestampType(),
            },
            string_transforms={
                "review_id": lambda c: upper(trim(c)),
                "order_id": lambda c: upper(trim(c)),
                "review_comment_title": lambda c: trim(c),
                "review_comment_message": lambda c: trim(c),
            },
            null_defaults={},
            validation_rules=[
                col("review_id").isNotNull(),
                col("order_id").isNotNull(),
                col("review_score").isNotNull(),
                (col("review_score") >= 1) & (col("review_score") <= 5),
                col("review_creation_date").isNotNull(),
            ],
            silver_columns=[
                "review_id",
                "order_id",
                "review_score",
                "review_comment_title",
                "review_comment_message",
                "review_creation_date",
                "review_answer_timestamp",
                "_cleaned_at",
                "_is_valid",
                "_source_file",
            ],
        )


# =============================================================================
# SCRIPT DE EJECUCIÓN
# =============================================================================

if __name__ == "__main__":
    from spark_jobs.utils.spark_utils import get_spark_session

    print("=" * 60)
    print("SILVER LAYER - LIMPIEZA DE DATOS")
    print("=" * 60)

    spark = get_spark_session("SilverCleaning")

    # Lista de cleaners a ejecutar (mismo patrón que Bronze)
    cleaners = [
        SilverOrdersCleaner,
        SilverCustomersCleaner,
        SilverOrderItemsCleaner,
        SilverProductsCleaner,
        SilverOrderPaymentsCleaner,
        SilverGeolocationCleaner,
        SilverSellersCleaner,
        SilverOrderReviewsCleaner,
    ]

    for CleanerClass in cleaners:
        try:
            print(f"\n{'='*20} {CleanerClass.__name__} {'='*20}")
            cleaner = CleanerClass(spark)
            result = cleaner.run()

            print(f"  status: {result['status']}")
            if result["status"] == "success":
                print(f"  records: {result['total_records']:,}")
                print(f"  valid: {result['valid_records']:,} ({result['validation_rate']}%)")
                print(f"  duration: {result['duration_seconds']}s")
        except Exception as e:
            print(f"Error procesando {CleanerClass.__name__}: {str(e)}")
            break

    spark.stop()
