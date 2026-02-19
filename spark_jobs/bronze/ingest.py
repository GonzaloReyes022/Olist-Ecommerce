"""
Ingesta de la capa Bronze.

=== PRINCIPIOS DE LA CAPA BRONZE ===

1. NO TRANSFORMAR DATOS
   - Guardar exactamente como llegan de la fuente
   - Si el dato está mal, guardarlo mal (lo arreglamos en Silver)

2. SÍ AGREGAR METADATOS
   - _ingestion_timestamp: cuándo se cargó
   - _source_file: de dónde vino
   - _batch_id: identificador del lote

3. MANEJAR ERRORES DE LECTURA
   - Usar modo PERMISSIVE para no perder datos
   - Capturar registros corruptos en columna especial

4. FORMATO DELTA LAKE
   - Append-only (nunca borrar/modificar)
   - Mantiene historial completo

=== ¿POR QUÉ ESTE ENFOQUE? ===

Si en Bronze transformamos los datos y hay un error:
- Perdemos el dato original
- No podemos auditar qué recibimos
- No podemos reprocesar

Con este enfoque:
- Siempre tenemos el dato original
- Podemos demostrar qué recibimos (auditoría)
- Si Silver/Gold tienen bugs, reprocesamos desde Bronze
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    input_file_name,
    col,
    count,
)
from delta import DeltaTable
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import logging

from spark_jobs.config.schemas import *
from spark_jobs.config.settings import config
from spark_jobs.utils.loggins_utils import get_logger

logger = get_logger(__name__)

class BronzeOrdersIngester():
    """
    Clase para ingestar órdenes desde CSV a capa Bronze.

    Uso:
        >>> from src.utils.spark_utils import get_spark_session
        >>> spark = get_spark_session()
        >>> ingester = BronzeOrdersIngester(spark)
        >>> result = ingester.run("data/raw/orders.csv")
        >>> print(result)
        {'status': 'success', 'records_processed': 10000, ...}
    """
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        """
        Inicializa el ingester.

        Args:
            spark: SparkSession activa
            bronze_path: Ruta para guardar datos Bronze (default: config)
        """
        self.spark = spark
        self.bronze_station = "orders"
        self.bronze_schema = BRONZE_ORDERS_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["order_id", "customer_id", "order_date", "total_amount"]


    def read_source(
        self,
        source_path: str,
        file_format: str = "csv",
    ) -> DataFrame:
        """
        Lee datos de la fuente.

        Args:
            source_path: Ruta al archivo/directorio fuente
            file_format: Formato del archivo (csv, json, parquet)

        Returns:
            DataFrame con los datos crudos

        Notas:
            - mode='PERMISSIVE': No falla en registros mal formados
            - columnNameOfCorruptRecord: Captura registros problemáticos
            - schema: Usamos schema explícito para detectar problemas temprano
        """
        logger.info(f"Leyendo datos de:{source_path}")

        if file_format == "csv":
            df = (self.spark.read \
                  .option("header", "true")   
                  #treats the first line as column names
                  .option("mode", "PERMISSIVE") 
                  .option("columnNameOfCorruptRecord", "_corrupt_record") 
                  .schema(self.bronze_schema) 
                  .csv(source_path))
        elif file_format == "json":
            df = (self.spark.read
                  .option("multiLine", "true")
                  .option("mode","PERMISSIVE")
                  .option("columnNameOfCorruptRecord", "_corrupt_record")
                  .schema(self.bronze_schema)
                  .json(source_path))
        elif file_format == "parquet":
            df = (self.spark.read
                  .schema(self.bronze_schema)
                  .parquet(source_path))
        elif file_format == "delta":
            df = (self.spark.read
                  .format("delta")
                  .load(source_path))
        else:
            raise ValueError(f"Formato no soportado: {file_format}")
        
        record_count = df.count()
        logger.record_count("source_read_orders_bronze", record_count)
        #validacion de registro corrupto
        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"Encontrados {corrupt_count} registros corruptos")
        return df
    
    def add_metadata(
        self,
        df: DataFrame,
        batch_id: str,
        source_name: Optional[str] = None,
    ) -> DataFrame:
        """
        Agrega metadatos de ingesta al DataFrame.

        Args:
            df: DataFrame original
            batch_id: Identificador único del lote
            source_name: Nombre de la fuente

        Returns:
            DataFrame con columnas de metadatos agregadas

        Metadatos agregados:
            - _ingestion_timestamp: Momento exacto de la carga
            - _batch_id: ID único de este lote (para rastreo)
            - _source_file: Archivo de origen (si está disponible)
            - _source_name: Nombre lógico de la fuente

        ¿Por qué estos metadatos?
            - Debugging: "¿Cuándo llegó este registro?"
            - Auditoría: "¿De dónde vino este dato?"
            - Reprocesamiento: "¿Qué lote debo reprocesar?"
        """
        if source_name is None:
            source_name = self.bronze_station
        
        df = df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_batch_id", lit(batch_id)) \
            .withColumn("_source_file", input_file_name()) \
            .withColumn("_source_name", lit(source_name))
        
        return df

    def write_to_bronze(
        self,
        df: DataFrame,
        mode: str = "append",
    ) -> None:
        """
        Escribe datos a Delta Lake en capa Bronze.

        Args:
            df: DataFrame a escribir
            mode: Modo de escritura ('append' recomendado para Bronze)

        ¿Por qué Delta Lake?
            1. ACID Transactions: No hay datos corruptos por fallos
            2. Time Travel: Puedes ver versiones anteriores
            3. Schema Enforcement: Rechaza datos con schema incorrecto
            4. Optimizaciones: Compaction, Z-ordering automático

        ¿Por qué append-only?
            - Bronze es un "landing zone" - todo lo que llega se guarda
            - No queremos perder datos históricos
            - La deduplicación se hace en Silver
        """
        record_count = df.count()
        logger.info(f"Escribiendo {record_count:,} registros a Bronze")
        df.write.format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .save(self.bronze_path)
        
        logger.info(f"Escritura completada en: {self.bronze_path}")

    def get_metrics(self, df: DataFrame, critical_columns: Optional[list] = None) -> Dict[str, Any]:
        """
        Calcula métricas del lote para monitoreo.

        Returns:
            Diccionario con métricas del procesamiento
        """
        total_records = df.count()
        # Contar nulos por columna crítica
        null_counts = {}
        if critical_columns is None:
            critical_columns = self.cricical_colums
        for column in critical_columns:
            if column in df.columns:
                null_counts[column] = df.filter(col(column).isNull()).count()
        
        # Detectar posibles duplicados usando la primera columna crítica (PK)
        pk_column = critical_columns[0]
        distinct_count = df.select(pk_column).distinct().count()
        #distinct es mejor y mas otimizado que groupBy para esto
        #esto se debe a que por cada groupby se hace un shuffle
        #en el cual se agrega un header, creando una comparticion para cada id
        #sin embargo el distinct lo que hace es usar un hash para contar, una masa jeje
        duplicados_count = (df.groupBy(pk_column)
            .agg(count(pk_column).alias("conteo"))
            .where("conteo > 1") # O .where(F.col("conteo") > 1)
            .count())

        return {
            "total_records": total_records,
            "distinct_pk": distinct_count,
            "pk_column": pk_column,
            "possible_duplicates": total_records - distinct_count,
            "null_counts": null_counts,
            "duplicados_count": duplicados_count,
        }

    def run(
        self,
        source_path: str,
        file_format: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline de ingesta completo.

        Args:
            source_path: Ruta al archivo/directorio fuente
            file_format: Formato del archivo

        Returns:
            Diccionario con resultado de la ejecución

        Ejemplo:
            >>> result = ingester.run("data/raw/orders.csv")
            >>> if result['status'] == 'success':
            ...     print(f"Procesados {result['records_processed']} registros")
        """
        # Generar batch_id único
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.start_batch(batch_id, source_path)
        start_time = datetime.now()
        if file_format is None:
            file_format = source_path.split(".")[-1]
        try:
            # 1. Leer fuente
            df_raw = self.read_source(source_path, file_format)
            # 2. Agregar metadatos
            df_with_metadata = self.add_metadata(df_raw, batch_id)
            # 3. Calcular métricas antes de escribir
            metrics = self.get_metrics(df_with_metadata)
            # 4. Escribir a Bronze
            self.write_to_bronze(df_with_metadata)
            # 5. Calcular duración
            duration = (datetime.now() - start_time).total_seconds()
            result = {
                "status" : "success",
                "batch_id": batch_id,
                "records_processed": metrics["total_records"],
                "bronze_path": self.bronze_path,
                "duration_seconds": round(duration, 2),
                "metrics": metrics,
            }
            logger.end_batch(
                batch_id,
                "success",
                metrics["total_records"],
                duration,
            )
            return result
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error en ingesta: {str(e)}", e)
            logger.end_batch(batch_id, "failed", 0, duration)
            return {
                "status": "failed",
                "batch_id": batch_id,
                "error": str(e),
                "duration_seconds": round(duration, 2),
            }
class BronzeCustomerIngester(BronzeOrdersIngester):
    """Ingester específico para clientes (misma lógica, diferente schema y rutas)."""
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        super().__init__(spark, bronze_path)
        self.bronze_station = "customers"
        self.bronze_schema = BRONZE_CUSTOMERS_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["customer_id", "customer_unique_id", "customer_zip_code_prefix"]

class BronzeOrderItemsIngester(BronzeOrdersIngester):
    """Ingester específico para order items (misma lógica, diferente schema y rutas)."""
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        super().__init__(spark, bronze_path)
        self.bronze_station = "order_items"
        self.bronze_schema = BRONZE_ORDER_ITEMS_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["order_id", "product_id", "seller_id", "price", "freight_value"]

class BronzeProductsIngester(BronzeOrdersIngester):
    """Ingester específico para productos (misma lógica, diferente schema y rutas)."""
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        super().__init__(spark, bronze_path)
        self.bronze_station = "products"
        self.bronze_schema = BRONZE_PRODUCTS_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["product_id", "product_category_name", "product_weight_g"]

class BronzeOrderPaymentsIngester(BronzeOrdersIngester):
    """Ingester específico para productos (misma lógica, diferente schema y rutas)."""
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        super().__init__(spark, bronze_path)
        self.bronze_station = "orders_payments"
        self.bronze_schema = BRONZE_ORDER_PAYMENTS_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["order_id", "payment_type", "payment_value"]

class BronzeGeolocationIngester(BronzeOrdersIngester):
    """Ingester específico para productos (misma lógica, diferente schema y rutas)."""
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        super().__init__(spark, bronze_path)
        self.bronze_station = "geolocation"
        self.bronze_schema = BRONZE_GEOLOCATION_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]

class BronzeSellersIngester(BronzeOrdersIngester):
    """Ingester específico para productos (misma lógica, diferente schema y rutas)."""
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        super().__init__(spark, bronze_path)
        self.bronze_station = "sellers"
        self.bronze_schema = BRONZE_SELLERS_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["seller_id", "seller_state"]

class BronzeOrdersReviewsIngester(BronzeOrdersIngester):
    """Ingester específico para productos (misma lógica, diferente schema y rutas)."""
    def __init__(self, spark: SparkSession, bronze_path: Optional[str] = None):
        super().__init__(spark, bronze_path)
        self.bronze_station = "orders_reviews"
        self.bronze_schema = BRONZE_ORDER_REVIEWS_SCHEMA
        self.bronze_path = bronze_path or str(config.paths.delta_bronze / self.bronze_station)
        self.cricical_colums = ["order_id", "review_score"]

#SCRIPT DE EJECUCION

if __name__ == "__main__":
    """
    Ejecutar desde línea de comandos:
        python -m spark_jobs.bronze.ingest_orders

    O importar y usar:
        from spark_jobs.bronze.ingest_orders import BronzeOrdersIngester
    """
    from spark_jobs.utils.spark_utils import get_spark_session

    print("=" * 60)
    print("BRONZE LAYER - INGESTA DE ÓRDENES")
    print("=" * 60)
    spark = get_spark_session("BronzeIngestion")
    # 1. Definimos la lista de trabajos: (Clase, Nombre del Archivo)
    ingestion_jobs = [
        (BronzeOrdersIngester, "orders.csv"),
        (BronzeCustomerIngester, "customers.csv"),
        (BronzeOrderItemsIngester, "order_items.csv"),
        (BronzeProductsIngester, "products.csv"),
        (BronzeOrderPaymentsIngester, "order_payments.csv"),
        (BronzeGeolocationIngester, "geolocation.csv"),
        (BronzeSellersIngester, "sellers.csv"),
        (BronzeOrdersReviewsIngester, "order_reviews.csv")
    ]

    # 2. Iteramos y ejecutamos
    for IngesterClass, filename in ingestion_jobs:
        try:
            print(f"\n{'='*20} Iniciando {IngesterClass.__name__} {'='*20}")
            
            # Instanciamos la clase dinámicamente
            ingester = IngesterClass(spark)
            
            # Construimos la ruta
            file_path = str(config.paths.data_raw / filename)
            
            # Ejecutamos
            ingester.run(file_path)
                        
        except Exception as e:
            print(f"Error procesando {filename}: {str(e)}")
            break
            # detener todo si uno falla