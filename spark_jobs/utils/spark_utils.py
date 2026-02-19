"""
Utilidades para trabajar con Spark y Delta Lake.

Este módulo centraliza la creación y configuración de SparkSession,
asegurando consistencia en todo el proyecto.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from delta import configure_spark_with_delta_pip
from typing import Optional, Dict, Any
import logging

from spark_jobs.config.settings import config

logger = logging.getLogger(__name__)

def get_spark_session(
    app_name: Optional[str] = None,
    additional_config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Crea o retorna una SparkSession configurada para Delta Lake.

    Args:
        app_name: Nombre de la aplicación Spark (default: config.spark.app_name)
        additional_config: Configuraciones adicionales de Spark

    Returns:
        SparkSession configurada

    Ejemplo:
        >>> spark = get_spark_session("MiJob")
        >>> df = spark.read.format("delta").load("path/to/table")

    Notas:
        - En Databricks, SparkSession ya está disponible como `spark`
        - Este método es principalmente para desarrollo local
    """

    app_name = app_name or config.spark.app_name

    #configuracion base
    spark_config = config.spark.to_spark_conf()

    #Agregar/Sobreescribir config adicional
    if additional_config:
        spark_config.update(additional_config)

    #Crear builder
    builder = SparkSession.builder.appName(app_name) # type: ignore
    
    #aplicamos configs
    for k, v in spark_config.items():
        builder = builder.config(k, v)
        
    #Configurar Delta Lake (como no se puede correr databrick free en local toca esto noma)
    #agrega las dependencias necesarias
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    #configurar nivel logs
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession creada: {app_name}")
    logger.debug(f"Configuración: {spark_config}")

    return spark
    

def read_delta(
    spark: SparkSession,
    path: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None
) -> DataFrame:
    """
    Lee una tabla Delta con soporte para Time Travel.

    Args:
        spark: SparkSession activa
        path: Ruta a la tabla Delta
        version: Versión específica a leer (Time Travel)
        timestamp: Timestamp específico a leer (Time Travel)

    Returns:
        DataFrame con los datos

    Ejemplo:
        >>> df = read_delta(spark, "data/delta/silver/orders")
        >>> df_v5 = read_delta(spark, "data/delta/silver/orders", version=5)
        >>> df_yesterday = read_delta(spark, "data/delta/silver/orders",
        ...                           timestamp="2024-01-15")
    """
    reader = spark.read.format("delta")

    if version is not None:
        reader = reader.option("versionAsOf", version)
        logger.info(f"Leyendo versión {version} de {path}")
    if timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
        logger.info(f"Leyendo timestamp {timestamp} de {path}")

    return reader.load(path)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Optional[list] = None,
    merge_schema: bool = True,
    optimize_write: bool = True
) -> None:
    """
    Escribe un DataFrame a Delta Lake con opciones optimizadas.

    Args:
        df: DataFrame a escribir
        path: Ruta destino
        mode: Modo de escritura (append, overwrite, error, ignore)
        partition_by: Columnas para particionar
        merge_schema: Permitir evolución de schema
        optimize_write: Optimizar archivos al escribir

    Ejemplo:
        >>> write_delta(df, "data/delta/bronze/orders",
        ...             partition_by=["order_date"])
    """
    writer = (df.write
              .format("delta")
              .mode(mode)
              .option("mergeSchema", str(merge_schema).lower())
              .option("optimizeWrite", str(optimize_write).lower()))

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)
    logger.info(f"Datos escritos a {path} (mode={mode})")


def optimize_table(spark: SparkSession, path: str, zorder_by: Optional[list] = None) -> None:
    """
    Optimiza una tabla Delta (compacta archivos pequeños).

    Args:
        spark: SparkSession activa
        path: Ruta a la tabla Delta
        zorder_by: Columnas para Z-ordering (mejora queries de rango)

    Ejemplo:
        >>> optimize_table(spark, "data/delta/gold/fact_sales",
        ...                zorder_by=["date_key", "customer_key"])

    Notas:
        - Z-ordering ordena los datos físicamente por las columnas especificadas
        - Mejora significativamente queries que filtran por esas columnas
        - Costoso de ejecutar, hacerlo periódicamente (ej: diario)
    """
    if zorder_by:
        zorder_clause = f"ZORDER BY ({', '.join(zorder_by)})"
        spark.sql(f"OPTIMIZE delta.`{path}` {zorder_clause}")
        logger.info(f"Tabla optimizada con ZORDER: {path}")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
        logger.info(f"Tabla optimizada: {path}")

def vacuum_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """
    Limpia archivos antiguos de una tabla Delta.

    Args:
        spark: SparkSession activa
        path: Ruta a la tabla Delta
        retention_hours: Horas de retención (default: 168 = 7 días)

    Notas:
        - VACUUM elimina archivos que ya no son necesarios
        - retention_hours mínimo recomendado: 168 (7 días)
        - Después de VACUUM no puedes hacer Time Travel más allá de la retención
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info(f"VACUUM completado: {path} (retención: {retention_hours}h)")


def get_table_history(spark: SparkSession, path: str, limit: int = 10) -> DataFrame:
    """
    Obtiene el historial de cambios de una tabla Delta.

    Útil para:
        - Debugging: ver qué cambió y cuándo
        - Auditoría: quién hizo qué
        - Time Travel: encontrar versión específica

    Args:
        spark: SparkSession activa
        path: Ruta a la tabla Delta
        limit: Número de versiones a retornar

    Returns:
        DataFrame con el historial
    """
    return spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT {limit}")

def table_exists(spark: SparkSession, path: str) -> bool:
    """
    Verifica si una tabla Delta existe en la ruta especificada.

    Args:
        spark: SparkSession activa
        path: Ruta a verificar

    Returns:
        True si existe, False si no
    """

    from delta import DeltaTable
    return DeltaTable.isDeltaTable(spark, path)

def get_table_details(spark: SparkSession, path: str) -> Dict[str, Any]:
    """
    Obtiene detalles de una tabla Delta.

    Returns:
        Diccionario con información de la tabla
    """
    from delta import DeltaTable

    if not table_exists(spark, path):
        return {"exists": False}

    dt = DeltaTable.forPath(spark, path)
    detail = dt.detail().collect()[0]

    return {
        "exists": True,
        "name": detail["name"],
        "num_files": detail["numFiles"],
        "size_bytes": detail["sizeInBytes"],
        "size_mb": round(detail["sizeInBytes"] / (1024 * 1024), 2),
        "partitions": detail["partitionColumns"],
        "created_at": detail["createdAt"],
        "last_modified": detail["lastModified"],
    }
