"""
Configuración centralizada del proyecto.

Principios:
- Un solo lugar para todas las configuraciones
- Fácil de cambiar entre ambientes (dev/prod)
- Documentado y tipado
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any
import os

@dataclass
class PathConfig:
    root : Path = field(default_factory= lambda: Path(__file__).parent.parent.parent)

    data_raw : Path =field(init=False)
    delta_bronze : Path =field(init=False)
    delta_silver : Path =field(init=False)
    delta_gold : Path =field(init=False)

    def _post_init_(self):
        self.data_raw = self.root / "data" / "raw"
        self.delta_bronze = self.root / "data" / "delta" / "bronze"
        self.delta_silver = self.root / "data" / "delta" / "silver"
        self.delta_gold = self.root / "data" / "delta" / "gold"

        #Creamos directorios si no existen
        for path in [self.data_raw, self.delta_bronze, self.delta_silver, self.delta_gold]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class SparkConfig:
    """Configuración de Spark."""
    #Identidad y Control
    app_name : str = "EcommercePipeline"
    master : str = "local[*]"

    #Gestión de Memoria
    driver_memory : str = "3g"
    driver_max_result_size :str = "2g"

    #Rendimiento y Paralelismo 
    shuffle_partition : int = 10
    adaptative_enabled : str = "true"
    parallelism : int = 10
    parquet_compression : str = "snappy"

    #Delta lake
    delta_extensions : str = "io.delta.sql.DeltaSparkSessionExtension"
    delta_catalog : str = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    #Conectividad (Snowflake) y Archivos
    #jars.packages : str =  
    # sources_partitionOverwriteMode : str = "dynamic"
    def to_spark_conf(self) -> Dict[str, Any]:
        """Convierte a diccionario de configuración de Spark."""
        return {
            "spark.master" : self.master,
            "spark.driver.memory" : self.driver_memory,
            "spark.driver.maxResultSize" : self.driver_max_result_size,
            "spark.sql.shuffle.partitions" : str(self.shuffle_partition),
            "spark.sql.adaptive.enabled" : self.adaptative_enabled,
            "spark.sql.parallelism" : str(self.parallelism),
            "spark.sql.parquet.compression.codec" : self.parquet_compression,
            "spark.sql.extensions" : self.delta_extensions,
            "spark.sql.catalog.spark_catalog" : self.delta_catalog,
        }   

@dataclass
class QualityConfig:
    """Configuración de calidad de datos."""

    # Thresholds de validación
    min_completeness: float = 0.95  # 95% de campos no nulos
    max_duplicates_pct: float = 0.01  # Máximo 1% duplicados
    max_data_age_hours: int = 48  # Datos no más viejos que 48 horas

    # Alertas
    alert_on_failure: bool = True
    alert_email: str = "gonzaloreyes022@gmail.com"

@dataclass
class PipelineConfig:
    """Configuración general del pipeline."""

    paths: PathConfig = field(default_factory=PathConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)

    # Ambiente
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


# Instancia global de configuración
config = PipelineConfig()

