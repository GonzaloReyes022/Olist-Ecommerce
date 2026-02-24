"""
Gold Layer - Dimension Tables.

=== QUÉ ES UNA DIMENSION TABLE? ===
En un Data Warehouse (esquema estrella), las tablas se dividen en:
  - Fact tables:      eventos transaccionales (ej: una venta)
  - Dimension tables: entidades de referencia que describen los hechos

Las dimensiones responden preguntas del tipo:
  - ¿CUÁNDO? → dim_date
  - ¿DÓNDE?  → dim_geography
  - ¿QUIÉN?  → (podría ser dim_customer, dim_seller)

=== POR QUÉ SEPARARLAS DE LA FACT? ===
1. Evitar repetición: la info del día "2018-03-01" se guarda una vez,
   no en cada una de las 500 órdenes de ese día.
2. Enriquecer análisis: la fact tiene `purchase_year=2018, purchase_month=3`,
   pero la dim_date agrega `quarter=Q1`, `is_weekend=False`, `week_of_year=9`.
3. Facilitar joins: los BI tools (Tableau, Power BI) conectan dimensiones
   automáticamente si los nombres de columna coinciden.

=== TABLAS ===

1. DimDate (gold/dim_date)
   Una fila por fecha del rango del dataset.
   Columnas: date, year, month, day, quarter, day_of_week, is_weekend, week_of_year

2. DimGeography (gold/dim_geography)
   Una fila por estado de Brasil.
   Columnas: state, total_customers, total_orders, total_revenue, avg_lat, avg_lng
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
    current_timestamp,
    year,
    month,
    dayofmonth,
    quarter,
    dayofweek,
    weekofyear,
    when,
    lit,
    to_date,
    explode,
    sequence,
)

from spark_jobs.config.settings import config
from spark_jobs.utils.loggins_utils import get_logger

logger = get_logger(__name__)


# =============================================================================
# 1. DIM DATE
# =============================================================================

class DimDate:
    """
    Tabla de dimensión de fechas.

    Genera una fila por cada fecha en el rango del dataset.
    Útil para análisis temporal: agrupar por trimestre, semana, etc.

    Por qué generarla sintéticamente y no extraerla de la fact:
    La fact table tiene fechas de compra de órdenes — hay días sin ventas
    (fines de semana, feriados). La dim_date incluye TODOS los días del rango,
    incluso los que no tuvieron actividad. Esto permite detectar gaps.
    """

    def __init__(self, spark: SparkSession, fact_path: Optional[str] = None, gold_path: Optional[str] = None):
        self.spark = spark
        self.fact_path = fact_path or str(config.paths.delta_gold / "fact_orders")
        self.gold_path = gold_path or str(config.paths.delta_gold / "dim_date")

    def build(self) -> DataFrame:
        # Obtener rango de fechas del dataset desde la fact table
        fact = self.spark.read.format("delta").load(self.fact_path)
        date_range = fact.select(
            _min("order_purchase_timestamp").alias("min_date"),
            _max("order_purchase_timestamp").alias("max_date"),
        ).collect()[0]

        min_date = date_range["min_date"].strftime("%Y-%m-%d")
        max_date = date_range["max_date"].strftime("%Y-%m-%d")
        logger.info(f"Generando dim_date: {min_date} → {max_date}")

        # Generar secuencia de fechas: una fila por día
        # sequence(start, end, interval) genera un array, explode lo convierte en filas
        dates = self.spark.sql(f"""
            SELECT explode(sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
            )) AS date
        """)

        return (
            dates
            .withColumn("year",         year("date"))
            .withColumn("month",        month("date"))
            .withColumn("day",          dayofmonth("date"))
            .withColumn("quarter",      quarter("date"))
            # dayofweek: 1=domingo, 2=lunes, ..., 7=sábado (estándar de Spark)
            .withColumn("day_of_week",  dayofweek("date"))
            .withColumn("day_name",
                when(col("day_of_week") == 1, "Sunday")
                .when(col("day_of_week") == 2, "Monday")
                .when(col("day_of_week") == 3, "Tuesday")
                .when(col("day_of_week") == 4, "Wednesday")
                .when(col("day_of_week") == 5, "Thursday")
                .when(col("day_of_week") == 6, "Friday")
                .otherwise("Saturday")
            )
            .withColumn("is_weekend",   (col("day_of_week").isin(1, 7)).cast("boolean"))
            .withColumn("week_of_year", weekofyear("date"))
            .withColumn("year_month",   col("date").cast("string").substr(1, 7))
            .withColumn("_created_at",  current_timestamp())
        )

    def run(self) -> dict:
        start = time.time()
        logger.info("Construyendo dim_date")
        df = self.build()
        n = df.count()
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(self.gold_path)
        )
        logger.info(f"Escritos {n:,} registros en {self.gold_path}")
        return {"status": "success", "table": "dim_date", "records": n, "duration_seconds": round(time.time() - start, 2)}


# =============================================================================
# 2. DIM GEOGRAPHY
# =============================================================================

class DimGeography:
    """
    Tabla de dimensión geográfica a nivel de estado de Brasil.

    Combina:
    - Métricas de clientes y órdenes por estado (desde fact_orders)
    - Coordenadas promedio del estado (desde silver/geolocation)

    Útil para: mapas, análisis regional, detección de zonas de mayor demanda.

    Por qué agregar a estado y no a ciudad:
    Brasil tiene 5.570 municipios. A nivel de ciudad los números
    son muy pequeños para sacar conclusiones. Estado (26 + DF = 27)
    da granularidad suficiente para análisis significativos.
    """

    def __init__(
        self,
        spark: SparkSession,
        fact_path: Optional[str] = None,
        geolocation_path: Optional[str] = None,
        gold_path: Optional[str] = None,
    ):
        self.spark = spark
        self.fact_path = fact_path or str(config.paths.delta_gold / "fact_orders")
        self.geolocation_path = geolocation_path or str(config.paths.delta_silver / "geolocation")
        self.gold_path = gold_path or str(config.paths.delta_gold / "dim_geography")

    def build(self) -> DataFrame:
        fact = self.spark.read.format("delta").load(self.fact_path)

        # Métricas por estado (desde fact)
        state_metrics = (
            fact.groupBy("customer_state")
            .agg(
                countDistinct("customer_unique_id").alias("total_customers"),
                countDistinct("order_id").alias("total_orders"),
                spark_round(_sum(col("price") + col("freight_value")), 2).alias("total_revenue"),
                spark_round(avg("review_score"), 2).alias("avg_review_score"),
                spark_round(
                    avg(when(col("is_late_delivery") == False, lit(1.0)).otherwise(lit(0.0))) * 100, 1
                ).alias("on_time_delivery_pct"),
                spark_round(avg("delivery_days"), 1).alias("avg_delivery_days"),
            )
        )

        # Coordenadas promedio por estado (desde silver geolocation)
        # Se promedian lat/lng de todos los zip codes del estado
        geo = (
            self.spark.read.format("delta")
            .load(self.geolocation_path)
            .filter(col("_is_valid") == True)
            .groupBy(col("geolocation_state").alias("customer_state"))
            .agg(
                spark_round(avg("geolocation_lat"), 4).alias("avg_lat"),
                spark_round(avg("geolocation_lng"), 4).alias("avg_lng"),
            )
        )

        return (
            state_metrics
            .join(geo, on="customer_state", how="left")
            .withColumnRenamed("customer_state", "state")
            .withColumn("_created_at", current_timestamp())
            .orderBy("state")
        )

    def run(self) -> dict:
        start = time.time()
        logger.info("Construyendo dim_geography")
        df = self.build()
        n = df.count()
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(self.gold_path)
        )
        logger.info(f"Escritos {n:,} registros en {self.gold_path}")
        return {"status": "success", "table": "dim_geography", "records": n, "duration_seconds": round(time.time() - start, 2)}


# =============================================================================
# SCRIPT DE EJECUCIÓN
# =============================================================================

if __name__ == "__main__":
    from spark_jobs.utils.spark_utils import get_spark_session

    print("=" * 60)
    print("GOLD LAYER - DIMENSION TABLES")
    print("=" * 60)

    spark = get_spark_session("GoldDimTables")

    tables = [DimDate, DimGeography]

    for TableClass in tables:
        try:
            print(f"\n{'='*20} {TableClass.__name__} {'='*20}")
            table = TableClass(spark)
            result = table.run()
            print(f"  status:   {result['status']}")
            print(f"  records:  {result['records']:,}")
            print(f"  duration: {result['duration_seconds']}s")
        except Exception as e:
            print(f"  ERROR: {e}")
            raise

    spark.stop()
