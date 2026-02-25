"""
DAG principal del pipeline Olist E-Commerce.

=== ORQUESTACIÓN ===
Airflow no procesa datos — coordina CUÁNDO y EN QUÉ ORDEN
se ejecutan los jobs de Spark.

Flujo del pipeline:
  [bronze] → [silver] → [gold_fact] → [gold_agg] → [gold_dim] → [ml_train]

Cada flecha = dependencia: la tarea siguiente no empieza hasta
que la anterior termine con éxito.

=== SCHEDULE ===
@daily → corre una vez por día a medianoche UTC.
En Olist los datos son históricos (no streaming), pero en producción
real el pipeline correría diario para procesar nuevas órdenes.

=== OPERADORES ===
BashOperator con `docker exec`:
  - Airflow (liviano) no tiene Spark instalado
  - El contenedor `spark` ya tiene todo: JARs, Python packages, datos
  - `docker exec` ejecuta un comando en un contenedor existente
  - Necesita el socket de Docker montado en Airflow (/var/run/docker.sock)

Por qué NO SparkSubmitOperator:
  - Requiere Spark instalado en el contenedor de Airflow (+500MB imagen)
  - Requiere Java en Airflow
  - Más configuración para poca ganancia en este setup local

=== MANEJO DE FALLOS ===
  retries=1:       reintenta una vez si la tarea falla
  retry_delay=5min: espera 5 minutos entre reintentos
  email_on_failure=False: desactivado (no hay servidor SMTP)

=== CÓMO VER EL DAG ===
  http://localhost:8080 → usuario: admin, password: admin
  Buscar "olist_pipeline"
  Activar el toggle (pausa → activo)
  Trigger manualmente con el botón "play"
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# =============================================================================
# CONFIGURACIÓN DEL CONTENEDOR SPARK
# =============================================================================

# Nombre del contenedor Spark (generado por docker-compose)
# Formato: {nombre_proyecto}_{servicio}_{índice}
# Si tu proyecto se llama distinto, ajustá este nombre.
# Para verificar: `docker ps --format "{{.Names}}" | grep spark`
SPARK_CONTAINER = "olist-ecommerce-spark-1"

# Comando base para ejecutar un spark-submit dentro del contenedor
def spark_submit_cmd(script_path: str) -> str:
    """
    Genera el comando docker exec + spark-submit para un script.

    --master local[*]:  usa todos los cores del contenedor (no Spark cluster)
    --driver-memory 2g: memoria para el driver Spark

    Por qué local[*] y no spark://spark:7077:
    El contenedor spark corre como Spark Master, pero nuestros jobs
    no necesitan workers distribuidos — con local[*] el driver mismo
    procesa los datos usando todos los CPUs del contenedor.

    Por qué /opt/spark/bin/spark-submit y no solo spark-submit:
    `docker exec` no hereda el PATH del contenedor. Hay que usar la ruta
    absoluta para que el binario sea encontrado.
    """
    return (
        f"docker exec {SPARK_CONTAINER} "
        f"/opt/spark/bin/spark-submit --master local[*] --driver-memory 2g "
        f"{script_path}"
    )

# =============================================================================
# ARGUMENTOS POR DEFECTO DEL DAG
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    # Si depende_on_past=True, el DAG de hoy no corre si el de ayer falló.
    # False = cada ejecución es independiente.

    "start_date": datetime(2024, 1, 1),
    # Fecha desde la cual Airflow empieza a programar el DAG.
    # No significa que corra automáticamente retroactivamente
    # (catchup=False lo evita).

    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================

with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    description="Pipeline Medallion: Bronze → Silver → Gold → ML",
    schedule="@daily",
    catchup=False,
    # catchup=False: si el DAG estuvo pausado varios días, NO ejecuta
    # los runs atrasados. Solo corre "ahora".
    tags=["olist", "medallion", "spark", "delta"],
    max_active_runs=1,
    # Solo una ejecución activa a la vez.
    # Evita que dos runs corran en paralelo y pisen los mismos datos.
):

    # -------------------------------------------------------------------------
    # START / END (marcadores visuales en la UI de Airflow)
    # -------------------------------------------------------------------------

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # -------------------------------------------------------------------------
    # BRONZE: Ingesta de CSVs → Delta Lake (append)
    # -------------------------------------------------------------------------

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=spark_submit_cmd("/opt/spark_jobs/bronze/ingest.py"),
        doc_md="""
        **Bronze Layer**
        Lee los CSVs crudos de `/opt/data/raw/` y los escribe en Delta Lake
        (append). Agrega metadata: `_ingestion_timestamp`, `_source_file`.
        """,
    )

    # -------------------------------------------------------------------------
    # SILVER: Limpieza y validación → Delta Lake (MERGE/upsert)
    # -------------------------------------------------------------------------

    silver_cleaning = BashOperator(
        task_id="silver_cleaning",
        bash_command=spark_submit_cmd("/opt/spark_jobs/silver/silver.py"),
        doc_md="""
        **Silver Layer**
        Limpia tipos, estandariza strings, deduplica, valida reglas de negocio.
        Escribe con MERGE (upsert) para ser idempotente.
        8 datasets: orders, customers, order_items, products,
        order_payments, geolocation, sellers, order_reviews.
        """,
    )

    # -------------------------------------------------------------------------
    # GOLD: Fact Table
    # -------------------------------------------------------------------------

    gold_fact = BashOperator(
        task_id="gold_fact_orders",
        bash_command=spark_submit_cmd("/opt/spark_jobs/gold/fact_table.py"),
        doc_md="""
        **Gold - Fact Table**
        Construye `fact_orders`: JOIN desnormalizado de todas las Silver.
        Grain: una fila por order_item.
        Agrega métricas: delivery_days, delay_days, is_late_delivery.
        """,
    )

    # -------------------------------------------------------------------------
    # GOLD: Aggregate Tables (pueden correr en paralelo entre sí)
    # -------------------------------------------------------------------------

    gold_agg = BashOperator(
        task_id="gold_agg_tables",
        bash_command=spark_submit_cmd("/opt/spark_jobs/gold/agg_tables.py"),
        doc_md="""
        **Gold - Aggregate Tables**
        Construye 3 tablas pre-calculadas desde fact_orders:
          - agg_seller_performance: métricas por seller
          - agg_customer_ltv: RFM + LTV segment (feature store para ML)
          - agg_monthly_metrics: evolución mensual del negocio
        """,
    )

    gold_dim = BashOperator(
        task_id="gold_dim_tables",
        bash_command=spark_submit_cmd("/opt/spark_jobs/gold/dim_table.py"),
        doc_md="""
        **Gold - Dimension Tables**
        Construye 2 tablas de dimensión:
          - dim_date: calendario completo del rango del dataset
          - dim_geography: métricas por estado de Brasil + coordenadas
        """,
    )

    # -------------------------------------------------------------------------
    # ML: Entrenamiento y scoring del modelo LTV
    # -------------------------------------------------------------------------

    ml_train = BashOperator(
        task_id="ml_train_ltv",
        bash_command=spark_submit_cmd("/opt/spark_jobs/ml/train_ltv.py"),
        doc_md="""
        **ML - Predicción LTV**
        Lee agg_customer_ltv (feature store), entrena XGBoost multiclase
        para predecir segmento LTV (High/Medium/Low).
        Guarda predicciones en gold/customer_ltv_predictions.
        """,
    )

    # =========================================================================
    # DEPENDENCIAS (el grafo del pipeline)
    # =========================================================================
    #
    # Diagrama:
    #
    #   start
    #     │
    #   bronze_ingestion
    #     │
    #   silver_cleaning
    #     │
    #   gold_fact_orders
    #     │
    #   ┌──────────────┐
    #   gold_agg_tables  gold_dim_tables   ← corren en paralelo
    #   └──────────────┘
    #         │
    #   ml_train_ltv    ← depende solo de agg (usa agg_customer_ltv)
    #         │
    #        end
    #
    # Por qué gold_agg y gold_dim en paralelo:
    # Ambas leen de fact_orders (ya escrita). No dependen entre sí.
    # Airflow los ejecuta simultáneamente → ahorra tiempo.
    #
    # Por qué ml_train depende solo de gold_agg y no de gold_dim:
    # El modelo lee agg_customer_ltv. gold_dim no produce datos que el ML necesite.
    # Pero ml_train igualmente espera a que AMBAS agg y dim terminen antes de
    # continuar hacia end, para que el pipeline quede completo.

    start >> bronze_ingestion >> silver_cleaning >> gold_fact >> [gold_agg, gold_dim]  # type: ignore[operator]
    gold_agg >> ml_train
    [ml_train, gold_dim] >> end  # type: ignore[operator]
