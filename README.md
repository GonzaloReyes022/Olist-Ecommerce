# Olist E-Commerce Data Pipeline

End-to-end data pipeline over the [Brazilian E-Commerce dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), following Medallion Architecture (Bronze → Silver → Gold) with a customer LTV prediction model on top.

## Stack

| Layer | Technology |
|---|---|
| Processing | Apache Spark 3.5 + PySpark |
| Storage | Delta Lake 3.1 |
| Orchestration | Apache Airflow 2.8 |
| ML | XGBoost + scikit-learn |
| Infrastructure | Docker + Docker Compose |

## Architecture

```
data/raw/*.csv
      │
      ▼
  [ BRONZE ]  append — raw data + ingestion metadata
      │
      ▼
  [ SILVER ]  MERGE (upsert) — typed, deduplicated, validated
      │
      ▼
  [ GOLD  ]
  ├── fact_orders          (grain: order_item)
  ├── agg_seller_performance
  ├── agg_customer_ltv     ← feature store for ML
  ├── agg_monthly_metrics
  ├── dim_date
  └── dim_geography
      │
      ▼
  [ ML ]  XGBoost — customer LTV segment prediction (High / Medium / Low)
          output: gold/customer_ltv_predictions
```

## Project Structure

```
.
├── dags/
│   └── olist_pipeline.py       # Airflow DAG (full pipeline)
├── docker/
│   ├── airflow/                # Airflow image + requirements
│   └── spark/                  # Spark image + entrypoint
├── spark_jobs/
│   ├── bronze/ingest.py
│   ├── silver/silver.py
│   ├── gold/
│   │   ├── fact_table.py
│   │   ├── agg_tables.py
│   │   └── dim_table.py
│   ├── ml/train_ltv.py
│   ├── config/settings.py
│   └── utils/
├── scripts/
│   └── download_dataset.py     # Kaggle dataset download
├── data/
│   ├── raw/                    # source CSVs
│   └── delta/                  # Bronze / Silver / Gold Delta tables
└── docker-compose.yml
```

## Setup

### 1. Prerequisites

- Docker + Docker Compose
- Kaggle account (for dataset download)
- Python 3.10+ (only needed locally to run the download script)

### 2. Docker group GID

The Airflow container needs access to the Docker socket. Check your GID and update `docker/airflow/Dockerfile` if it differs from `968`:

```bash
getent group docker | cut -d: -f3
```

Edit line in `docker/airflow/Dockerfile`:
```dockerfile
RUN groupmod -g <YOUR_GID> docker 2>/dev/null || groupadd -g <YOUR_GID> docker && \
    usermod -aG docker airflow
```

### 3. Build and start

```bash
docker compose build
docker compose up -d
```

### 4. Download the dataset

Requires a [Kaggle API token](https://www.kaggle.com/settings/account) in `~/.kaggle/kaggle.json`.

```bash
pip install kagglehub
python scripts/download_dataset.py
```

This downloads the dataset and places the CSVs in `data/raw/` with the names expected by the pipeline.

---

## Running the pipeline

### Manually (layer by layer)

Each layer is an independent `spark-submit` job running inside the Spark container.

```bash
# Bronze — ingest raw CSVs into Delta (append)
docker compose exec spark /opt/spark/bin/spark-submit --master local[*] /opt/spark_jobs/bronze/ingest.py

# Silver — clean, deduplicate, validate (MERGE)
docker compose exec spark /opt/spark/bin/spark-submit --master local[*] /opt/spark_jobs/silver/silver.py

# Gold — fact table
docker compose exec spark /opt/spark/bin/spark-submit --master local[*] /opt/spark_jobs/gold/fact_table.py

# Gold — aggregate tables (seller performance, customer LTV, monthly metrics)
docker compose exec spark /opt/spark/bin/spark-submit --master local[*] /opt/spark_jobs/gold/agg_tables.py

# Gold — dimension tables (date, geography)
docker compose exec spark /opt/spark/bin/spark-submit --master local[*] /opt/spark_jobs/gold/dim_table.py

# ML — train XGBoost and score all customers
docker compose exec spark /opt/spark/bin/spark-submit --master local[*] /opt/spark_jobs/ml/train_ltv.py
```

### Via Airflow

The DAG `olist_pipeline` orchestrates the full pipeline in the correct order with parallelism where possible:

```
bronze → silver → gold_fact → [gold_agg ∥ gold_dim] → ml_train
```

**Steps:**

1. Open the Airflow UI: [http://localhost:8080](http://localhost:8080)
   - User: `admin` / Password: `admin`
2. Find the DAG `olist_pipeline`
3. Toggle it from paused → active
4. Click **Trigger DAG** (▶) to run immediately

The DAG is scheduled `@daily`. Use `catchup=False` so it only runs on trigger, not retroactively.

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:8081 | — |

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist** — ~100k orders from 2016–2018.

| Table | Rows |
|---|---|
| orders | 99,441 |
| customers | 99,441 |
| order_items | 112,650 |
| order_payments | 103,886 |
| order_reviews | 102,885 |
| products | 32,951 |
| sellers | 3,095 |
| geolocation | ~1M (19k unique zip codes after Silver) |

Source: [kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Useful commands

```bash
# Check container status
docker compose ps

# View logs for a specific container
docker compose logs spark -f
docker compose logs airflow-scheduler -f

# Restart everything after config changes
docker compose down && docker compose up -d

# Rebuild images after Dockerfile or requirements changes
docker compose down
docker compose build --no-cache
docker compose up -d

# Open a shell inside the Spark container
docker compose exec spark bash

# Check Delta table contents (from inside the Spark container)
docker compose exec spark python3 -c "
from spark_jobs.utils.spark_utils import get_spark_session
spark = get_spark_session('inspect')
spark.read.format('delta').load('/opt/data/delta/gold/fact_orders').show(5)
"
```
