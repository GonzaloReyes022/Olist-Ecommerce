"""
Descarga el dataset de Olist desde Kaggle y lo coloca en data/raw/.

Uso:
    python scripts/download_dataset.py
"""
import kagglehub
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

def main():
    print("Descargando dataset Olist desde Kaggle...")
    dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    print(f"Dataset descargado en: {dataset_path}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Mapeo: nombre en Kaggle -> nombre que espera el pipeline
    file_mapping = {
        "olist_orders_dataset.csv": "orders.csv",
        "olist_customers_dataset.csv": "customers.csv",
        "olist_order_items_dataset.csv": "order_items.csv",
        "olist_products_dataset.csv": "products.csv",
        "olist_order_payments_dataset.csv": "order_payments.csv",
        "olist_geolocation_dataset.csv": "geolocation.csv",
        "olist_sellers_dataset.csv": "sellers.csv",
        "olist_order_reviews_dataset.csv": "order_reviews.csv",
    }

    src_dir = Path(dataset_path)
    copied = 0
    for kaggle_name, local_name in file_mapping.items():
        src = src_dir / kaggle_name
        if src.exists():
            shutil.copy2(src, RAW_DIR / local_name)
            size_mb = src.stat().st_size / (1024 * 1024)
            print(f"  {kaggle_name} -> {local_name} ({size_mb:.1f} MB)")
            copied += 1
        else:
            print(f"  WARN: {kaggle_name} no encontrado en {src_dir}")

    # Copiar cualquier otro CSV que no esté en el mapeo
    for csv_file in src_dir.glob("*.csv"):
        if csv_file.name not in file_mapping:
            shutil.copy2(csv_file, RAW_DIR / csv_file.name)
            print(f"  {csv_file.name} (extra)")
            copied += 1

    print(f"\n{copied} archivos copiados a {RAW_DIR}")

if __name__ == "__main__":
    main()
