"""
ML Pipeline: Predicción de segmento LTV de cliente.

=== PROBLEMA DE NEGOCIO ===
Dado el comportamiento histórico de un cliente, ¿en qué segmento de
Lifetime Value cae?

  High   → top 25% por gasto total
  Medium → percentil 25-75
  Low    → bottom 25%

=== POR QUÉ ES ÚTIL? ===
Permite acciones diferenciadas:
  - High:   programas de fidelidad, atención preferencial
  - Medium: campañas de up-sell para convertirlos en High
  - Low:    promociones de reactivación o reducir costos de atención

=== FEATURES (de gold/agg_customer_ltv) ===
  Numéricas:
    recency_days          → ¿cuándo compró por última vez?
    frequency             → ¿cuántas órdenes hizo?
    avg_order_value       → ticket promedio
    total_items           → cantidad total de productos
    avg_review_score_given → ¿qué tan satisfecho estuvo?
    pct_late_deliveries   → % de envíos que llegaron tarde
    avg_delivery_days     → tiempo promedio de entrega

  Categóricas (Label Encoding):
    preferred_category    → categoría más comprada
    preferred_payment     → método de pago preferido
    customer_state        → estado geográfico

TARGET: ltv_segment (Low=0, Medium=1, High=2)

=== POR QUÉ XGBOOST? ===
  - Maneja bien features mixtas (numéricas + categóricas encodadas)
  - Robusto a outliers y valores nulos
  - Feature importances interpretables (saber qué factores predicen LTV)
  - Rápido para 100k registros
  - No requiere normalización (a diferencia de SVM/KNN)

=== PIPELINE ===
  1. Leer gold/agg_customer_ltv → Spark DataFrame
  2. Convertir a pandas (100k filas caben en memoria)
  3. Preprocesar (encoding, imputación)
  4. Train/test split (80/20 estratificado)
  5. Entrenar XGBoost
  6. Evaluar (accuracy, classification report, feature importances)
  7. Generar predicciones para todos los clientes
  8. Guardar predicciones en gold/customer_ltv_predictions (Delta)

=== OUTPUT ===
  gold/customer_ltv_predictions:
    customer_unique_id, predicted_segment, prediction_probability,
    model_version, _scored_at
"""

import time
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from sklearn.experimental import enable_halving_search_cv  # noqa: F401 — requerido en sklearn < 2.0
from sklearn.model_selection import StratifiedKFold, HalvingRandomSearchCV, train_test_split # type: ignore
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from xgboost import XGBClassifier

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

from spark_jobs.config.settings import config
from spark_jobs.utils.loggins_utils import get_logger

logger = get_logger(__name__)

# Columnas que se usan como features
NUMERIC_FEATURES = [
    "recency_days",
    "frequency",
    "avg_order_value",
    "total_items",
    "avg_review_score_given",
    "pct_late_deliveries",
    "avg_delivery_days",
]

CATEGORICAL_FEATURES = [
    "preferred_category",
    "preferred_payment",
    "customer_state",
]

TARGET = "ltv_segment"
LABEL_MAP = {"Low": 0, "Medium": 1, "High": 2}
LABEL_MAP_INV = {v: k for k, v in LABEL_MAP.items()}

MODEL_VERSION = datetime.now().strftime("%Y%m%d_%H%M%S")


# =============================================================================
# 1. CARGA Y PREPROCESAMIENTO
# =============================================================================

def load_features(spark: SparkSession, ltv_path: str) -> pd.DataFrame:
    """
    Lee gold/agg_customer_ltv y convierte a pandas.

    Por qué convertir a pandas y no usar Spark MLlib:
    - Con 100k clientes (una fila por cliente), el DataFrame cabe en memoria
    - XGBoost de sklearn es más maduro y tiene más opciones que Spark MLlib
    - En producción con millones de clientes usar XGBoost en Spark (spark-xgboost)
    """
    logger.info(f"Cargando features desde {ltv_path}")
    df = (
        spark.read.format("delta")
        .load(ltv_path)
        .select(
            "customer_unique_id",
            *NUMERIC_FEATURES,
            *CATEGORICAL_FEATURES,
            TARGET,
        )
        .toPandas()
    )
    logger.info(f"Cargados {len(df):,} clientes")
    return df

def target_preprocess(dataset: pd.DataFrame) -> pd.DataFrame:
    # Label Encoding para categóricas del target
    # Por qué LabelEncoder y no OneHotEncoder:
    # XGBoost maneja ordinales internamente vía splits en árboles.
    # One-hot expandiría el feature space innecesariamente.

    dataset[TARGET] = dataset[TARGET].map(LABEL_MAP)
    return dataset

def build_preprocessor_pipeline() -> ColumnTransformer:
    """
    Creacion de Pipeline que preprocesa features para XGBoost.

    Pasos:
    1. Imputar nulos numéricos con mediana (robusto a outliers)
    2. Imputar nulos categóricos con "Unknown"
    3. Label Encoding para categóricas (XGBoost nativo acepta enteros)
    4. Encodear target (Low=0, Medium=1, High=2)

    Retorna el DataFrame procesado y los encoders para aplicar
    el mismo preprocesamiento a datos nuevos.
    """
    
    # Imputar nulos numéricos con mediana
    logger.info(f"Numerical Pipeline")
    numerical_pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])

    # Imputar nulos categóricos con "Unknown"
    logger.info(f"Categorical Pipeline")
    categorical_pipeline = Pipeline([
    # Paso A: Rellenar nulos con "Unknown"
    ('imputer', SimpleImputer(strategy='constant', fill_value='Unknown')),
    
    # Paso B: Transformar Strings a Números (Ordinal)
    # handle_unknown='use_encoded_value' es VITAL para producción.
    # Si llega una categoría nueva que no viste en el train, le pone -1
    ('ordinal', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1))
    ])

    # 3. El ColumnTransformer final
    logger.info(f"ColumnTransformer")
    preprocessor = ColumnTransformer([
    # (nombre, transformador, columnas)
    ('num', numerical_pipeline, NUMERIC_FEATURES),
    ('cat', categorical_pipeline, CATEGORICAL_FEATURES)
    ], verbose_feature_names_out=False) # Para que no te cambie los nombres de columnas


    
    
    return preprocessor




# =============================================================================
# 2. BÚSQUEDA DE HIPERPARÁMETROS
# =============================================================================

def build_search(full_pipeline: Pipeline, skf: StratifiedKFold) -> HalvingRandomSearchCV:
    """
    Configura HalvingRandomSearchCV sobre el pipeline completo.

    Por qué HalvingRandomSearchCV vs GridSearch/RandomSearch:
    - GridSearch evalúa TODAS las combinaciones → muy lento
    - RandomSearch evalúa N aleatorias → mejor, pero sin eliminar malos
    - HalvingRandomSearchCV (Successive Halving):
        * Ronda 1: evalúa muchos candidatos con POCOS datos (25% del train)
        * Ronda 2: elimina los peores (factor=3 → queda 1/3) y usa más datos
        * Ronda N: queda 1 candidato evaluado con todos los datos
      → Converge al mejor candidato con muchos menos fits totales

    Los parámetros van prefijados con el nombre del step en el Pipeline:
    'model__' porque el step se llama 'model'.
    Si el step fuera 'clf', serían 'clf__n_estimators', etc.
    """
    param_distributions = {
        "model__n_estimators":      [100, 200, 300, 500],
        "model__max_depth":         [3, 4, 5, 6, 8],
        "model__learning_rate":     [0.01, 0.05, 0.1, 0.2],
        "model__subsample":         [0.6, 0.7, 0.8, 0.9],
        "model__colsample_bytree":  [0.6, 0.7, 0.8, 0.9],
        "model__min_child_weight":  [1, 3, 5],
        "model__gamma":             [0, 0.1, 0.3],
    }

    return HalvingRandomSearchCV(
        full_pipeline,
        param_distributions,
        cv=skf,
        n_candidates=50,   # sin esto el default "exhaust" genera >2000 candidatos (~12k fits)
        factor=3,          # cada ronda descarta 2/3: 50→17→6→2→1
        scoring="accuracy",
        n_jobs=-1,
        random_state=42,
        verbose=1,
    )


# =============================================================================
# 3. EVALUACIÓN
# =============================================================================

def evaluate(pipeline: Pipeline, X_test: pd.DataFrame, y_test: pd.Series) -> float:
    """
    Evalúa el pipeline completo (preprocessor + modelo) sobre el test set.

    Recibe un Pipeline, no un XGBClassifier directo.
    - pipeline.predict(X_test): aplica preprocessor.transform() + model.predict()
      El preprocessor NO se re-fitea — usa los parámetros aprendidos en train.
    - Las feature importances se extraen del step 'model' dentro del pipeline:
      pipeline.named_steps['model'].feature_importances_
    """
    y_pred = pipeline.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    logger.info(f"Accuracy en test: {acc:.4f} ({acc*100:.2f}%)")

    print("\n--- Classification Report ---")
    print("PRESICION: ¿De todos los que predije como positivos, ¿cuántos eran realmente positivos? \n ")
    print("RECALL: De todos los positivos reales, ¿cuántos detecté?  \n")
    print("F1 score: Promedio armónico: castiga fuerte cuando una de las dos (precision/recall) es baja")
    print(classification_report(y_test, y_pred, target_names=["Low", "Medium", "High"]))

    print("\n--- Confusion Matrix ---")
    cm = confusion_matrix(y_test, y_pred)
    print(pd.DataFrame(cm, index=["Low", "Medium", "High"], columns=["Low", "Medium", "High"]))

    print("\n--- Feature Importances (Top 10) ---")
    xgb_step = pipeline.named_steps["model"]
    importances = pd.Series(
        xgb_step.feature_importances_,
        index=NUMERIC_FEATURES + CATEGORICAL_FEATURES,
    )
    print(importances.sort_values(ascending=False).head(10).to_string())

    return float(acc)


# =============================================================================
# 4. PREDICCIONES → GOLD DELTA TABLE
# =============================================================================

def save_predictions(
    spark: SparkSession,
    customer_ids: pd.Series,
    predictions: np.ndarray,
    probabilities: np.ndarray,
    output_path: str,
) -> int:
    """
    Guarda las predicciones en una tabla Delta en Gold.

    Por qué guardar en Delta y no en CSV:
    - Permite versionado (Time Travel) para auditar cambios de predicciones
    - Integra con el resto del pipeline
    - Soporta ACID: si el scoring falla a mitad, no quedan datos corruptos

    Schema:
      customer_unique_id:       ID del cliente
      predicted_segment:        "Low", "Medium" o "High"
      score_high:               probabilidad de ser "High"
      score_medium:             probabilidad de ser "Medium"
      score_low:                probabilidad de ser "Low"
      model_version:            timestamp de cuándo se entrenó el modelo
      _scored_at:               timestamp de la predicción
    """
    pred_df = pd.DataFrame({
        "customer_unique_id": customer_ids.values,
        "predicted_segment": [LABEL_MAP_INV[p] for p in predictions],
        "score_low": probabilities[:, 0].round(4),
        "score_medium": probabilities[:, 1].round(4),
        "score_high": probabilities[:, 2].round(4),
        "model_version": MODEL_VERSION,
    })

    # Convertir de pandas a Spark y escribir en Delta
    spark_df = spark.createDataFrame(pred_df)
    spark_df = spark_df.withColumn("_scored_at", current_timestamp())

    n = spark_df.count()
    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )
    logger.info(f"Predicciones guardadas: {n:,} clientes en {output_path}")
    return n


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================

def run_ltv_pipeline(spark: SparkSession) -> dict:
    """
    Ejecuta el pipeline completo de ML.

    Flujo:
    1. Cargar features desde Gold
    2. Preprocesar
    3. Split train/test (80/20 estratificado por target)
    4. Entrenar XGBoost
    5. Evaluar
    6. Scorear TODOS los clientes (no solo el test set)
    7. Guardar predicciones en Gold Delta

    Por qué scorear todos y no solo el test:
    En producción queremos predicciones para TODOS los clientes,
    no solo para evaluar el modelo. El test set es solo para medir calidad.
    """
    start = time.time()
    ltv_path = str(config.paths.delta_gold / "agg_customer_ltv")
    predictions_path = str(config.paths.delta_gold / "customer_ltv_predictions")

    # 1. Cargar
    df_raw = load_features(spark, ltv_path)

    # 2. Encodear target y separar X / y
    df = target_preprocess(df_raw)
    # Eliminar filas donde ltv_segment era NULL en Delta (→ NaN tras .map()).
    # No deben usarse para entrenar ni evaluar.
    n_before = len(df)
    df = df.dropna(subset=[TARGET])
    n_dropped = n_before - len(df)
    if n_dropped:
        logger.warning(f"Descartados {n_dropped:,} clientes con ltv_segment nulo")

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y = df[TARGET]
    customer_ids = df["customer_unique_id"]

    print(f"\nDistribución de clases ({len(df):,} clientes):")
    for label, cnt in df[TARGET].value_counts().sort_index().items():
        print(f"  {LABEL_MAP_INV[int(label)]}: {cnt:,} ({cnt/len(df)*100:.1f}%)") # type: ignore

    # 3. Train/test split — X_test queda COMPLETAMENTE AISLADO hasta evaluate()
    # El preprocessor nunca ve X_test durante el fit → sin data leakage
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    logger.info(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

    # 4. Pipeline completo: preprocessor + modelo
    # Al pasarlo a HalvingRandomSearchCV, en cada fold interno:
    #   - preprocessor.fit_transform(X_fold_train) → aprende mediana, scaler, ordinal
    #   - preprocessor.transform(X_fold_val)       → aplica sin re-fitear
    preprocessor = build_preprocessor_pipeline()
    full_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("model", XGBClassifier(
            objective="multi:softmax",
            num_class=3,
            eval_metric="mlogloss",
            random_state=42,
            n_jobs=-1,
        )),
    ])

    # 5. StratifiedKFold + HalvingRandomSearchCV
    # skf se pasa como cv= → la búsqueda usa solo X_train para todos los folds
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    search = build_search(full_pipeline, skf)

    logger.info("Iniciando HalvingRandomSearchCV...")
    search.fit(X_train, y_train)

    logger.info(f"Mejor score CV: {search.best_score_:.4f}")
    print(f"\nMejor score CV: {search.best_score_*100:.2f}%")
    print(f"Mejores params: {search.best_params_}")

    # 6. Evaluar en X_test con el mejor pipeline encontrado
    best_pipeline = search.best_estimator_
    acc = evaluate(best_pipeline, X_test, y_test)

    # 7. Scorear TODOS los clientes (train + test) con el mejor modelo
    all_predictions = best_pipeline.predict(X)
    all_probabilities = best_pipeline.predict_proba(X)

    # 8. Guardar predicciones en Gold Delta
    n = save_predictions(spark, customer_ids, all_predictions, all_probabilities, predictions_path)

    duration = round(time.time() - start, 2)
    return {
        "status": "success",
        "total_customers": n,
        "cv_best_score": round(float(search.best_score_), 4),
        "test_accuracy": round(acc, 4),
        "best_params": search.best_params_,
        "model_version": MODEL_VERSION,
        "duration_seconds": duration,
    }


# =============================================================================
# SCRIPT DE EJECUCIÓN
# =============================================================================

if __name__ == "__main__":
    from spark_jobs.utils.spark_utils import get_spark_session

    print("=" * 60)
    print("ML - PREDICCION LTV DE CLIENTES")
    print("=" * 60)

    spark = get_spark_session("MLTrainLTV")

    try:
        result = run_ltv_pipeline(spark)

        print(f"\n  status:      {result['status']}")
        print(f"  clientes:    {result['total_customers']:,}")
        print(f"  cv score:    {result['cv_best_score']*100:.2f}%")
        print(f"  test acc:    {result['test_accuracy']*100:.2f}%")
        print(f"  version:     {result['model_version']}")
        print(f"  duration:    {result['duration_seconds']}s")
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
    finally:
        spark.stop()
