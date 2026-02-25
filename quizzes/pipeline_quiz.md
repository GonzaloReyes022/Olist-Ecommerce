# Quiz — Olist Data Pipeline

> **Regla única**: Cerrá todos los archivos del proyecto antes de responder.
> El objetivo no es copiar — es recuperar de tu cabeza y ejecutar.
> Si no podés responder algo, anotá "no sé" y después verificá. Eso también es aprendizaje.

---

## BLOQUE 1 — Arquitectura Medallion

**1.1** Sin mirar código: ¿cuál es la diferencia de escritura entre Bronze, Silver y Gold?
Completá la tabla de memoria:

| Capa   | Operación de escritura | ¿Por qué esa operación? |
|--------|------------------------|-------------------------|
| Bronze | append                      | no se quiere perder los regristros anteriores                     |
| Silver | Whenmatchupdate & whennotmatchinsert| se actualizan registros existentes y se insertan nuevos registros que no existían previamente                     |
| Gold   | overwrite                      | se sobreescribe todo el contenido con los datos limpios y procesados                     |
---

**1.2** Implementá de memoria un `BronzeIngester` mínimo para un dataset nuevo llamado `returns`.
- El archivo CSV se llama `returns.csv`
- La PK es `return_id`
- Las columnas críticas son `return_id` y `order_id`
- No heredés nada — escribí solo `__init__`

```python
logger = get_logger(__name__)
class BronzeReturnsIngester():
    def __init__(self,spark : SparkSession, datapath: Optional[str]= None, pk_columns: Optional[Any]=None, crit_columns: Optional[Any]=None):
        self.spark = spark
        if datapath is None:
            datapath = 'returns.csv'
        self.datapath = datapath
        if crit_columns is None:
            crit_columns = ['return_id', 'order_id']
        self.crit_columns = crit_columns
        if pk_columns is None:
            pk_columns = ['return_id']
        self.pk_columns = pk_columns

    def read_bronze(self) -> Dataframe:
        logger.info(f"Reading data from {self.datapath}")
        return (self.spark.read.format('csv')
                .option('header',True)
                .load(self.datapath))
    def validation_schema(self, df: DataFrame) -> DataFrame:
        logger.info("Validating critical columns in the dataframe")
        for col in self.crit_columns:
            if col not in df.columns:
                logger.error(f"Critical column {col} is missing in the dataframe")
                raise ValueError(f"Critical column {col} is missing in the dataframe")
        return df
    def add_metadata(self, df: DataFrame) -> DataFrame:
        logger.info("Adding metadata columns to the dataframe")
        return (df.withColumn('source_file', lit(self.datapath))
                .withColumn('batch_id', lit(str(current_timestamp())))
                .withColumn('created_at', lit(current_timestamp()))
                )

    def write_to_bronze(self, df:Dataframe, bronze_path: str)-> None:    
        logger.info(f"Writing data to bronze layer at {bronze_path} in mode 'append'")
        df.write.format('delta').mode('append').save(bronze_path)

    def run(self):
        logger.info("Starting Bronze Ingestion process")
        try:
            df = self.read_bronze()
        
            df = self.validation_schema(df)
        
            df = self.add_metadata(df)
            self.write_to_bronze(df, bronze_path=self.bronze_path)
        except Exception as e:
            logger.error(f"Error in Bronze Ingestion process: {e}")
            raise e
        logger.info("Finished Bronze Ingestion process")

        
``` 

---

**1.3** ¿Qué columnas de metadata agrega Bronze a cada registro?
Listalas de memoria con su tipo y para qué sirve cada una.
    source_ingested : de donde salio
    created_at : en que dia se creo
    batch_id : a que batch de ingesta corresponde para seguimientos 
---

**1.4** Tenés una tabla Silver de `returns`. ¿Cuál de estas condiciones en el MERGE es correcta? ¿Por qué la otra está mal?

```python
# Opción A
merge_condition = "target.return_id = source.return_id"

# Opción B — una devolución puede tener múltiples ítems
merge_condition = "target.order_id = source.order_id"
```
La B esta mal, ya que al tener una orden con multiples items, al hacer una match se mezclarian y tiraria error por coincidencia de multiples filas. La A es correcta ya que es el pk y ademas en en realidad la que necesitamos
---

## BLOQUE 2 — PySpark y Delta Lake

**2.1** ¿Qué imprime este código y por qué?

```python
@dataclass
class Config:
    root: str = "/opt"
    path: str = field(init=False)

    def _post_init_(self):  # ← ojo al detalle
        self.path = self.root + "/data"

c = Config()
print(c.path)
```
Deberia imprimir /opt/data pero no lo hace por un error de tipeo en el método _post_init_ que debería ser __post_init__. Al corregirlo, el método se ejecuta automáticamente después de la inicialización y asigna el valor correcto a c.path, permitiendo que se imprima /opt/data.
---

**2.2** Escribí de memoria una función `deduplicate` para una tabla con PK compuesta `(order_id, item_id)`.
Usá Window functions. La columna de timestamp se llama `_ingestion_timestamp`.

```python
def deduplicate(self, df: DataFrame) -> DataFrame:
    # Tu respuesta acá
    w = Windown.patitionBy(*self.pk_columns).orderBy(col('_ingestion_timestamp').desc())
    df.withColum('rn', row_number().over(w)).where('rn == 1').drop('rn')
```
#no hacer groupby pq pierdo los datos de esa forma al agrupar
---

**2.3** Este join está mal. Encontrá el bug y explicá exactamente qué pasa en el resultado:

```python
fact = items.join(orders, on="order_id", how="inner") \
            .join(payments, on="order_id", how="inner")  # payments: 1 fila por método de pago
```
¿Cuántas filas produce si una orden tiene 3 items y 2 métodos de pago?
El problema es que payments puede tener mas de un pago por orden, entonces al hacer el join se va a multiplicar la cantidad de filas por cada método de pago. Por ejemplo, si una orden tiene 3 items y 2 métodos de pago, el resultado del join sería 3 (items) x 2 (métodos de pago) = 6 filas para esa orden, lo cual no es correcto.
Sin embargo no es un bug en sí

---

**2.4** ¿Qué hace `.transform()` en PySpark? Reescribí este código SIN usar `.transform()`:

```python
df = (read_bronze()
      .transform(cast_types)
      .transform(clean_strings)
      .transform(deduplicate))
```
Transforme pasa direntamente el dataframe con el que terminaste de usar con ese metodo y se lo pasa al sigueinte, lo que hace que el codigo no sea tan anidado. Sin usar transform, el código quedaría así:
    df = read_bronze()
    df = cast_types(df)
    df = clean_strings(df)
    df = deduplicate(df)
    O de otra forma df = deduplicate(clean_strings(cast_types(read_bronze())))
```python
---

**2.5** ¿Cuándo usarías `percentile_approx` en vez de `percentile` en Spark? ¿Por qué uno y no el otro en un DataFrame distribuido?
Que percentile_aprox podes decidir vos los rangos de los percentiles y es mucho mas eficiente para grandes volúmenes de datos, ya que no requiere ordenar todo el dataset como lo hace percentile. En un DataFrame distribuido, percentile podría ser muy lento o incluso no ejecutarse debido a la cantidad de datos, mientras que percentile_approx proporcionaría una estimación rápida y razonablemente precisa.
---


**2.6** Escribí el código para obtener la `preferred_category` por cliente — la categoría que más veces compró. Usá Window + row_number. Partís de:

```python
# fact tiene columnas: customer_unique_id, product_category_name (una fila por item)
```

```python
# Tu respuesta acá
preferred_category = (fact.groupBy('customer_unique_id','product_category_name')
                        .agg(count('*').alias('cnt')))
w = Window.partitionBy('customer_unique_id').orderBy(col('cnt').desc())
preferred_category.withColumn('rn', row_number().over(w)).where('rn == 1 ').drop('rn').select('customer_unique_id','product_category_name')
```

---

## BLOQUE 3 — Silver Layer

**3.1** Listá de memoria los 5 pasos que ejecuta `SilverCleaner.run()` en orden.
No mirés el código.

```
1.read_from_bronze + validate_schmea
2.deduplicate
3.validatoin_rules 
4.add_metadata
5.write_silver

```

---

**3.2** Implementá de memoria el `__init__` de un `SilverReturnsCleaner` con estas reglas:
- Castear `return_date` a Timestamp y `refund_amount` a Double
- Limpiar `return_reason` con lower + trim
- Si `refund_amount` es null, default a 0.0
- Validar: `return_id` not null, `refund_amount >= 0`
- PK: `return_id`

```python
from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, lit, lower, trim, coalesce, expr, when
from pyspark.sql.types import DataType, TimestampType, DoubleType
from typing import Dict, List, Optional, Callable, Any

class SilverReturnsCleaner(SilverCleaner):
    def __init__(self, 
                 spark: SparkSession, 
                 bronze_path: str = None, 
                 silver_path: str = None, 
                 pk_columns: List[str] = ['return_id'],
                 crit_columns: List[str] = ['return_id', 'refund_amount'], 
                 cast_transformations: Optional[Dict[str, DataType]] = None,
                 cleaning_transformations: Optional[Dict[str, Callable[[Column], Column]]] = None,
                 default_values: Optional[Dict[str, Any]] = None,
                 validation_rules: Optional[List[str]] = None):
        
        # 1. Corrección: Pasamos 'spark' (arg), no 'self.spark'
        super().__init__(
            spark, bronze_path, silver_path, pk_columns, crit_columns, 
            cast_transformations, cleaning_transformations, default_values, validation_rules
        )
        
        # Nota: Si SilverCleaner ya asigna estas variables en su __init__, 
        # estas líneas son redundantes. Si no, están bien.
        self.spark = spark
        self.cast_transformations = cast_transformations
        self.cleaning_transformations = cleaning_transformations
        self.default_values = default_values
        self.validation_rules = validation_rules

    def cast_types(self, df: DataFrame) -> DataFrame:
        # 2. Corrección: 'transformations' en plural
        if not self.cast_transformations: return df

        for column, trans_type in self.cast_transformations.items():
            if column not in df.columns: # 3. Corrección: Faltaban los dos puntos
                continue
            
            # 4. Corrección: Asignación df = ...
            if isinstance(trans_type, TimestampType): # 5. Corrección: isinstance y TimestampType
                # Usamos .cast() nativo que es más robusto
                df = df.withColumn(column, col(column).cast(trans_type))
            else:
                df = df.withColumn(column, col(column).cast(trans_type))
        return df

    def cleaning(self, df: DataFrame) -> DataFrame:
        if not self.cleaning_transformations: return df

        for column, cleaning_trans in self.cleaning_transformations.items():
            if column in df.columns:
                # 4. Corrección: Asignación df = ...
                df = df.withColumn(column, cleaning_trans(col(column)))
        return df

    def null_handling(self, df: DataFrame) -> DataFrame:
        if not self.default_values: return df

        for column, default_value in self.default_values.items():
            if column in df.columns:
                # Esto estaba bien
                df = df.withColumn(column, coalesce(col(column), lit(default_value)))
        return df

    def validate_rules(self, df: DataFrame) -> DataFrame:
        if not self.validation_rules: return df
        
        # Esto estaba bien, crea la columna booleana
        rules = ' AND '.join(self.validation_rules)
        df = df.withColumn('is_valid', expr(rules))
        return df

    def run(self, df: DataFrame) -> DataFrame:
        # Pipeline perfecto
        df = self.cast_types(df)
        df = self.cleaning(df)
        df = self.null_handling(df)
        df = self.validate_rules(df)
        return df

# Instanciación (Corregida sintaxis menor)
silver = SilverReturnsCleaner(
    spark=spark, 
    bronze_path='/opt/data/delta/bronze/returns', 
    silver_path='/opt/data/delta/silver/returns',
    cast_transformations={'return_date': TimestampType(), 'refund_amount': DoubleType()},
    cleaning_transformations={'return_reason': lambda c: lower(trim(c))}, # Lambda perfecta
    default_values={'refund_amount': 0.0},
    validation_rules=['return_id IS NOT NULL', 'refund_amount >= 0']
)
```
**3.3** El error `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
- ¿Qué lo causa exactamente?
- ¿En qué tablas del proyecto ocurrió?
- ¿Cómo se solucionó?
Lo causa 
---

## BLOQUE 4 — Gold Layer

**4.1** La fact table tiene grain = `order_item`. Explicá:
- ¿Qué significa eso en términos concretos?
- ¿Qué se pierde si el grain fuera `order`?
- ¿Qué problema aparece si el grain fuera `customer`?

---

**4.2** Escribí las métricas derivadas que calcula la fact table (las columnas que NO vienen directamente de Silver sino que se calculan). Listalas de memoria con su fórmula.

---

**4.3** Completá el join de geolocation de memoria. El problema específico es que los nombres de columna son distintos:

```python
# customers tiene: customer_zip_code_prefix
# geolocation tiene: geolocation_zip_code_prefix

fact = fact.join(
    geolocation,
    on=???,      # ← escribí la condición correcta
    how="left"
).drop(???)      # ← ¿qué columna hay que dropear y por qué?
```

---

**4.4** Implementá `AggMonthlyMetrics.build()` de memoria. Debe retornar una fila por año-mes con: total_orders, total_revenue, avg_review_score, on_time_delivery_pct, unique_customers.

```python
def build(self, fact: DataFrame) -> DataFrame:
    # Tu respuesta acá
```

---

**4.5** ¿Qué es `dim_date` y por qué no alcanza con tener `purchase_year`/`purchase_month` en la fact table?

---

## BLOQUE 5 — ML (LTV)

**5.1** El target `ltv_segment` tiene 3 clases. ¿Cómo se calculan? ¿Qué percentiles definen cada clase?

---

**5.2** Listá de memoria los `NUMERIC_FEATURES` del modelo. No mirés el código.

```
1.
2.
3.
...
```

---

**5.3** ¿Por qué usamos `LabelEncoder` para las features categóricas y no `OneHotEncoder`?

---

**5.4** ¿Qué es un train/test split estratificado? ¿En qué situación es necesario?

---

**5.5** Implementá de memoria la función que convierte las predicciones de numpy a un Spark DataFrame y lo guarda en Delta:

```python
def save_predictions(spark, customer_ids, predictions, probabilities, output_path):
    # Pista: usá pd.DataFrame → spark.createDataFrame → write.format("delta")
    # Tu respuesta acá
```

---

**5.6** Justificá por qué elegimos XGBoost sobre KNN para este problema.
Mencioná al menos 3 razones concretas relacionadas con los datos que tenemos.

---

## BLOQUE 6 — Docker e Infraestructura

**6.1** Explicá el patrón `entrypoint + gosu` del contenedor Spark.
- ¿Por qué el entrypoint corre como root?
- ¿Qué hace `gosu spark "$@"`?
- ¿Por qué no alcanza con poner `user: "1000:1000"` en docker-compose?

---

**6.2** ¿Cuándo hay que hacer `docker compose down && docker compose up` en vez de solo `docker compose restart`? ¿Qué situación concreta pasó en este proyecto que lo requirió?

---

**6.3** ¿Qué es `/var/run/docker.sock` y por qué lo montamos en los contenedores de Airflow?

---

**6.4** Escribí el comando completo para ejecutar el script de Silver desde la terminal del host (no dentro del contenedor):

```bash
# Tu respuesta acá
```

---

## BLOQUE 7 — Airflow

**7.1** Dibujá el grafo del DAG `olist_pipeline` de memoria (con flechas → y el signo ∥ para paralelos).

```
start →
```

---

**7.2** ¿Qué hace exactamente `catchup=False` en el DAG? Describí el escenario concreto donde importa.

---

**7.3** ¿Por qué `gold_agg` y `gold_dim` pueden correr en paralelo pero `ml_train` no puede correr en paralelo con `gold_agg`?

---

**7.4** Completá el `spark_submit_cmd` de memoria:

```python
def spark_submit_cmd(script_path: str) -> str:
    return (
        # Tu respuesta acá
        # Pista: docker exec + spark-submit + flags + path
    )
```

---

**7.5** `max_active_runs=1` en el DAG. ¿Qué problema específico de datos previene?

---

## BLOQUE 8 — Debugging (encontrá el bug)

Para cada fragmento, identificá el bug y escribí la corrección.

**8.1**
```python
orders = (
    spark.read.format("delta").load(path)
    .filter(col("is_valid") == True)   # ← bug acá
    .filter(col("order_status") == "delivered")
)
```

---

**8.2**
```python
@dataclass
class PathConfig:
    root: Path = field(default_factory=lambda: Path("/opt"))
    delta_silver: Path = field(init=False)

    def _post_init_(self):           # ← bug acá
        self.delta_silver = self.root / "data" / "delta" / "silver"
```

---

**8.3**
```python
builder = SparkSession.builder.appName("test")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # esta línea está bien
builder.sparkContext.setLogLevel("WARN") # ← esta línea tiene un bug ¿cuál?
```

---

**8.4**
```python
def get_metrics(self, df):
    return df.filter(col("order_id").isNotNull()).count()  # ← bug potencial
```

¿En qué tabla falla este código y por qué?

---

## Cómo usar este quiz

1. **Cerrá todos los archivos** del proyecto
2. Respondé cada pregunta escribiendo en un archivo separado `respuestas.md`
3. Para las de código: ejecutalo de verdad contra el proyecto
4. Después de responder cada bloque, abrí el código y compará
5. Las preguntas que te costaron más → son las que más necesitás repasar
6. Repetí el quiz en una semana sin mirar las respuestas anteriores
