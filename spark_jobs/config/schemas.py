from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
)

# =============================================================================
# CUSTOMERS
# =============================================================================
BRONZE_CUSTOMERS_SCHEMA = StructType([ 
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

SILVER_CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False), # PK no nula
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", IntegerType(), False),
    StructField("customer_city", StringType(), False),
    StructField("customer_state", StringType(), False),
    # Metadata
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False) # Linaje de datos
])

# =============================================================================
# ORDERS
# =============================================================================
BRONZE_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_purchase_timestamp", TimestampType(), False),
    StructField("order_approved_at", TimestampType(), True), # Puede ser nulo si no se aprobó
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), False),
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])

# =============================================================================
# PRODUCTS
# =============================================================================
BRONZE_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_length", StringType(), True), # Corregido typo 'lenght'
    StructField("product_description_length", StringType(), True),
    # Estos en Bronze pueden ser String para evitar fallos de casteo inicial
    StructField("product_photos_qty", StringType(), True),
    StructField("product_weight_g", StringType(), True),
    StructField("product_length_cm", StringType(), True),
    StructField("product_height_cm", StringType(), True),
    StructField("product_width_cm", StringType(), True)
])

SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_length", IntegerType(), True),
    StructField("product_description_length", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    # Cambiados a Double para mayor precisión física
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True),
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])

# =============================================================================
# ORDER ITEMS
# =============================================================================
BRONZE_ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", StringType(), True),
    StructField("freight_value", StringType(), True),
])

SILVER_ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), False),
    StructField("price", DoubleType(), False),
    StructField("freight_value", DoubleType(), False),
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])

# =============================================================================
# GEOLOCATION
# =============================================================================
BRONZE_GEOLOCATION_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", StringType(), True),
    StructField("geolocation_lng", StringType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

SILVER_GEOLOCATION_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", IntegerType(), False),
    # Latitud y Longitud DEBEN ser Double
    StructField("geolocation_lat", DoubleType(), False),
    StructField("geolocation_lng", DoubleType(), False),
    StructField("geolocation_city", StringType(), False),
    StructField("geolocation_state", StringType(), False),
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])
# =============================================================================
# ORDER PAYMENTS
# =============================================================================
BRONZE_ORDER_PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True),
])

SILVER_ORDER_PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_installments", IntegerType(), False),
    StructField("payment_value", DoubleType(), False),
    # Metadata
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])

# =============================================================================
# ORDER REVIEWS
# =============================================================================
BRONZE_ORDER_REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
])

SILVER_ORDER_REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), False),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), False),
    StructField("review_answer_timestamp", TimestampType(), True),
    # Metadata
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])

# =============================================================================
# SELLERS
# =============================================================================
BRONZE_SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

SILVER_SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", IntegerType(), False),
    StructField("seller_city", StringType(), False),
    StructField("seller_state", StringType(), False),
    # Metadata
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])

# =============================================================================
# CATEGORY TRANSLATION
# =============================================================================
BRONZE_CATEGORY_TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])

SILVER_CATEGORY_TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), False),
    # Metadata
    StructField("_cleaned_at", TimestampType(), False),
    StructField("_is_valid", BooleanType(), False),
    StructField("_source_file", StringType(), False)
])