# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC Ingest raw data from S3 landing zone using Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configuration
CATALOG = spark.conf.get("catalog", "ecom_prod")
SCHEMA = spark.conf.get("schema", "main")
S3_LANDING_PATH = spark.conf.get("s3_landing_path", "s3://ecommerce-landing-zone")
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Using: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables (if not exist)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    status STRING,
    total DECIMAL(10,2),
    currency STRING,
    payment_method STRING,
    billing_address STRUCT<
        first_name: STRING,
        last_name: STRING,
        address_1: STRING,
        city: STRING,
        state: STRING,
        postcode: STRING,
        country: STRING,
        email: STRING,
        phone: STRING
    >,
    line_items ARRAY<STRUCT<
        product_id: STRING,
        name: STRING,
        quantity: INT,
        price: DECIMAL(10,2),
        sku: STRING
    >>,
    _ingest_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
PARTITIONED BY (DATE(order_date))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_customers (
    customer_id STRING,
    email STRING,
    first_name STRING,
    last_name STRING,
    username STRING,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    billing STRUCT<
        address_1: STRING,
        city: STRING,
        state: STRING,
        postcode: STRING,
        country: STRING,
        phone: STRING
    >,
    shipping STRUCT<
        address_1: STRING,
        city: STRING,
        state: STRING,
        postcode: STRING,
        country: STRING
    >,
    _ingest_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_products (
    product_id STRING,
    name STRING,
    slug STRING,
    sku STRING,
    price DECIMAL(10,2),
    regular_price DECIMAL(10,2),
    sale_price DECIMAL(10,2),
    stock_quantity INT,
    stock_status STRING,
    categories ARRAY<STRUCT<id: STRING, name: STRING>>,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    _ingest_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

print("Bronze tables created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader: Ingest Orders

# COMMAND ----------

def ingest_with_autoloader(source_path, target_table, checkpoint_path, schema=None):
    """Generic Auto Loader ingestion function"""

    reader = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
    )

    if schema:
        reader = reader.schema(schema)

    df = reader.load(source_path)

    # Add metadata columns
    df = (df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    # Write stream
    query = (df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    return query

# COMMAND ----------

# Ingest Orders
print("Starting Orders ingestion...")
orders_query = ingest_with_autoloader(
    source_path=f"{S3_LANDING_PATH}/orders/",
    target_table=f"{CATALOG}.{SCHEMA}.bronze_orders",
    checkpoint_path=f"{CHECKPOINT_BASE}/bronze_orders"
)
orders_query.awaitTermination()
print("Orders ingestion complete")

# COMMAND ----------

# Ingest Customers
print("Starting Customers ingestion...")
customers_query = ingest_with_autoloader(
    source_path=f"{S3_LANDING_PATH}/customers/",
    target_table=f"{CATALOG}.{SCHEMA}.bronze_customers",
    checkpoint_path=f"{CHECKPOINT_BASE}/bronze_customers"
)
customers_query.awaitTermination()
print("Customers ingestion complete")

# COMMAND ----------

# Ingest Products
print("Starting Products ingestion...")
products_query = ingest_with_autoloader(
    source_path=f"{S3_LANDING_PATH}/products/",
    target_table=f"{CATALOG}.{SCHEMA}.bronze_products",
    checkpoint_path=f"{CHECKPOINT_BASE}/bronze_products"
)
products_query.awaitTermination()
print("Products ingestion complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Ingestion

# COMMAND ----------

# Show counts
print("\n=== Bronze Layer Summary ===")
print(f"Orders:    {spark.table('bronze_orders').count()} records")
print(f"Customers: {spark.table('bronze_customers').count()} records")
print(f"Products:  {spark.table('bronze_products').count()} records")

# COMMAND ----------

# Preview data
display(spark.table("bronze_orders").limit(5))
