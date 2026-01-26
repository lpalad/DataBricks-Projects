# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Orders
# MAGIC Clean and transform order data with line item explosion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = spark.conf.get("catalog", "ecom_prod")
SCHEMA = spark.conf.get("schema", "main")
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Orders Tables

# COMMAND ----------

# Main orders table (header level)
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    order_date_key INT,
    status STRING,
    total DECIMAL(10,2),
    currency STRING,
    payment_method STRING,
    billing_first_name STRING,
    billing_last_name STRING,
    billing_city STRING,
    billing_state STRING,
    billing_country STRING,
    billing_email STRING,
    item_count INT,
    _updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (order_date_key)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

# Order line items (exploded)
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_order_items (
    order_id STRING,
    order_item_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    product_id STRING,
    product_name STRING,
    sku STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    _updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Orders CDC

# COMMAND ----------

def process_orders_cdc(batch_df, batch_id):
    """Process orders - header and line items"""

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No records to process")
        return

    # Deduplicate within batch
    window = Window.partitionBy("order_id").orderBy(F.col("_ingest_timestamp").desc())
    deduped_df = (batch_df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # ========== Silver Orders (Header) ==========
    orders_df = (deduped_df
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("order_date"),
            F.date_format("order_date", "yyyyMMdd").cast("int").alias("order_date_key"),
            F.col("status"),
            F.col("total"),
            F.col("currency"),
            F.col("payment_method"),
            F.col("billing_address.first_name").alias("billing_first_name"),
            F.col("billing_address.last_name").alias("billing_last_name"),
            F.col("billing_address.city").alias("billing_city"),
            F.col("billing_address.state").alias("billing_state"),
            F.col("billing_address.country").alias("billing_country"),
            F.col("billing_address.email").alias("billing_email"),
            F.size("line_items").alias("item_count"),
            F.current_timestamp().alias("_updated_at")
        )
    )

    # MERGE orders header
    if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.silver_orders"):
        orders_target = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.silver_orders")
        (orders_target.alias("target")
            .merge(orders_df.alias("source"), "target.order_id = source.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        orders_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_orders")

    # ========== Silver Order Items (Exploded) ==========
    items_df = (deduped_df
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("order_date"),
            F.explode("line_items").alias("item")
        )
        .select(
            F.col("order_id"),
            F.concat_ws("-", F.col("order_id"), F.col("item.product_id")).alias("order_item_id"),
            F.col("customer_id"),
            F.col("order_date"),
            F.col("item.product_id").alias("product_id"),
            F.col("item.name").alias("product_name"),
            F.col("item.sku").alias("sku"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.price").alias("unit_price"),
            (F.col("item.quantity") * F.col("item.price")).alias("line_total"),
            F.current_timestamp().alias("_updated_at")
        )
    )

    # MERGE order items
    if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.silver_order_items"):
        items_target = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.silver_order_items")
        (items_target.alias("target")
            .merge(items_df.alias("source"), "target.order_item_id = source.order_item_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        items_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_order_items")

    print(f"Batch {batch_id}: Processed {orders_df.count()} orders, {items_df.count()} line items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream from Bronze to Silver

# COMMAND ----------

orders_stream = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(f"{CATALOG}.{SCHEMA}.bronze_orders")
)

query = (orders_stream
    .writeStream
    .foreachBatch(process_orders_cdc)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_orders")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Orders

# COMMAND ----------

print(f"\n=== Silver Orders Summary ===")
print(f"Total orders: {spark.table('silver_orders').count()}")
print(f"Total line items: {spark.table('silver_order_items').count()}")

# Order status distribution
display(
    spark.table("silver_orders")
    .groupBy("status")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("total").alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)
