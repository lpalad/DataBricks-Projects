# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Customers
# MAGIC Clean, deduplicate, and apply CDC for customer data

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
# MAGIC ## Create Silver Customers Table

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id STRING,
    email STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    username STRING,
    billing_address STRING,
    billing_city STRING,
    billing_state STRING,
    billing_postcode STRING,
    billing_country STRING,
    billing_phone STRING,
    shipping_address STRING,
    shipping_city STRING,
    shipping_state STRING,
    shipping_postcode STRING,
    shipping_country STRING,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    _updated_at TIMESTAMP,
    _is_current BOOLEAN
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Processing with Deduplication

# COMMAND ----------

def process_customers_cdc(batch_df, batch_id):
    """Process customer CDC - handle inserts, updates, deletes"""

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No records to process")
        return

    # Deduplicate within batch - keep latest by date_modified
    window = Window.partitionBy("customer_id").orderBy(F.col("date_modified").desc())

    deduped_df = (batch_df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Transform to silver schema
    silver_df = (deduped_df
        .select(
            F.col("customer_id"),
            F.col("email"),
            F.col("first_name"),
            F.col("last_name"),
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
            F.col("username"),
            F.col("billing.address_1").alias("billing_address"),
            F.col("billing.city").alias("billing_city"),
            F.col("billing.state").alias("billing_state"),
            F.col("billing.postcode").alias("billing_postcode"),
            F.col("billing.country").alias("billing_country"),
            F.col("billing.phone").alias("billing_phone"),
            F.col("shipping.address_1").alias("shipping_address"),
            F.col("shipping.city").alias("shipping_city"),
            F.col("shipping.state").alias("shipping_state"),
            F.col("shipping.postcode").alias("shipping_postcode"),
            F.col("shipping.country").alias("shipping_country"),
            F.col("date_created"),
            F.col("date_modified"),
            F.current_timestamp().alias("_updated_at"),
            F.lit(True).alias("_is_current")
        )
    )

    # MERGE into silver table
    target_table = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.silver_customers")

    (target_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.customer_id = source.customer_id"
        )
        .whenMatchedUpdate(
            condition="source.date_modified > target.date_modified",
            set={
                "email": "source.email",
                "first_name": "source.first_name",
                "last_name": "source.last_name",
                "full_name": "source.full_name",
                "username": "source.username",
                "billing_address": "source.billing_address",
                "billing_city": "source.billing_city",
                "billing_state": "source.billing_state",
                "billing_postcode": "source.billing_postcode",
                "billing_country": "source.billing_country",
                "billing_phone": "source.billing_phone",
                "shipping_address": "source.shipping_address",
                "shipping_city": "source.shipping_city",
                "shipping_state": "source.shipping_state",
                "shipping_postcode": "source.shipping_postcode",
                "shipping_country": "source.shipping_country",
                "date_modified": "source.date_modified",
                "_updated_at": "source._updated_at"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Batch {batch_id}: Processed {silver_df.count()} customer records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream from Bronze to Silver

# COMMAND ----------

# Read from bronze with Change Data Feed
customers_stream = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(f"{CATALOG}.{SCHEMA}.bronze_customers")
)

# Process with foreachBatch for CDC logic
query = (customers_stream
    .writeStream
    .foreachBatch(process_customers_cdc)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_customers")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Customers

# COMMAND ----------

print(f"\n=== Silver Customers Summary ===")
print(f"Total records: {spark.table('silver_customers').count()}")
print(f"Unique customers: {spark.table('silver_customers').select('customer_id').distinct().count()}")

# Country distribution
display(
    spark.table("silver_customers")
    .groupBy("billing_country")
    .count()
    .orderBy(F.col("count").desc())
)
