# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Products (SCD Type 2)
# MAGIC Maintain product history with Slowly Changing Dimension Type 2

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
# MAGIC ## Create Silver Products Table (SCD Type 2)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_products (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY,
    product_id STRING,
    name STRING,
    slug STRING,
    sku STRING,
    price DECIMAL(10,2),
    regular_price DECIMAL(10,2),
    sale_price DECIMAL(10,2),
    stock_quantity INT,
    stock_status STRING,
    category_id STRING,
    category_name STRING,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    _updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

# Current products view
spark.sql("""
CREATE OR REPLACE VIEW current_products AS
SELECT * FROM silver_products WHERE is_current = TRUE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Processing

# COMMAND ----------

def process_products_scd2(batch_df, batch_id):
    """Process products with SCD Type 2 logic"""

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No records to process")
        return

    # Deduplicate within batch
    window = Window.partitionBy("product_id").orderBy(F.col("date_modified").desc())
    deduped_df = (batch_df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Explode categories (take first category if multiple)
    products_df = (deduped_df
        .select(
            F.col("product_id"),
            F.col("name"),
            F.col("slug"),
            F.col("sku"),
            F.col("price"),
            F.col("regular_price"),
            F.col("sale_price"),
            F.col("stock_quantity"),
            F.col("stock_status"),
            F.col("categories").getItem(0).getField("id").alias("category_id"),
            F.col("categories").getItem(0).getField("name").alias("category_name"),
            F.col("date_modified").alias("effective_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(True).alias("is_current"),
            F.current_timestamp().alias("_updated_at")
        )
    )

    # Check if table has data
    target_table = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.silver_products")
    current_df = spark.table(f"{CATALOG}.{SCHEMA}.silver_products").filter("is_current = TRUE")

    if current_df.count() == 0:
        # First load - just insert
        products_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_products")
        print(f"Batch {batch_id}: Initial load of {products_df.count()} products")
        return

    # Find products with price changes
    changes_df = (products_df.alias("new")
        .join(
            current_df.alias("current"),
            "product_id",
            "inner"
        )
        .filter(
            (F.col("new.price") != F.col("current.price")) |
            (F.col("new.stock_quantity") != F.col("current.stock_quantity"))
        )
        .select("new.*")
    )

    # Find new products
    new_products_df = (products_df.alias("new")
        .join(
            current_df.select("product_id").alias("current"),
            "product_id",
            "left_anti"
        )
    )

    # Records to expire (close out old records)
    products_to_expire = changes_df.select("product_id").distinct()

    if products_to_expire.count() > 0:
        # Expire old records
        (target_table.alias("target")
            .merge(
                products_to_expire.alias("expire"),
                "target.product_id = expire.product_id AND target.is_current = TRUE"
            )
            .whenMatchedUpdate(set={
                "end_date": F.current_timestamp(),
                "is_current": F.lit(False),
                "_updated_at": F.current_timestamp()
            })
            .execute()
        )

        # Insert new versions
        changes_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_products")

    # Insert completely new products
    if new_products_df.count() > 0:
        new_products_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_products")

    print(f"Batch {batch_id}: {changes_df.count()} updated, {new_products_df.count()} new products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream from Bronze to Silver

# COMMAND ----------

products_stream = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(f"{CATALOG}.{SCHEMA}.bronze_products")
)

query = (products_stream
    .writeStream
    .foreachBatch(process_products_scd2)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_products")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Products

# COMMAND ----------

print(f"\n=== Silver Products Summary ===")
print(f"Total records (all versions): {spark.table('silver_products').count()}")
print(f"Current products: {spark.table('current_products').count()}")
print(f"Historical records: {spark.table('silver_products').filter('is_current = FALSE').count()}")

# Price history example
display(
    spark.table("silver_products")
    .orderBy("product_id", "effective_date")
    .limit(20)
)
