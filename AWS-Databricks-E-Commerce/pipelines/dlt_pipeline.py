# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Delta Live Tables Pipeline
# MAGIC Production-grade DLT pipeline - Orders Only (for initial testing)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, MapType

# Configuration
S3_LANDING_PATH = spark.conf.get("s3_landing_path", "s3://ecommerce-landing-zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_orders_dlt",
    comment="Raw orders from WooCommerce",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["_ingest_date"]
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{S3_LANDING_PATH}/orders/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned & Validated

# COMMAND ----------

@dlt.table(
    name="silver_orders_dlt",
    comment="Cleaned and validated orders"
)
def silver_orders():
    return (
        dlt.read_stream("bronze_orders_dlt")
        .select(
            F.col("data.id").cast("string").alias("order_id"),
            F.col("data.customer_id").cast("string").alias("customer_id"),
            F.col("data.date_created").cast("timestamp").alias("order_date"),
            F.col("data.status").alias("status"),
            F.col("data.total").cast("decimal(10,2)").alias("total"),
            F.col("data.currency").alias("currency"),
            F.col("data.payment_method").alias("payment_method"),
            F.col("data.billing.first_name").alias("billing_first_name"),
            F.col("data.billing.last_name").alias("billing_last_name"),
            F.col("data.billing.city").alias("billing_city"),
            F.col("data.billing.country").alias("billing_country"),
            F.col("data.billing.email").alias("billing_email"),
            F.size("data.line_items").alias("item_count"),
            F.col("_webhook_topic"),
            F.col("_received_at").cast("timestamp").alias("received_at"),
            F.col("_ingest_timestamp")
        )
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Order Line Items (Exploded)

# COMMAND ----------

@dlt.table(
    name="silver_order_items_dlt",
    comment="Exploded order line items"
)
def silver_order_items():
    return (
        dlt.read_stream("bronze_orders_dlt")
        .select(
            F.col("data.id").cast("string").alias("order_id"),
            F.explode("data.line_items").alias("item")
        )
        .select(
            F.col("order_id"),
            F.col("item.id").cast("string").alias("line_item_id"),
            F.col("item.product_id").cast("string").alias("product_id"),
            F.col("item.name").alias("product_name"),
            F.col("item.sku").alias("sku"),
            F.col("item.quantity").cast("int").alias("quantity"),
            F.col("item.price").cast("decimal(10,2)").alias("unit_price"),
            F.col("item.total").cast("decimal(10,2)").alias("line_total")
        )
        .filter(F.col("order_id").isNotNull())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Aggregations

# COMMAND ----------

@dlt.table(
    name="gold_daily_sales_dlt",
    comment="Daily sales aggregations for reporting"
)
def gold_daily_sales():
    return (
        dlt.read("silver_orders_dlt")
        .filter(~F.col("status").isin("cancelled", "refunded", "failed"))
        .groupBy(F.date_trunc("day", "order_date").alias("sale_date"))
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("total").alias("gross_revenue"),
            F.avg("total").alias("avg_order_value")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_sales_by_country_dlt",
    comment="Sales aggregated by country"
)
def gold_sales_by_country():
    return (
        dlt.read("silver_orders_dlt")
        .filter(~F.col("status").isin("cancelled", "refunded", "failed"))
        .groupBy("billing_country")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("total").alias("gross_revenue"),
            F.avg("total").alias("avg_order_value")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_product_sales_dlt",
    comment="Product sales summary"
)
def gold_product_sales():
    return (
        dlt.read("silver_order_items_dlt")
        .groupBy("product_id", "product_name", "sku")
        .agg(
            F.sum("quantity").alias("total_quantity_sold"),
            F.sum("line_total").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("unit_price").alias("avg_unit_price")
        )
    )
