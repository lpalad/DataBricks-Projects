# Databricks notebook source
# MAGIC %md
# MAGIC # Land New Data
# MAGIC This notebook simulates/checks for new data landing in S3 from WooCommerce

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime
import json

# Configuration
CATALOG = spark.conf.get("catalog", "ecom_prod")
SCHEMA = spark.conf.get("schema", "main")
S3_LANDING_PATH = spark.conf.get("s3_landing_path", "s3://ecommerce-landing-zone")

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"S3 Landing Path: {S3_LANDING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for New Files

# COMMAND ----------

# List files in landing zone
orders_path = f"{S3_LANDING_PATH}/orders/"
customers_path = f"{S3_LANDING_PATH}/customers/"
products_path = f"{S3_LANDING_PATH}/products/"

def list_landing_files(path, entity):
    """List files in landing zone for an entity"""
    try:
        files = dbutils.fs.ls(path)
        print(f"\n{entity.upper()} - Found {len(files)} files:")
        for f in files[:10]:  # Show first 10
            print(f"  - {f.name} ({f.size} bytes)")
        return len(files)
    except Exception as e:
        print(f"\n{entity.upper()} - No files or path doesn't exist: {e}")
        return 0

orders_count = list_landing_files(orders_path, "orders")
customers_count = list_landing_files(customers_path, "customers")
products_count = list_landing_files(products_path, "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Task Values for Downstream Tasks

# COMMAND ----------

# Pass information to downstream tasks
dbutils.jobs.taskValues.set(key="orders_count", value=orders_count)
dbutils.jobs.taskValues.set(key="customers_count", value=customers_count)
dbutils.jobs.taskValues.set(key="products_count", value=products_count)
dbutils.jobs.taskValues.set(key="landing_timestamp", value=datetime.now().isoformat())

print(f"\nTask values set for downstream processing")
print(f"Total files to process: {orders_count + customers_count + products_count}")
