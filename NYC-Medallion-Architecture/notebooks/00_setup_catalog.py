# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi - Setup Unity Catalog
# MAGIC
# MAGIC This notebook creates the catalog, schemas, and volumes for the Medallion Architecture.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - `nyctaxi` (Catalog)
# MAGIC   - `00_landing` (Schema) - Raw files in Volume
# MAGIC   - `01_bronze` (Schema) - Raw Delta tables
# MAGIC   - `02_silver` (Schema) - Cleansed & enriched tables
# MAGIC   - `03_gold` (Schema) - Aggregated analytics tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS nyctaxi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas (Medallion Layers)

# COMMAND ----------

# Landing zone for raw files
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.`00_landing`")

# Bronze layer - raw data as Delta tables
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.`01_bronze`")

# Silver layer - cleansed and enriched data
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.`02_silver`")

# Gold layer - aggregated analytics
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.`03_gold`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for Landing Zone

# COMMAND ----------

spark.sql("""
    CREATE VOLUME IF NOT EXISTS nyctaxi.`00_landing`.data_sources
    COMMENT 'Landing zone for raw data files (Parquet, CSV)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Subfolders in Volume
# MAGIC
# MAGIC Structure:
# MAGIC ```
# MAGIC /Volumes/nyctaxi/00_landing/data_sources/
# MAGIC ├── nyctaxi_yellow/
# MAGIC │   └── (parquet files)
# MAGIC └── lookup/
# MAGIC     └── taxi_zone_lookup.csv
# MAGIC ```

# COMMAND ----------

import os

# Create folder structure
base_path = "/Volumes/nyctaxi/00_landing/data_sources"
os.makedirs(f"{base_path}/nyctaxi_yellow", exist_ok=True)
os.makedirs(f"{base_path}/lookup", exist_ok=True)

print("Folder structure created:")
print(f"  {base_path}/nyctaxi_yellow/")
print(f"  {base_path}/lookup/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# List all schemas in the catalog
spark.sql("SHOW SCHEMAS IN nyctaxi").display()

# COMMAND ----------

# List volumes
spark.sql("SHOW VOLUMES IN nyctaxi.`00_landing`").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. Upload raw parquet files to `/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/`
# MAGIC 2. Upload `taxi_zone_lookup.csv` to `/Volumes/nyctaxi/00_landing/data_sources/lookup/`
# MAGIC 3. Run `01_bronze_ingestion.py`
