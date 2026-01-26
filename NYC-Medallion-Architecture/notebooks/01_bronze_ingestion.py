# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi - Bronze Layer Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest raw Parquet files from landing zone into Bronze Delta table.
# MAGIC
# MAGIC **Input:** `/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*.parquet`
# MAGIC
# MAGIC **Output:** `nyctaxi.01_bronze.yellow_trips_raw`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Parquet Files
# MAGIC
# MAGIC The `*` wildcard reads all parquet files in the folder.

# COMMAND ----------

# Read all parquet files from landing zone
df = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")

# COMMAND ----------

# Check row count and schema
print(f"Total records: {df.count():,}")
df.printSchema()

# COMMAND ----------

# Preview data
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Processing Metadata
# MAGIC
# MAGIC Add `processed_timestamp` column to track when the data was ingested.

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Bronze Layer
# MAGIC
# MAGIC Save as a managed Delta table in Unity Catalog.

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.`01_bronze`.yellow_trips_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Table

# COMMAND ----------

# Check the table exists and row count
spark.sql("SELECT COUNT(*) as row_count FROM nyctaxi.`01_bronze`.yellow_trips_raw").display()

# COMMAND ----------

# Preview the Bronze table
spark.table("nyctaxi.`01_bronze`.yellow_trips_raw").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Table Details

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL nyctaxi.`01_bronze`.yellow_trips_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Run `02_silver_cleansing.py` to transform Bronze data into cleansed Silver tables.
