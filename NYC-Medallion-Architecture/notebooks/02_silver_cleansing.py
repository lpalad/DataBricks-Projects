# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi - Silver Layer Cleansing
# MAGIC
# MAGIC **Purpose:** Transform Bronze data by:
# MAGIC - Filtering to valid date range
# MAGIC - Mapping coded values to human-readable names
# MAGIC - Calculating derived fields (trip_duration)
# MAGIC - Renaming columns to snake_case
# MAGIC
# MAGIC **Input:** `nyctaxi.01_bronze.yellow_trips_raw`
# MAGIC
# MAGIC **Output:**
# MAGIC - `nyctaxi.02_silver.yellow_trips_cleansed`
# MAGIC - `nyctaxi.02_silver.taxi_zone_lookup`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql.functions import col, when, timestamp_diff, current_timestamp, lit
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Load Taxi Zone Lookup Table
# MAGIC
# MAGIC This dimension table maps location IDs to borough/zone names.

# COMMAND ----------

# Read the lookup CSV file
df_zones = spark.read.format("csv").option("header", True).load(
    "/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv"
)

# COMMAND ----------

# Preview raw lookup data
df_zones.display()

# COMMAND ----------

# Transform: rename columns, add effective dates
df_zones = df_zones.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

# Save to Silver layer
df_zones.write.mode("overwrite").saveAsTable("nyctaxi.`02_silver`.taxi_zone_lookup")

print("Taxi zone lookup table saved to Silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Cleanse Yellow Trips Data

# COMMAND ----------

# Read from Bronze layer
df = spark.read.table("nyctaxi.`01_bronze`.yellow_trips_raw")

print(f"Bronze records: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter by Date Range
# MAGIC
# MAGIC Keep only trips from first half of 2025 (January - June).

# COMMAND ----------

df = df.filter("tpep_pickup_datetime >= '2025-01-01' AND tpep_pickup_datetime < '2025-07-01'")

print(f"Records after date filter: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Transformations
# MAGIC
# MAGIC - Map VendorID to vendor names
# MAGIC - Map RatecodeID to rate type descriptions
# MAGIC - Map payment_type to payment method names
# MAGIC - Calculate trip_duration in minutes
# MAGIC - Rename location columns to snake_case

# COMMAND ----------

df = df.select(
    # Vendor mapping
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 6, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
        .alias("vendor"),

    # Datetime fields
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    # Calculate trip duration in minutes
    timestamp_diff('MINUTE', df.tpep_pickup_datetime, df.tpep_dropoff_datetime).alias("trip_duration"),

    # Passenger and distance
    "passenger_count",
    "trip_distance",

    # Rate type mapping
    when(col("RatecodeID") == 1, "Standard Rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated Fare")
        .when(col("RatecodeID") == 6, "Group Ride")
        .otherwise("Unknown")
        .alias("rate_type"),

    # Store and forward flag
    "store_and_fwd_flag",

    # Rename location IDs to snake_case
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    # Payment type mapping
    when(col("payment_type") == 0, "Flex Fare trip")
        .when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Unknown")
        .alias("payment_type"),

    # Fare components
    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",

    # Audit timestamp
    "processed_timestamp"
)

# COMMAND ----------

# Preview transformed data
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to Silver Layer

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.`02_silver`.yellow_trips_cleansed")

print("Cleansed trips saved to Silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables

# COMMAND ----------

# Check row counts
print("Silver Layer Tables:")
print("-" * 40)

cleansed_count = spark.table("nyctaxi.`02_silver`.yellow_trips_cleansed").count()
lookup_count = spark.table("nyctaxi.`02_silver`.taxi_zone_lookup").count()

print(f"yellow_trips_cleansed: {cleansed_count:,} rows")
print(f"taxi_zone_lookup: {lookup_count:,} rows")

# COMMAND ----------

# Preview cleansed data
spark.table("nyctaxi.`02_silver`.yellow_trips_cleansed").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Run `03_silver_enrichment.py` to join trips with zone lookup for location names.
