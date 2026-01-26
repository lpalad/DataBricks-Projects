# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi - Silver Layer Enrichment
# MAGIC
# MAGIC **Purpose:** Enrich trip data by joining with taxi zone lookup to add:
# MAGIC - Pickup borough and zone names
# MAGIC - Dropoff borough and zone names
# MAGIC
# MAGIC **Input:**
# MAGIC - `nyctaxi.02_silver.yellow_trips_cleansed`
# MAGIC - `nyctaxi.02_silver.taxi_zone_lookup`
# MAGIC
# MAGIC **Output:** `nyctaxi.02_silver.yellow_trips_joined`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Tables

# COMMAND ----------

# Read cleansed trips
df_trips = spark.read.table("nyctaxi.`02_silver`.yellow_trips_cleansed")

# Read zone lookup
df_zones = spark.read.table("nyctaxi.`02_silver`.taxi_zone_lookup")

print(f"Trips: {df_trips.count():,} rows")
print(f"Zones: {df_zones.count():,} rows")

# COMMAND ----------

# Preview zone lookup
df_zones.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join 1: Enrich Pickup Location
# MAGIC
# MAGIC Join trips with zones on `pu_location_id` to get pickup borough and zone names.

# COMMAND ----------

df_join_1 = df_trips.join(
    df_zones,
    df_trips.pu_location_id == df_zones.location_id,
    "left"
).select(
    df_trips.vendor,
    df_trips.tpep_pickup_datetime,
    df_trips.tpep_dropoff_datetime,
    df_trips.trip_duration,
    df_trips.passenger_count,
    df_trips.trip_distance,
    df_trips.rate_type,
    df_zones.borough.alias("pu_borough"),      # Pickup borough
    df_zones.zone.alias("pu_zone"),            # Pickup zone
    df_trips.do_location_id,                   # Keep for second join
    df_trips.payment_type,
    df_trips.fare_amount,
    df_trips.extra,
    df_trips.mta_tax,
    df_trips.tolls_amount,
    df_trips.improvement_surcharge,
    df_trips.total_amount,
    df_trips.congestion_surcharge,
    df_trips.airport_fee,
    df_trips.cbd_congestion_fee,
    df_trips.processed_timestamp
)

# COMMAND ----------

# Preview after first join
df_join_1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join 2: Enrich Dropoff Location
# MAGIC
# MAGIC Join again with zones on `do_location_id` to get dropoff borough and zone names.

# COMMAND ----------

df_join_final = df_join_1.join(
    df_zones,
    df_join_1.do_location_id == df_zones.location_id,
    "left"
).select(
    df_join_1.vendor,
    df_join_1.tpep_pickup_datetime,
    df_join_1.tpep_dropoff_datetime,
    df_join_1.trip_duration,
    df_join_1.passenger_count,
    df_join_1.trip_distance,
    df_join_1.rate_type,
    df_join_1.pu_borough,
    df_join_1.pu_zone,
    df_zones.borough.alias("do_borough"),      # Dropoff borough
    df_zones.zone.alias("do_zone"),            # Dropoff zone
    df_join_1.payment_type,
    df_join_1.fare_amount,
    df_join_1.extra,
    df_join_1.mta_tax,
    df_join_1.tolls_amount,
    df_join_1.improvement_surcharge,
    df_join_1.total_amount,
    df_join_1.congestion_surcharge,
    df_join_1.airport_fee,
    df_join_1.cbd_congestion_fee,
    df_join_1.processed_timestamp
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Enriched Data
# MAGIC
# MAGIC Now we have human-readable pickup and dropoff locations!

# COMMAND ----------

df_join_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Silver Layer

# COMMAND ----------

df_join_final.write.mode("overwrite").saveAsTable("nyctaxi.`02_silver`.yellow_trips_joined")

print("Enriched trips saved to Silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Enrichment

# COMMAND ----------

# Check a sample of enriched data
spark.sql("""
    SELECT
        vendor,
        pu_borough,
        pu_zone,
        do_borough,
        do_zone,
        trip_distance,
        fare_amount,
        payment_type
    FROM nyctaxi.`02_silver`.yellow_trips_joined
    LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Analytics Check
# MAGIC
# MAGIC Verify the join worked by checking trip counts by borough.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pu_borough,
# MAGIC     COUNT(*) as trip_count
# MAGIC FROM nyctaxi.`02_silver`.yellow_trips_joined
# MAGIC WHERE pu_borough IS NOT NULL
# MAGIC GROUP BY pu_borough
# MAGIC ORDER BY trip_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Run `04_gold_aggregation.py` to create aggregated analytics tables.
