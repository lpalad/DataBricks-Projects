# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi - Gold Layer Aggregation
# MAGIC
# MAGIC **Purpose:** Create aggregated analytics tables for business reporting.
# MAGIC
# MAGIC **Input:** `nyctaxi.02_silver.yellow_trips_joined`
# MAGIC
# MAGIC **Output:** `nyctaxi.03_gold.daily_trip_summary`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Layer Data

# COMMAND ----------

df = spark.read.table("nyctaxi.`02_silver`.yellow_trips_joined")

print(f"Silver records: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Daily Trip Summary
# MAGIC
# MAGIC Aggregate trips by pickup date to calculate:
# MAGIC - Total trips per day
# MAGIC - Average passengers per trip
# MAGIC - Average distance per trip
# MAGIC - Average fare per trip
# MAGIC - Max/min fares
# MAGIC - Total revenue per day

# COMMAND ----------

df_daily = df.groupBy(
    df.tpep_pickup_datetime.cast("date").alias("pickup_date")
).agg(
    count("*").alias("total_trips"),
    round(avg("passenger_count"), 1).alias("average_passengers"),
    round(avg("trip_distance"), 1).alias("average_distance"),
    round(avg("fare_amount"), 2).alias("average_fare_per_trip"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# COMMAND ----------

# Order by date
df_daily = df_daily.orderBy("pickup_date")

# COMMAND ----------

# Preview daily summary
df_daily.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Gold Layer

# COMMAND ----------

df_daily.write.mode("overwrite").saveAsTable("nyctaxi.`03_gold`.daily_trip_summary")

print("Daily trip summary saved to Gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Table

# COMMAND ----------

# Row count
gold_count = spark.table("nyctaxi.`03_gold`.daily_trip_summary").count()
print(f"Daily summaries: {gold_count} days")

# COMMAND ----------

# Preview gold data
spark.table("nyctaxi.`03_gold`.daily_trip_summary").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Details

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL nyctaxi.`03_gold`.daily_trip_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Analytics Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 Revenue Days

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_date,
# MAGIC     total_trips,
# MAGIC     total_revenue
# MAGIC FROM nyctaxi.`03_gold`.daily_trip_summary
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Busiest Days (Most Trips)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_date,
# MAGIC     total_trips,
# MAGIC     average_fare_per_trip
# MAGIC FROM nyctaxi.`03_gold`.daily_trip_summary
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(total_trips) as total_trips_all_time,
# MAGIC     ROUND(AVG(average_fare_per_trip), 2) as avg_fare,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue_all_time
# MAGIC FROM nyctaxi.`03_gold`.daily_trip_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete!
# MAGIC
# MAGIC ```
# MAGIC LANDING → BRONZE → SILVER → GOLD
# MAGIC    ✓        ✓        ✓       ✓
# MAGIC ```
# MAGIC
# MAGIC Your medallion architecture pipeline is now complete. The Gold layer table is ready for:
# MAGIC - Business dashboards
# MAGIC - Reporting
# MAGIC - Time series analysis
