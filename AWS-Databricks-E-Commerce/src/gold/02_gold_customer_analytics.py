# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Customer Analytics
# MAGIC Customer behavior and segmentation analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog", "ecom_prod")
SCHEMA = spark.conf.get("schema", "main")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Lifetime Value

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_ltv AS
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.billing_country,
    c.billing_city,
    c.date_created as customer_since,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total) as lifetime_value,
    AVG(o.total) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    DATEDIFF(MAX(o.order_date), MIN(o.order_date)) as customer_tenure_days,
    DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) as days_since_last_order,
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) <= 30 THEN 'Active'
        WHEN DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) <= 90 THEN 'At Risk'
        WHEN DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) <= 180 THEN 'Dormant'
        ELSE 'Churned'
    END as customer_status
FROM silver_customers c
LEFT JOIN silver_orders o ON c.customer_id = o.customer_id
    AND o.status NOT IN ('cancelled', 'refunded', 'failed')
GROUP BY
    c.customer_id,
    c.full_name,
    c.email,
    c.billing_country,
    c.billing_city,
    c.date_created
ORDER BY lifetime_value DESC
""")

print("✓ gold_customer_ltv created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Segmentation (RFM Analysis)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_rfm AS
WITH rfm_base AS (
    SELECT
        customer_id,
        DATEDIFF(CURRENT_DATE(), MAX(order_date)) as recency,
        COUNT(DISTINCT order_id) as frequency,
        SUM(total) as monetary
    FROM silver_orders
    WHERE status NOT IN ('cancelled', 'refunded', 'failed')
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        recency,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency) as f_score,
        NTILE(5) OVER (ORDER BY monetary) as m_score
    FROM rfm_base
)
SELECT
    r.customer_id,
    c.full_name,
    c.email,
    c.billing_country,
    r.recency as days_since_last_purchase,
    r.frequency as total_orders,
    r.monetary as total_spend,
    r.r_score,
    r.f_score,
    r.m_score,
    CONCAT(r.r_score, r.f_score, r.m_score) as rfm_score,
    CASE
        WHEN r.r_score >= 4 AND r.f_score >= 4 AND r.m_score >= 4 THEN 'Champions'
        WHEN r.r_score >= 4 AND r.f_score >= 3 THEN 'Loyal Customers'
        WHEN r.r_score >= 4 AND r.f_score <= 2 THEN 'New Customers'
        WHEN r.r_score >= 3 AND r.f_score >= 3 THEN 'Potential Loyalists'
        WHEN r.r_score <= 2 AND r.f_score >= 4 THEN 'At Risk'
        WHEN r.r_score <= 2 AND r.f_score <= 2 AND r.m_score >= 4 THEN 'Cant Lose Them'
        WHEN r.r_score <= 2 AND r.f_score <= 2 THEN 'Hibernating'
        ELSE 'Need Attention'
    END as customer_segment
FROM rfm_scores r
JOIN silver_customers c ON r.customer_id = c.customer_id
ORDER BY r.monetary DESC
""")

print("✓ gold_customer_rfm created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Cohort Analysis

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_cohorts AS
WITH first_purchase AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) as cohort_month
    FROM silver_orders
    WHERE status NOT IN ('cancelled', 'refunded', 'failed')
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT
        o.customer_id,
        f.cohort_month,
        DATE_TRUNC('month', o.order_date) as activity_month,
        MONTHS_BETWEEN(DATE_TRUNC('month', o.order_date), f.cohort_month) as months_since_first
    FROM silver_orders o
    JOIN first_purchase f ON o.customer_id = f.customer_id
    WHERE o.status NOT IN ('cancelled', 'refunded', 'failed')
)
SELECT
    cohort_month,
    CAST(months_since_first AS INT) as months_since_first,
    COUNT(DISTINCT customer_id) as active_customers,
    SUM(COUNT(DISTINCT customer_id)) OVER (PARTITION BY cohort_month ORDER BY months_since_first ROWS UNBOUNDED PRECEDING) as cumulative_customers
FROM monthly_activity
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first
""")

print("✓ gold_customer_cohorts created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Geographic Distribution

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_geography AS
SELECT
    billing_country as country,
    billing_state as state,
    billing_city as city,
    COUNT(DISTINCT customer_id) as customer_count,
    COUNT(DISTINCT CASE WHEN _is_current THEN customer_id END) as active_customers
FROM silver_customers
GROUP BY billing_country, billing_state, billing_city
ORDER BY customer_count DESC
""")

print("✓ gold_customer_geography created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("GOLD LAYER - CUSTOMER ANALYTICS TABLES")
print("="*60)

tables = [
    "gold_customer_ltv",
    "gold_customer_rfm",
    "gold_customer_cohorts",
    "gold_customer_geography"
]

for table in tables:
    count = spark.table(table).count()
    print(f"  {table}: {count} records")

# COMMAND ----------

# Customer segment distribution
display(
    spark.table("gold_customer_rfm")
    .groupBy("customer_segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.sum("total_spend").alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)
