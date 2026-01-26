# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Sales Summary
# MAGIC Aggregated sales analytics for business reporting

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
# MAGIC ## Create Gold Tables

# COMMAND ----------

# Daily Sales Summary
spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT
    DATE(order_date) as sale_date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total) as gross_revenue,
    AVG(total) as avg_order_value,
    SUM(item_count) as total_items_sold
FROM silver_orders
WHERE status NOT IN ('cancelled', 'refunded', 'failed')
GROUP BY DATE(order_date)
ORDER BY sale_date DESC
""")

print("✓ gold_daily_sales created")

# COMMAND ----------

# Sales by Country
spark.sql("""
CREATE OR REPLACE TABLE gold_sales_by_country AS
SELECT
    billing_country as country,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total) as gross_revenue,
    AVG(total) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM silver_orders
WHERE status NOT IN ('cancelled', 'refunded', 'failed')
GROUP BY billing_country
ORDER BY gross_revenue DESC
""")

print("✓ gold_sales_by_country created")

# COMMAND ----------

# Product Performance
spark.sql("""
CREATE OR REPLACE TABLE gold_product_performance AS
SELECT
    oi.product_id,
    oi.product_name,
    p.category_name,
    p.price as current_price,
    COUNT(DISTINCT oi.order_id) as order_count,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.line_total) as total_revenue,
    AVG(oi.unit_price) as avg_selling_price
FROM silver_order_items oi
LEFT JOIN current_products p ON oi.product_id = p.product_id
GROUP BY oi.product_id, oi.product_name, p.category_name, p.price
ORDER BY total_revenue DESC
""")

print("✓ gold_product_performance created")

# COMMAND ----------

# Sales by Category
spark.sql("""
CREATE OR REPLACE TABLE gold_sales_by_category AS
SELECT
    COALESCE(p.category_name, 'Unknown') as category,
    COUNT(DISTINCT oi.order_id) as order_count,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.line_total) as total_revenue,
    COUNT(DISTINCT oi.product_id) as unique_products
FROM silver_order_items oi
LEFT JOIN current_products p ON oi.product_id = p.product_id
GROUP BY p.category_name
ORDER BY total_revenue DESC
""")

print("✓ gold_sales_by_category created")

# COMMAND ----------

# Monthly Trends
spark.sql("""
CREATE OR REPLACE TABLE gold_monthly_trends AS
SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total) as gross_revenue,
    AVG(total) as avg_order_value,
    SUM(total) - LAG(SUM(total)) OVER (ORDER BY DATE_TRUNC('month', order_date)) as revenue_change,
    ROUND(
        (SUM(total) - LAG(SUM(total)) OVER (ORDER BY DATE_TRUNC('month', order_date)))
        / LAG(SUM(total)) OVER (ORDER BY DATE_TRUNC('month', order_date)) * 100, 2
    ) as revenue_change_pct
FROM silver_orders
WHERE status NOT IN ('cancelled', 'refunded', 'failed')
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month DESC
""")

print("✓ gold_monthly_trends created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("GOLD LAYER - SALES SUMMARY TABLES")
print("="*60)

tables = [
    "gold_daily_sales",
    "gold_sales_by_country",
    "gold_product_performance",
    "gold_sales_by_category",
    "gold_monthly_trends"
]

for table in tables:
    count = spark.table(table).count()
    print(f"  {table}: {count} records")

# COMMAND ----------

# Preview daily sales
display(spark.table("gold_daily_sales").limit(10))
