# E-Commerce Data Platform

A production-grade data engineering platform connecting WooCommerce to Databricks with Power BI analytics.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────────┐
│   WooCommerce   │────▶│   AWS Lambda    │────▶│         Databricks          │
│   (Live Store)  │     │   + API Gateway │     │   Bronze → Silver → Gold    │
└─────────────────┘     └─────────────────┘     └─────────────────────────────┘
                                                              │
                               ┌──────────────────────────────┘
                               ▼
                        ┌─────────────────┐
                        │    Power BI     │
                        │   Dashboards    │
                        └─────────────────┘
```

## Quick Start

### Prerequisites

- AWS CLI configured (`aws configure`)
- Databricks CLI installed (`brew install databricks`)
- Python 3.9+
- Databricks workspace with Unity Catalog

### 1. Set Up AWS Infrastructure

```bash
# Run the setup script
./scripts/setup_aws.sh
```

This creates:
- S3 landing zone bucket
- Lambda webhook handler
- API Gateway endpoint

### 2. Configure Databricks CLI

```bash
# Configure with your workspace
databricks configure

# Enter:
# - Databricks Host: https://your-workspace.cloud.databricks.com
# - Token: your-access-token
```

### 3. Deploy to Databricks

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

### 4. Configure WooCommerce Webhooks

In WordPress Admin → WooCommerce → Settings → Advanced → Webhooks:

1. Add webhook for `order.created` → `https://your-api-endpoint/webhook`
2. Add webhook for `customer.created` → `https://your-api-endpoint/webhook`
3. Add webhook for `product.updated` → `https://your-api-endpoint/webhook`

### 5. Generate Sample Data (Testing)

```bash
# Install dependencies
pip install faker boto3

# Generate and upload to S3
python scripts/generate_sample_data.py --upload --orders 200

# Or generate locally first
python scripts/generate_sample_data.py --local ./sample_data
```

## Project Structure

```
ecommerce-data-platform/
├── databricks.yml              # Databricks Asset Bundle config
├── src/
│   ├── bronze/                 # Raw data ingestion
│   │   ├── 01_land_data.py
│   │   └── 02_bronze_ingestion.py
│   ├── silver/                 # Transformations
│   │   ├── 01_silver_customers.py
│   │   ├── 02_silver_orders.py
│   │   └── 03_silver_products.py
│   └── gold/                   # Analytics tables
│       ├── 01_gold_sales_summary.py
│       └── 02_gold_customer_analytics.py
├── pipelines/
│   └── dlt_pipeline.py         # Delta Live Tables pipeline
├── infrastructure/
│   ├── lambda/                 # AWS Lambda code
│   └── terraform/              # IaC (optional)
├── scripts/
│   ├── setup_aws.sh            # AWS setup script
│   └── generate_sample_data.py # Test data generator
└── .github/
    └── workflows/
        └── deploy.yml          # CI/CD pipeline
```

## Data Model

### Bronze Layer (Raw)
- `bronze_orders` - Raw order events
- `bronze_customers` - Raw customer data
- `bronze_products` - Raw product catalog

### Silver Layer (Cleaned)
- `silver_orders` - Deduplicated orders
- `silver_order_items` - Exploded line items
- `silver_customers` - Cleaned customer profiles (CDC)
- `silver_products` - Product catalog (SCD Type 2)

### Gold Layer (Analytics)
- `gold_daily_sales` - Daily sales metrics
- `gold_sales_by_country` - Geographic breakdown
- `gold_product_performance` - Product analytics
- `gold_customer_ltv` - Customer lifetime value
- `gold_customer_rfm` - RFM segmentation

## Environment Variables

Create a `.env` file (copy from `.env.example`):

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi_xxxxx
AWS_ACCESS_KEY_ID=AKIA_xxxxx
AWS_SECRET_ACCESS_KEY=xxxxx
AWS_DEFAULT_REGION=ap-southeast-2
S3_LANDING_BUCKET=ecommerce-landing-zone
```

## CI/CD

Push to `main` branch triggers automatic deployment:

1. Validates Databricks bundle
2. Deploys to production
3. Optionally triggers DLT pipeline refresh

## Power BI Connection

1. Install Databricks ODBC/Spark connector
2. Connect to: `https://your-workspace.cloud.databricks.com`
3. Use SQL Warehouse for queries
4. Build reports on Gold layer tables

## Cost Optimization

- Clusters auto-terminate after 10 minutes
- Use Jobs compute ($0.15/DBU) instead of All-Purpose ($0.55/DBU)
- DLT runs on-demand, not continuously
- S3 lifecycle moves old data to Glacier

## License

MIT
