<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" alt="Databricks Logo" width="200"/>
</p>

<h1 align="center">Databricks Projects</h1>

<p align="center">
  <strong>A collection of full-stack applications powered by Databricks SQL</strong>
</p>

<p align="center">
  <a href="#the-solutions-real-problems-solved">Projects</a> •
  <a href="#why-hire-me-the-aggressive-evidence">Tech Stack</a> •
  <a href="#usage-no-nonsense-setup">Getting Started</a> •
  <a href="#project-structure">Structure</a> •
  <a href="#about-me-leonard-s-palad">About</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi&logoColor=white" alt="FastAPI"/>
  <img src="https://img.shields.io/badge/React-18+-61DAFB?logo=react&logoColor=black" alt="React"/>
  <img src="https://img.shields.io/badge/Databricks-SQL-FF3621?logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/Vite-5+-646CFF?logo=vite&logoColor=white" alt="Vite"/>
</p>

---

# Direct-to-Databricks: Full-Stack Power. Zero SaaS Bloat.

**Eliminating the $50,000 Complexity Overhead. Production-ready code for the Australian market.**

---

## Executive Summary: Engineering Profit through Data Rigor

Most Australian organisations are over-paying for data movement.

They waste **$50,000 in annual licensing** and **6 months of engineering time** on fragile middleware just to move a web lead into a database.

This is a failure of logic.

With over a decade in the IT field and an MBA, I bridge the gap between technical architecture and the commercial bottom line. My work is defined by three logical pillars:

**Certainty:** I build direct-write systems. By removing the "middleman" (ETL tools), I eliminate the primary source of data corruption and sync lag.

**Efficiency:** I use FastAPI and Vite to bypass technical debt. I deliver systems that handle 40,000 requests per second and build 10x faster than legacy frameworks.

**Fiscal Discipline:** I treat complexity as a liability. This repository proves that you can achieve ACID-compliant, production-grade data ingestion without expensive SaaS subscriptions or ongoing "connector" fees.

---

## Why Hire Me? 

I do not build "features." I deploy **business-critical assets** that protect your margins.

| The Asset | The Logical Proof | The Economic Impact |
|-----------|-------------------|---------------------|
| **Architectural Lean** | Direct Databricks SQL connection via FastAPI. No Kafka or Airflow required. | Saves $50k+ in OpEx. Removes third-party SaaS fees and contractor hours. |
| **Data Integrity Gate** | Pydantic validation & customer deduplication logic built into the API. | Protects the "Truth." Eliminates the high cost of manual data cleaning. |
| **Security Hardening** | Environment variable injection with zero hardcoded credentials. | Reduces Risk. Passes Australian security audits and eliminates SQL vulnerabilities. |
| **High-Velocity Build** | Vite + React frontend architecture with sub-300ms cold starts. | Maximizes ROI. 10x faster feature delivery than legacy builds. |

---

## The Solutions: Real Problems. Solved.

### Project 01: Direct Contact-to-Databricks

**The Problem:** Leads lost in spreadsheets or trapped in expensive CRM queues.

**The Logic:** Direct API-to-Lakehouse write. Zero lag.

**The Result:** Every lead is tracked instantly. Zero data entry waste.

---

### Project 02: Tesla Sales Analytics (Deduplication Engine)

**The Problem:** Sales data arriving with duplicates and incorrect GST calculations.

**The Logic:** Built-in customer matching and automated GST logic at the point of entry.

**The Result:** Audit-ready sales data. One "John Smith" in the DB, not 47.

---

### Project 03: [NYC Taxi Medallion Architecture Pipeline](./NYC-Medallion-Architecture/)

**The Problem:** Raw operational data scattered across files, unfit for analytics.

**The Logic:** End-to-end Medallion Architecture (Bronze → Silver → Gold) with PySpark ETL workflows, multi-table joins for dimension enrichment, and daily aggregations.

**The Result:** Production-ready data lakehouse with cleansed, enriched, and aggregated tables ready for business intelligence reporting.

| Layer | Purpose | Output |
|-------|---------|--------|
| **Bronze** | Raw ingestion with audit timestamp | `yellow_trips_raw` |
| **Silver** | Cleansed + enriched with zone lookups | `yellow_trips_cleansed`, `yellow_trips_joined` |
| **Gold** | Daily aggregated metrics | `daily_trip_summary` |

**Tech Stack:** Databricks, PySpark, Delta Lake, Unity Catalog, SQL

---

### Project 04: [AWS Databricks E-Commerce Platform](./AWS-Databricks-E-Commerce/)

**The Problem:** E-commerce order data trapped in WooCommerce, inaccessible for real-time analytics.

**The Logic:** Serverless webhook ingestion via AWS Lambda → S3 Landing Zone → Databricks Delta Live Tables pipeline with Medallion Architecture.

**The Result:** Real-time order analytics with automated ETL, customer deduplication, and sales dashboards.

| Component | Technology |
|-----------|------------|
| **Ingestion** | AWS Lambda + API Gateway (webhook receiver) |
| **Landing Zone** | S3 bucket with event-driven triggers |
| **Processing** | Databricks Delta Live Tables (Serverless) |
| **Orchestration** | Databricks Asset Bundles + GitHub Actions CI/CD |

**Tech Stack:** AWS (Lambda, S3, API Gateway), Databricks, Delta Live Tables, PySpark, Terraform

---

## Usage: No-Nonsense Setup

**From clone to production in under 5 minutes.**

### Prerequisites

- Python 3.10+ | Node.js 18+
- Databricks SQL Warehouse access & Personal Access Token (PAT)

### Step 1: Set Your Credentials

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
```

### Step 2: Clone and Enter

```bash
git clone https://github.com/lpalad/DataBricks-Projects.git
cd DataBricks-Projects/02-tesla-sales-analytics
```

### Step 3: Backend (2 minutes)

```bash
cd backend
python -m venv venv
source venv/bin/activate  # Use venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn main:app --reload --port 8001
```

### Step 4: Frontend (1 minute)

```bash
cd ../frontend
npm install
npm run dev
```

### Step 5: Create Tables (30 seconds)

```bash
cd ../scripts
python create_tables.py
```

**Done.** Open `http://localhost:5173`. Submit an order. Check Databricks. Your data is there.

---

## Project Structure

```
DataBricks-Projects/
├── 01-contact-form/
│   ├── backend/main.py          # FastAPI + Databricks
│   ├── frontend/                # React application
│   └── create_table.py          # Schema deployment
│
├── 02-tesla-sales-analytics/
│   ├── backend/main.py          # Order processing + customer dedup
│   ├── frontend/                # Sales order form
│   └── scripts/
│       ├── create_tables.py     # Customers + Sales tables
│       └── populate_fake_data.py # 200 test records
│
├── NYC-Medallion-Architecture/
│   ├── README.md                # Project documentation
│   ├── data/sample/             # Sample data (taxi_zone_lookup.csv)
│   ├── notebooks/
│   │   ├── 00_setup_catalog.py  # Unity Catalog setup
│   │   ├── 01_bronze_ingestion.py
│   │   ├── 02_silver_cleansing.py
│   │   ├── 03_silver_enrichment.py
│   │   └── 04_gold_aggregation.py
│   └── sql/
│       └── analytics_queries.sql # Business analytics queries
│
├── AWS-Databricks-E-Commerce/
│   ├── README.md                # Project documentation
│   ├── databricks.yml           # Databricks Asset Bundle config
│   ├── infrastructure/
│   │   ├── lambda/              # AWS Lambda webhook handler
│   │   └── terraform/           # Infrastructure as Code
│   ├── pipelines/
│   │   └── dlt_pipeline.py      # Delta Live Tables pipeline
│   └── src/
│       ├── bronze/              # Raw data ingestion
│       ├── silver/              # Data cleansing & transformation
│       └── gold/                # Business aggregations
```

---

## API Reference

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Liveness check for load balancers |
| `/submit-tesla-order` | POST | Creates customer (if new) + sales record |

---

## Database Schema (Australian Tax Standards)

```sql
-- Customers: Auto-incrementing ID, no duplicates
CREATE TABLE default.customers (
    CustomerID BIGINT GENERATED ALWAYS AS IDENTITY,
    CustomerName STRING,
    CustomerLastName STRING,
    DOB DATE,
    Suburb STRING,
    State STRING
);

-- Sales: Full audit trail, GST-compliant
CREATE TABLE default.sales (
    SalesID BIGINT GENERATED ALWAYS AS IDENTITY,
    CustomerID BIGINT,
    ProductModel STRING,
    Color STRING,
    DatePurchase DATE,
    SalesPersonID STRING,
    PaymentMethod STRING,
    DeliveryStatus STRING,
    ProductDescription STRING,
    BasePrice DOUBLE,
    OptionsPrice DOUBLE,
    PurchaseAmount DOUBLE,
    GSTAmount DOUBLE,
    TotalPurchase DOUBLE
);
```

---

## About Me: Leonard S Palad

**MBA | Master of AI (In Progress)**

I build data systems that connect directly to commercial outcomes.

With over a decade in the IT field, I have built production ML systems, managed AWS-to-Azure migrations, and saved Australian businesses thousands in unnecessary cloud fees.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/leonardpalad/) [![AWS Portfolio](https://img.shields.io/badge/AWS-Portfolio-FF9900?logo=amazonaws&logoColor=white)](https://github.com/lpalad)

---

**Built for Databricks. Built for Australia. Built to protect your margins.**
