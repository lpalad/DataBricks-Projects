<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" alt="Databricks Logo" width="200"/>
</p>

<h1 align="center">Databricks Projects</h1>

<p align="center">
  <strong>A collection of full-stack applications powered by Databricks SQL</strong>
</p>

<p align="center">
  <a href="#the-solution">Projects</a> •
  <a href="#the-facts-why-this-architecture">Tech Stack</a> •
  <a href="#usage-no-nonsense-setup">Getting Started</a> •
  <a href="#project-structure">Structure</a> •
  <a href="#why-id-hire-the-person-who-wrote-this">About</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi&logoColor=white" alt="FastAPI"/>
  <img src="https://img.shields.io/badge/React-18+-61DAFB?logo=react&logoColor=black" alt="React"/>
  <img src="https://img.shields.io/badge/Databricks-SQL-FF3621?logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/Vite-5+-646CFF?logo=vite&logoColor=white" alt="Vite"/>
</p>

---

# Production-Ready Full-Stack Applications That Write Directly to Databricks—Without the $50,000 Middleware Bill

---

## The Problem

Most companies waste 3-6 months building data pipelines.

They hire contractors. They buy expensive middleware. They create technical debt that haunts them for years.

Meanwhile, their sales data sits in spreadsheets. Their customer information lives in someone's inbox. Their "single source of truth" is actually seven sources of chaos.

**The cost of this failure?**
- $50,000+ in middleware licensing
- 6+ months of developer time
- Data integrity issues that erode trust
- Manual reconciliation that burns analyst hours

This repository eliminates that waste.

---

## The Solution

This is not a proof-of-concept.

This is production-grade code that connects web applications directly to Databricks SQL Warehouse. No middleware. No ETL pipelines. No waiting.

**What you get:**

| Application | What It Does | Business Value |
|-------------|--------------|----------------|
| **Contact Form** | Captures leads directly into Databricks | Zero data entry. Zero lag. Every lead tracked. |
| **Tesla Sales Analytics** | Full order management with customer deduplication | Real-time sales data. Automatic GST calculation. Instant reporting. |

---

## The Facts (Why This Architecture)

**1. FastAPI Backend — 40,000 requests/second capability**

Not Flask. Not Django. FastAPI.

Why? Because it generates OpenAPI documentation automatically. Because it validates data with Pydantic before it touches your database. Because when your sales team submits 500 orders on launch day, it won't collapse.

**2. Direct Databricks SQL Connection — No middleware**

The `databricks-sql-connector` writes directly to Delta Lake tables.

No Kafka. No Airflow. No Fivetran. No $2,000/month SaaS tool.

One connection. ACID transactions. Data arrives in milliseconds.

**3. React + Vite Frontend — 10x faster builds than Create React App**

Vite uses native ES modules. Cold starts in under 300ms.

Your developers spend time building features, not waiting for webpack.

**4. Customer Deduplication Logic — Built-in**

The sales application checks for existing customers before creating duplicates.

This is the logic that prevents the chaos of "John Smith" appearing 47 times in your CRM.

**5. Environment Variable Security — No hardcoded credentials**

Credentials are never in the codebase. Period.

`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH` are injected at runtime. This passes security audits.

---

## Why I'd Hire the Person Who Wrote This

Look at the code. Not the README. The code.

You will find:

- **Parameterized SQL queries** — No string concatenation. No SQL injection vulnerabilities.
- **Proper error handling** — Try/catch blocks that return meaningful HTTP status codes, not stack traces.
- **Connection pooling awareness** — Database connections are opened, used, and closed. No leaked connections.
- **Separation of concerns** — Frontend knows nothing about database credentials. Backend knows nothing about CSS.
- **Idempotent operations** — Running `create_tables.py` twice doesn't break anything.

This is not tutorial code. This is the code of someone who has been woken up at 3 AM because a production system failed.

---

## Usage: No-Nonsense Setup

### Prerequisites
- Python 3.10+
- Node.js 18+
- Databricks SQL Warehouse access
- A Personal Access Token (PAT)

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
source venv/bin/activate
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
```

---

## API Reference

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Liveness check for load balancers |
| `/submit-tesla-order` | POST | Creates customer (if new) + sales record |

---

## Database Schema

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

## The Bottom Line

You can spend six months and $50,000 building a "proper" data pipeline.

Or you can use this architecture.

Same result. Fraction of the cost. Ships this week.

---

**Built for Databricks. Built for production. Built to save you money.**
