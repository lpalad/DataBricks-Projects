# NYC Taxi Medallion Architecture Pipeline

End-to-end data pipeline implementing Medallion Architecture (Bronze → Silver → Gold) in Databricks, processing NYC Yellow Taxi trip data using PySpark and Delta Lake.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              MEDALLION ARCHITECTURE                                          │
├──────────────┬──────────────┬────────────────────────┬──────────────────────────────────────┤
│   LANDING    │    BRONZE    │         SILVER         │                GOLD                  │
├──────────────┼──────────────┼────────────────────────┼──────────────────────────────────────┤
│              │              │                        │                                      │
│  Raw Parquet │ yellow_trips │  yellow_trips_cleansed │  yellow_trips_enriched               │
│    Files     │    _raw      │                        │                                      │
│              │              │  taxi_zone_lookup      │  daily_trip_summary                  │
│              │              │                        │                                      │
│              │              │  yellow_trips_joined   │                                      │
│              │              │                        │                                      │
└──────────────┴──────────────┴────────────────────────┴──────────────────────────────────────┘
```

## Tech Stack

| Category | Technologies |
|----------|--------------|
| **Platform** | Databricks, Apache Spark |
| **Languages** | PySpark, SQL |
| **Storage** | Delta Lake, Parquet |
| **Architecture** | Medallion (Bronze/Silver/Gold), Data Lakehouse |
| **Governance** | Unity Catalog |

## Project Structure

```
NYC-Medallion-Architecture/
├── README.md
├── data/
│   └── sample/
│       ├── taxi_zone_lookup.csv
│       └── yellow_tripdata_2025-*.parquet
├── notebooks/
│   ├── 00_setup_catalog.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleansing.py
│   ├── 03_silver_enrichment.py
│   └── 04_gold_aggregation.py
├── images/
│   └── (screenshots of catalog structure, outputs)
└── sql/
    └── analytics_queries.sql
```

## Pipeline Steps

### Step 0: Setup Unity Catalog
Creates the catalog, schemas, and volumes for the medallion architecture.

```python
CREATE CATALOG IF NOT EXISTS nyctaxi;
CREATE SCHEMA IF NOT EXISTS nyctaxi.`00_landing`;
CREATE SCHEMA IF NOT EXISTS nyctaxi.`01_bronze`;
CREATE SCHEMA IF NOT EXISTS nyctaxi.`02_silver`;
CREATE SCHEMA IF NOT EXISTS nyctaxi.`03_gold`;
CREATE VOLUME IF NOT EXISTS nyctaxi.`00_landing`.data_sources;
```

### Step 1: Bronze Layer - Raw Ingestion
- Reads raw Parquet files from landing zone
- Adds `processed_timestamp` for audit tracking
- Saves as Delta table: `nyctaxi.01_bronze.yellow_trips_raw`

### Step 2: Silver Layer - Data Cleansing
- Filters data to valid date range
- Maps coded values to human-readable names:
  - VendorID → Vendor names
  - RatecodeID → Rate type descriptions
  - payment_type → Payment method names
- Calculates `trip_duration` in minutes
- Renames columns to snake_case convention
- Saves as Delta table: `nyctaxi.02_silver.yellow_trips_cleansed`

### Step 3: Silver Layer - Data Enrichment
- Joins with `taxi_zone_lookup` dimension table
- Enriches pickup location: `pu_borough`, `pu_zone`
- Enriches dropoff location: `do_borough`, `do_zone`
- Saves as Delta table: `nyctaxi.02_silver.yellow_trips_joined`

### Step 4: Gold Layer - Aggregation
- Groups by pickup date
- Calculates daily metrics:
  - `total_trips`
  - `average_passengers`
  - `average_distance`
  - `average_fare_per_trip`
  - `max_fare`, `min_fare`
  - `total_revenue`
- Saves as Delta table: `nyctaxi.03_gold.daily_trip_summary`

## Data Transformations

### Value Mappings

**Vendor ID:**
| Code | Vendor Name |
|------|-------------|
| 1 | Creative Mobile Technologies, LLC |
| 2 | Curb Mobility, LLC |
| 6 | Myle Technologies Inc |
| 7 | Helix |

**Rate Type:**
| Code | Rate Type |
|------|-----------|
| 1 | Standard Rate |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau or Westchester |
| 5 | Negotiated Fare |
| 6 | Group Ride |

**Payment Type:**
| Code | Payment Method |
|------|----------------|
| 0 | Flex Fare trip |
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 6 | Voided trip |

## Sample Queries

```sql
-- Which vendor makes the most revenue?
SELECT vendor, ROUND(SUM(total_amount), 2) AS total_revenue
FROM nyctaxi.02_silver.yellow_trips_joined
GROUP BY vendor
ORDER BY total_revenue DESC;

-- Most popular pickup borough
SELECT pu_borough, COUNT(*) AS total_trips
FROM nyctaxi.02_silver.yellow_trips_joined
WHERE pu_borough IS NOT NULL
GROUP BY pu_borough
ORDER BY total_trips DESC;

-- Most common journey (borough to borough)
SELECT pu_borough, do_borough, COUNT(*) AS total_trips
FROM nyctaxi.02_silver.yellow_trips_joined
WHERE pu_borough IS NOT NULL AND do_borough IS NOT NULL
GROUP BY pu_borough, do_borough
ORDER BY total_trips DESC;
```

## Setup Instructions

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Access to create catalogs, schemas, and volumes

### Steps

1. **Upload Data to Databricks Volume**
   ```bash
   # Upload via Databricks CLI or UI
   databricks fs cp data/sample/ /Volumes/nyctaxi/00_landing/data_sources/ --recursive
   ```

2. **Run Notebooks in Order**
   ```
   00_setup_catalog.py → 01_bronze_ingestion.py → 02_silver_cleansing.py → 03_silver_enrichment.py → 04_gold_aggregation.py
   ```

3. **Query the Gold Layer**
   ```sql
   SELECT * FROM nyctaxi.03_gold.daily_trip_summary ORDER BY pickup_date;
   ```

## Key Learnings

- **Serverless Compute**: No public internet access - data must be uploaded via UI/CLI
- **Unity Catalog**: Provides governance, lineage tracking, and managed storage
- **Delta Lake**: Enables ACID transactions, time travel, and schema evolution
- **Medallion Architecture**: Bronze (raw) → Silver (cleansed/enriched) → Gold (aggregated)

## Data Source

NYC Taxi & Limousine Commission Trip Record Data:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

**Author:** [Your Name]
**GitHub:** [github.com/lpalad/DataBricks-Projects](https://github.com/lpalad/DataBricks-Projects)
