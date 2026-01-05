# DataBricks Projects

A collection of projects demonstrating Databricks integration with modern web applications.

## Projects

### 01-contact-form

A full-stack contact form application that stores submissions in Databricks.

**Tech Stack:**
- **Backend:** FastAPI (Python)
- **Frontend:** React + Vite
- **Database:** Databricks SQL

**Setup:**

1. Set environment variables:
   ```bash
   export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   export DATABRICKS_TOKEN=your-token
   export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   ```

2. Create the database table:
   ```bash
   cd 01-contact-form
   python create_table.py
   ```

3. Start the backend:
   ```bash
   cd 01-contact-form/backend
   source venv/bin/activate
   uvicorn main:app --reload
   ```

4. Start the frontend:
   ```bash
   cd 01-contact-form/frontend
   npm run dev
   ```

### 02-tesla-sales-analytics

Tesla sales analytics dashboard (coming soon).

**Planned Tech Stack:**
- **Backend:** FastAPI (Python)
- **Frontend:** React + Vite
- **Database:** Databricks SQL
- **Scripts:** Data ingestion and analysis scripts

## Project Structure

```
DataBricks-Projects/
├── 01-contact-form/
│   ├── backend/          # FastAPI server
│   ├── frontend/         # React application
│   └── create_table.py   # Database setup script
├── 02-tesla-sales-analytics/
│   ├── backend/          # FastAPI server
│   ├── frontend/         # React application
│   └── scripts/          # Data scripts
└── README.md
```

## Requirements

- Python 3.10+
- Node.js 18+
- Databricks workspace with SQL warehouse access
