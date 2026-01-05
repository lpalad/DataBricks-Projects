# Contact Form Application - Instructions

## Prerequisites

- Python 3.10+
- Node.js 18+
- Databricks workspace with SQL warehouse access

## Environment Variables

Set these in your terminal before running the backend:

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

## Running the Application

### Backend (FastAPI)

```bash
cd /Volumes/SSD-CRUCIAL/DataBricks-Projects/01-contact-form/backend
source venv/bin/activate
uvicorn main:app --reload
```

The API will be available at: http://localhost:8000

### Frontend (React + Vite)

```bash
cd /Volumes/SSD-CRUCIAL/DataBricks-Projects/01-contact-form/frontend
npm run dev
```

The frontend will be available at: http://localhost:5173

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/submit-contact` | POST | Submit a contact form |
| `/health` | GET | Health check |

## Database Setup

If you need to recreate the contacts table:

```bash
cd /Volumes/SSD-CRUCIAL/DataBricks-Projects/01-contact-form
./backend/venv/bin/python create_table.py
```
