<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" alt="Databricks Logo" width="200"/>
</p>

<h1 align="center">Databricks Projects</h1>

<p align="center">
  <strong>A collection of full-stack applications powered by Databricks SQL</strong>
</p>

<p align="center">
  <a href="#projects">Projects</a> •
  <a href="#tech-stack">Tech Stack</a> •
  <a href="#getting-started">Getting Started</a> •
  <a href="#project-structure">Structure</a> •
  <a href="#contributing">Contributing</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi&logoColor=white" alt="FastAPI"/>
  <img src="https://img.shields.io/badge/React-18+-61DAFB?logo=react&logoColor=black" alt="React"/>
  <img src="https://img.shields.io/badge/Databricks-SQL-FF3621?logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/Vite-5+-646CFF?logo=vite&logoColor=white" alt="Vite"/>
</p>

---

## Overview

This repository contains full-stack web applications that demonstrate integration with **Databricks SQL Warehouse** for data storage and analytics. Each project features a React frontend, FastAPI backend, and connects to Databricks for persistent data storage.

## Projects

### 01. Contact Form
> A simple contact form application that stores submissions in Databricks.

| Feature | Description |
|---------|-------------|
| **Purpose** | Capture and store contact form submissions |
| **Database** | `default.contacts` table |
| **Fields** | Name, Email, Phone, Subject, Message |

### 02. Tesla Sales Analytics
> A comprehensive Tesla vehicle sales tracking system with customer management.

| Feature | Description |
|---------|-------------|
| **Purpose** | Track Tesla vehicle orders and customer data |
| **Database** | `default.customers` and `default.sales` tables |
| **Models** | Model 3, Model Y, Model S, Model X, Cybertruck |
| **Features** | Customer lookup, order submission, GST calculation |

**Sales Form Features:**
- Customer information capture (auto-detection of existing customers)
- Vehicle configuration (model, color, options)
- Real-time pricing calculation with GST
- Payment method and delivery status tracking

## Tech Stack

<table>
<tr>
<td align="center" width="120">
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="48" height="48" alt="Python" />
<br><strong>Python</strong>
</td>
<td align="center" width="120">
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/fastapi/fastapi-original.svg" width="48" height="48" alt="FastAPI" />
<br><strong>FastAPI</strong>
</td>
<td align="center" width="120">
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/react/react-original.svg" width="48" height="48" alt="React" />
<br><strong>React</strong>
</td>
<td align="center" width="120">
<img src="https://vitejs.dev/logo.svg" width="48" height="48" alt="Vite" />
<br><strong>Vite</strong>
</td>
<td align="center" width="120">
<img src="https://www.databricks.com/wp-content/uploads/2022/06/db-nav-logo.svg" width="48" height="48" alt="Databricks" />
<br><strong>Databricks</strong>
</td>
</tr>
</table>

### Backend
- **FastAPI** - Modern Python web framework with automatic OpenAPI documentation
- **databricks-sql-connector** - Official Databricks SQL connector for Python
- **Pydantic** - Data validation using Python type annotations
- **Uvicorn** - Lightning-fast ASGI server

### Frontend
- **React 18** - Component-based UI library
- **Vite** - Next-generation frontend build tool
- **Axios** - Promise-based HTTP client

### Database
- **Databricks SQL Warehouse** - Serverless SQL analytics
- **Delta Lake** - ACID transactions on data lakes

## Getting Started

### Prerequisites

- Python 3.10 or higher
- Node.js 18 or higher
- Databricks account with SQL Warehouse access
- Personal Access Token (PAT) for Databricks

### Environment Variables

Create a `.env` file or set these environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
```

### Installation

#### 1. Clone the repository

```bash
git clone https://github.com/lpalad/DataBricks-Projects.git
cd DataBricks-Projects
```

#### 2. Set up a project (e.g., Tesla Sales Analytics)

```bash
cd 02-tesla-sales-analytics
```

#### 3. Backend Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

#### 4. Frontend Setup

```bash
cd ../frontend
npm install
```

#### 5. Create Database Tables

```bash
cd ../scripts
python create_tables.py
```

#### 6. (Optional) Populate with Fake Data

```bash
python populate_fake_data.py
```

### Running the Applications

**Backend** (from project backend folder):
```bash
source venv/bin/activate
uvicorn main:app --reload --port 8001
```

**Frontend** (from project frontend folder):
```bash
npm run dev
```

Access the application at `http://localhost:5173`

## Project Structure

```
DataBricks-Projects/
├── 01-contact-form/
│   ├── backend/
│   │   ├── main.py              # FastAPI application
│   │   ├── requirements.txt
│   │   └── venv/
│   ├── frontend/
│   │   ├── src/
│   │   ├── package.json
│   │   └── vite.config.js
│   └── create_table.py          # Database setup script
│
├── 02-tesla-sales-analytics/
│   ├── backend/
│   │   ├── main.py              # FastAPI application
│   │   ├── requirements.txt
│   │   └── venv/
│   ├── frontend/
│   │   ├── src/
│   │   │   ├── components/
│   │   │   │   └── TeslaSalesForm.jsx
│   │   │   └── styles/
│   │   ├── package.json
│   │   └── vite.config.js
│   ├── scripts/
│   │   ├── create_tables.py     # Database setup
│   │   └── populate_fake_data.py
│   ├── instructions.md
│   └── README.md
│
├── .gitignore
└── README.md                    # This file
```

## API Endpoints

### Contact Form (Port 8000)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API info |
| `GET` | `/health` | Health check |
| `POST` | `/submit-contact` | Submit contact form |

### Tesla Sales Analytics (Port 8001)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API info |
| `GET` | `/health` | Health check |
| `POST` | `/submit-tesla-order` | Submit Tesla order |

## Database Schema

### Customers Table
```sql
CREATE TABLE default.customers (
    CustomerID BIGINT GENERATED ALWAYS AS IDENTITY,
    CustomerName STRING,
    CustomerLastName STRING,
    DOB DATE,
    Suburb STRING,
    State STRING
);
```

### Sales Table
```sql
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is open source and available under the [MIT License](LICENSE).

---

<p align="center">
  Made with ❤️ using <strong>Databricks</strong> and <strong>FastAPI</strong>
</p>
