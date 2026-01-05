import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from databricks import sql
from datetime import datetime

# Initialize FastAPI app
app = FastAPI()

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic model for Tesla order validation
class TeslaSalesOrder(BaseModel):
    # Customer fields
    CustomerName: str
    CustomerLastName: str
    DOB: str
    Suburb: str
    State: str
    # Sales fields
    ProductModel: str
    Color: str
    DatePurchase: str
    SalesPersonID: str = None
    PaymentMethod: str = None
    DeliveryStatus: str = "Ordered"
    ProductDescription: str = None
    BasePrice: float
    OptionsPrice: float = 0
    PurchaseAmount: float
    GSTAmount: float
    TotalPurchase: float

# Get Databricks credentials from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Validate credentials exist
if not DATABRICKS_HOST or not DATABRICKS_TOKEN or not DATABRICKS_HTTP_PATH:
    raise ValueError("Missing Databricks credentials in environment variables")

def get_databricks_connection():
    """Helper function to create Databricks connection"""
    # Remove https:// prefix if present
    server_hostname = DATABRICKS_HOST.replace("https://", "").replace("http://", "")

    return sql.connect(
        server_hostname=server_hostname,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

def customer_exists(cursor, customer_name: str, customer_last_name: str, dob: str):
    """
    Check if customer already exists in the database.
    Returns: (exists: bool, customer_id: int or None)
    """
    try:
        query = """
        SELECT CustomerID FROM default.customers 
        WHERE CustomerName = ? AND CustomerLastName = ? AND DOB = ?
        """
        cursor.execute(query, (customer_name, customer_last_name, dob))
        result = cursor.fetchone()
        
        if result:
            return True, result[0]
        return False, None
    except Exception as e:
        print(f"Error checking customer existence: {e}")
        return False, None

def insert_customer(cursor, customer_data: dict):
    """
    Insert new customer into customers table.
    Returns: CustomerID
    """
    insert_query = """
    INSERT INTO default.customers (CustomerName, CustomerLastName, DOB, Suburb, State)
    VALUES (?, ?, ?, ?, ?)
    """

    cursor.execute(
        insert_query,
        (
            customer_data['CustomerName'],
            customer_data['CustomerLastName'],
            customer_data['DOB'],
            customer_data['Suburb'],
            customer_data['State']
        )
    )

    # Get the last inserted CustomerID
    cursor.execute("SELECT MAX(CustomerID) FROM default.customers")
    result = cursor.fetchone()
    return result[0] if result else None

def insert_sale(cursor, customer_id: int, sales_data: dict):
    """
    Insert sale record into sales table.
    Returns: SalesID
    """
    insert_query = """
    INSERT INTO default.sales (
        CustomerID, ProductModel, Color, DatePurchase, SalesPersonID,
        PaymentMethod, DeliveryStatus, ProductDescription, BasePrice,
        OptionsPrice, PurchaseAmount, GSTAmount, TotalPurchase
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.execute(
        insert_query,
        (
            customer_id,
            sales_data['ProductModel'],
            sales_data['Color'],
            sales_data['DatePurchase'],
            sales_data['SalesPersonID'],
            sales_data['PaymentMethod'],
            sales_data['DeliveryStatus'],
            sales_data['ProductDescription'],
            sales_data['BasePrice'],
            sales_data['OptionsPrice'],
            sales_data['PurchaseAmount'],
            sales_data['GSTAmount'],
            sales_data['TotalPurchase']
        )
    )

    # Get the last inserted SalesID
    cursor.execute("SELECT MAX(SalesID) FROM default.sales")
    result = cursor.fetchone()
    return result[0] if result else None

@app.post("/submit-tesla-order")
async def submit_tesla_order(order: TeslaSalesOrder):
    """
    Handle Tesla sales order submission.
    1. Check if customer exists
    2. If not, create new customer and get CustomerID
    3. Insert sale record with CustomerID
    4. Return success response
    """
    try:
        with get_databricks_connection() as connection:
            cursor = connection.cursor()
            
            # Check if customer already exists
            exists, existing_customer_id = customer_exists(
                cursor,
                order.CustomerName,
                order.CustomerLastName,
                order.DOB
            )
            
            if exists:
                customer_id = existing_customer_id
                customer_status = "existing"
            else:
                # Customer doesn't exist, create new one
                customer_data = {
                    'CustomerName': order.CustomerName,
                    'CustomerLastName': order.CustomerLastName,
                    'DOB': order.DOB,
                    'Suburb': order.Suburb,
                    'State': order.State
                }
                customer_id = insert_customer(cursor, customer_data)
                customer_status = "new"
                
                if not customer_id:
                    raise Exception("Failed to create customer record")
            
            # Insert sales record with the customer ID
            sales_data = {
                'ProductModel': order.ProductModel,
                'Color': order.Color,
                'DatePurchase': order.DatePurchase,
                'SalesPersonID': order.SalesPersonID,
                'PaymentMethod': order.PaymentMethod,
                'DeliveryStatus': order.DeliveryStatus,
                'ProductDescription': order.ProductDescription,
                'BasePrice': order.BasePrice,
                'OptionsPrice': order.OptionsPrice,
                'PurchaseAmount': order.PurchaseAmount,
                'GSTAmount': order.GSTAmount,
                'TotalPurchase': order.TotalPurchase
            }
            sales_id = insert_sale(cursor, customer_id, sales_data)
            
            if not sales_id:
                raise Exception("Failed to create sales record")
            
            cursor.close()
            
            return {
                "status": "success",
                "message": "Tesla order submitted successfully!",
                "data": {
                    "customer_id": customer_id,
                    "customer_status": customer_status,
                    "sales_id": sales_id,
                    "customer_name": order.CustomerName,
                    "product_model": order.ProductModel,
                    "total_purchase": order.TotalPurchase
                }
            }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing order: {str(e)}")

@app.get("/health")
async def health_check():
    """
    Simple health check endpoint
    """
    return {"status": "healthy"}

@app.get("/")
async def root():
    """
    Root endpoint
    """
    return {"message": "Tesla Sales Analytics API v1.0"}