import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from databricks import sql
from datetime import datetime

# Initialize FastAPI app
app = FastAPI()

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to specific origin in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic model for form validation
class ContactForm(BaseModel):
    full_name: str
    email: EmailStr
    mobile: str = None
    message: str

# Get Databricks credentials from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Validate credentials exist
if not DATABRICKS_HOST or not DATABRICKS_TOKEN or not DATABRICKS_HTTP_PATH:
    raise ValueError("Missing Databricks credentials in environment variables")

@app.post("/submit-contact")
async def submit_contact(form: ContactForm):
    """
    Receives contact form data and inserts into Databricks table
    """
    try:
        # Connect to Databricks
        # Remove https:// prefix if present
        server_hostname = DATABRICKS_HOST.replace("https://", "").replace("http://", "")

        with sql.connect(
            server_hostname=server_hostname,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        ) as connection:
            cursor = connection.cursor()
            
            # Insert data into contacts table
            insert_query = """
            INSERT INTO default.contacts (full_name, email, mobile, message)
            VALUES (?, ?, ?, ?)
            """
            
            cursor.execute(
                insert_query,
                (form.full_name, form.email, form.mobile, form.message)
            )
            
            cursor.close()
        
        return {
            "status": "success",
            "message": "Contact form submitted successfully!",
            "data": {
                "full_name": form.full_name,
                "email": form.email
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting data: {str(e)}")

@app.get("/health")
async def health_check():
    """
    Simple health check endpoint
    """
    return {"status": "healthy"}