import os
from databricks import sql
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker for generating realistic data
fake = Faker('en_AU')

# Get Databricks credentials from environment variables
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
http_path = os.getenv("DATABRICKS_HTTP_PATH")

# Validate that all credentials are set
if not host or not token or not http_path:
    print("❌ Error: Missing Databricks credentials!")
    exit(1)

# Tesla data
TESLA_MODELS = ['Model 3', 'Model Y', 'Model S', 'Model X', 'Cybertruck']
COLORS = ['Pearl White Multi-Coat', 'Solid Black', 'Stealth Grey', 'Ultra Violet', 'Midnight Blue', 'Solid Red']
PAYMENT_METHODS = ['Cash', 'Finance', 'Trade-in', 'Lease']
DELIVERY_STATUSES = ['Ordered', 'In Production', 'In Transit', 'Delivered', 'Pending']

# Base prices for each model (AUD)
BASE_PRICES = {
    'Model 3': 55000,
    'Model Y': 65000,
    'Model S': 125000,
    'Model X': 135000,
    'Cybertruck': 95000
}

SALES_PEOPLE = [f'SP{str(i).zfill(3)}' for i in range(1, 21)]

def generate_fake_customer():
    """Generate fake customer data"""
    first_name = fake.first_name()
    last_name = fake.last_name()
    dob = fake.date_of_birth(minimum_age=25, maximum_age=75).isoformat()
    suburb = fake.city()
    state = random.choice(['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT'])
    
    return {
        'CustomerName': first_name,
        'CustomerLastName': last_name,
        'DOB': dob,
        'Suburb': suburb,
        'State': state
    }

def generate_fake_sale(model):
    """Generate fake sale data"""
    base_price = BASE_PRICES[model]
    options_price = round(random.uniform(5000, 25000), 2)
    purchase_amount = base_price + options_price
    gst_amount = round(purchase_amount * 0.1, 2)
    total_purchase = purchase_amount + gst_amount
    
    purchase_date = fake.date_between(start_date='-6m').isoformat()
    
    descriptions = [
        'Long Range, All-Wheel Drive, Premium Interior',
        'Performance, Dual Motor, Enhanced AutoPilot',
        'Standard Range, RWD, Basic Package',
        'Long Range Plus, Full Self-Driving Capable',
        'Plaid, Tri-Motor, Ultra High Performance',
        'Cybertruck, All-Wheel Drive, Exoskeleton',
        'Model with 20-inch Wheels and Tinted Windows',
        'Premium Package with Heated Seats',
        'Standard Configuration'
    ]
    
    return {
        'ProductModel': model,
        'Color': random.choice(COLORS),
        'DatePurchase': purchase_date,
        'SalesPersonID': random.choice(SALES_PEOPLE),
        'PaymentMethod': random.choice(PAYMENT_METHODS),
        'DeliveryStatus': random.choice(DELIVERY_STATUSES),
        'ProductDescription': random.choice(descriptions),
        'BasePrice': base_price,
        'OptionsPrice': options_price,
        'PurchaseAmount': purchase_amount,
        'GSTAmount': gst_amount,
        'TotalPurchase': total_purchase
    }

def insert_customer(cursor, customer_data):
    """Insert customer and return CustomerID"""
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

def insert_sale(cursor, customer_id, sale_data):
    """Insert sale and return SalesID"""
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
            sale_data['ProductModel'],
            sale_data['Color'],
            sale_data['DatePurchase'],
            sale_data['SalesPersonID'],
            sale_data['PaymentMethod'],
            sale_data['DeliveryStatus'],
            sale_data['ProductDescription'],
            sale_data['BasePrice'],
            sale_data['OptionsPrice'],
            sale_data['PurchaseAmount'],
            sale_data['GSTAmount'],
            sale_data['TotalPurchase']
        )
    )
    
    # Get the last inserted SalesID
    cursor.execute("SELECT MAX(SalesID) FROM default.sales")
    result = cursor.fetchone()
    return result[0] if result else None

print("🔗 Connecting to Databricks...")

try:
    with sql.connect(
        host=host,
        http_path=http_path,
        auth_type="pat",
        token=token
    ) as connection:
        print("✓ Connected to Databricks!")
        
        cursor = connection.cursor()
        
        print(f"📝 Generating and inserting 200 fake Tesla sales records...")
        
        for i in range(200):
            # Generate fake customer
            customer_data = generate_fake_customer()
            customer_id = insert_customer(cursor, customer_data)
            
            if not customer_id:
                print(f"❌ Failed to insert customer {i+1}")
                continue
            
            # Generate and insert fake sale
            model = random.choice(TESLA_MODELS)
            sale_data = generate_fake_sale(model)
            sale_id = insert_sale(cursor, customer_id, sale_data)
            
            if not sale_id:
                print(f"❌ Failed to insert sale {i+1}")
                continue
            
            if (i + 1) % 50 == 0:
                print(f"  ✓ Inserted {i + 1} records...")
        
        cursor.close()
        
        print(f"\n✅ Successfully inserted 200 fake Tesla sales records!")
        print("\nSummary:")
        print(f"  - 200 customers created")
        print(f"  - 200 sales transactions recorded")
        print(f"  - Models distributed across: {', '.join(TESLA_MODELS)}")
        print(f"  - Colors: {len(COLORS)} variants")
        print(f"  - Payment methods: {len(PAYMENT_METHODS)} types")
        print(f"  - Date range: Last 6 months")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

print("\n✅ Data population complete! Ready for analytics.")