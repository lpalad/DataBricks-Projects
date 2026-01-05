import os
import random
from datetime import datetime, timedelta
from databricks import sql

# Read credentials from environment variables
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
http_path = os.getenv("DATABRICKS_HTTP_PATH")

# Validate that all credentials are set
if not host or not token or not http_path:
    print("❌ Error: Missing Databricks credentials!")
    print("Make sure to set these environment variables:")
    print("  - DATABRICKS_HOST")
    print("  - DATABRICKS_TOKEN")
    print("  - DATABRICKS_HTTP_PATH")
    exit(1)

# Fake data options
first_names = [
    "James", "Emma", "Oliver", "Sophia", "William", "Ava", "Benjamin", "Isabella",
    "Lucas", "Mia", "Henry", "Charlotte", "Alexander", "Amelia", "Sebastian",
    "Harper", "Jack", "Evelyn", "Aiden", "Abigail", "Owen", "Emily", "Samuel",
    "Elizabeth", "Ryan", "Sofia", "Nathan", "Avery", "Leo", "Ella"
]

last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"
]

suburbs = [
    "Bondi", "Manly", "Parramatta", "Chatswood", "Surry Hills", "Newtown",
    "St Kilda", "South Yarra", "Fitzroy", "Richmond", "Brunswick", "Collingwood",
    "Fortitude Valley", "South Brisbane", "West End", "Paddington",
    "Fremantle", "Subiaco", "Cottesloe", "Claremont",
    "Glenelg", "Norwood", "Unley", "Hyde Park",
    "Sandy Bay", "Battery Point", "North Hobart", "New Town"
]

states = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]

tesla_models = ["Model 3", "Model Y", "Model S", "Model X", "Cybertruck"]

colors = [
    "Pearl White Multi-Coat", "Solid Black", "Stealth Grey",
    "Ultra Violet", "Midnight Blue", "Solid Red"
]

payment_methods = ["Cash", "Finance", "Trade-in", "Lease"]

delivery_statuses = ["Ordered", "In Production", "In Transit", "Delivered", "Pending"]

sales_person_ids = ["SP001", "SP002", "SP003", "SP004", "SP005", "SP006", "SP007", "SP008"]

# Base prices for each model (AUD)
model_base_prices = {
    "Model 3": 55000,
    "Model Y": 65000,
    "Model S": 120000,
    "Model X": 130000,
    "Cybertruck": 95000
}


def random_date(start_year=1960, end_year=2000):
    """Generate a random date of birth"""
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")


def random_purchase_date():
    """Generate a random purchase date within last 2 years"""
    start_date = datetime.now() - timedelta(days=730)
    random_days = random.randint(0, 730)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")


def generate_customer():
    """Generate a random customer"""
    return {
        "CustomerName": random.choice(first_names),
        "CustomerLastName": random.choice(last_names),
        "DOB": random_date(),
        "Suburb": random.choice(suburbs),
        "State": random.choice(states)
    }


def generate_sale(customer_id):
    """Generate a random sale for a customer"""
    model = random.choice(tesla_models)
    base_price = model_base_prices[model] + random.randint(-5000, 10000)
    options_price = random.randint(0, 15000)
    purchase_amount = base_price + options_price
    gst_amount = round(purchase_amount * 0.1, 2)
    total_purchase = round(purchase_amount + gst_amount, 2)

    return {
        "CustomerID": customer_id,
        "ProductModel": model,
        "Color": random.choice(colors),
        "DatePurchase": random_purchase_date(),
        "SalesPersonID": random.choice(sales_person_ids),
        "PaymentMethod": random.choice(payment_methods),
        "DeliveryStatus": random.choice(delivery_statuses),
        "ProductDescription": f"{model} - {random.choice(['Standard Range', 'Long Range', 'Performance'])}",
        "BasePrice": base_price,
        "OptionsPrice": options_price,
        "PurchaseAmount": purchase_amount,
        "GSTAmount": gst_amount,
        "TotalPurchase": total_purchase
    }


def main():
    num_customers = 50  # Number of customers to create
    max_sales_per_customer = 3  # Maximum sales per customer

    print("🔗 Connecting to Databricks...")

    try:
        # Connect to Databricks
        server_hostname = host.replace("https://", "").replace("http://", "")

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=token
        ) as connection:
            print("✓ Connected to Databricks!")

            cursor = connection.cursor()

            customers_created = 0
            sales_created = 0

            print(f"\n📝 Creating {num_customers} customers with sales records...")

            for i in range(num_customers):
                # Generate and insert customer
                customer = generate_customer()

                insert_customer = """
                INSERT INTO default.customers (CustomerName, CustomerLastName, DOB, Suburb, State)
                VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(
                    insert_customer,
                    (
                        customer["CustomerName"],
                        customer["CustomerLastName"],
                        customer["DOB"],
                        customer["Suburb"],
                        customer["State"]
                    )
                )
                customers_created += 1

                # Get the customer ID
                cursor.execute("SELECT MAX(CustomerID) FROM default.customers")
                customer_id = cursor.fetchone()[0]

                # Generate 1-3 sales for this customer
                num_sales = random.randint(1, max_sales_per_customer)
                for _ in range(num_sales):
                    sale = generate_sale(customer_id)

                    insert_sale = """
                    INSERT INTO default.sales (
                        CustomerID, ProductModel, Color, DatePurchase, SalesPersonID,
                        PaymentMethod, DeliveryStatus, ProductDescription, BasePrice,
                        OptionsPrice, PurchaseAmount, GSTAmount, TotalPurchase
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    cursor.execute(
                        insert_sale,
                        (
                            sale["CustomerID"],
                            sale["ProductModel"],
                            sale["Color"],
                            sale["DatePurchase"],
                            sale["SalesPersonID"],
                            sale["PaymentMethod"],
                            sale["DeliveryStatus"],
                            sale["ProductDescription"],
                            sale["BasePrice"],
                            sale["OptionsPrice"],
                            sale["PurchaseAmount"],
                            sale["GSTAmount"],
                            sale["TotalPurchase"]
                        )
                    )
                    sales_created += 1

                # Progress indicator
                if (i + 1) % 10 == 0:
                    print(f"  ✓ Created {i + 1}/{num_customers} customers...")

            cursor.close()

            print(f"\n✅ Data population complete!")
            print(f"   - Customers created: {customers_created}")
            print(f"   - Sales records created: {sales_created}")

    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
