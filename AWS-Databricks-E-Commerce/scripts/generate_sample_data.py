#!/usr/bin/env python3
"""
Generate sample WooCommerce-style data for testing
Simulates 200 transactions per day
"""

import json
import random
import boto3
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker(['en_AU', 'en_US', 'en_GB'])

# Configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'ecommerce-landing-zone')
NUM_CUSTOMERS = 50
NUM_PRODUCTS = 30
NUM_ORDERS = 200  # Orders per day

# Sample product categories
CATEGORIES = [
    {'id': '1', 'name': 'Electronics'},
    {'id': '2', 'name': 'Books'},
    {'id': '3', 'name': 'Clothing'},
    {'id': '4', 'name': 'Home & Garden'},
    {'id': '5', 'name': 'Sports'},
]

ORDER_STATUSES = ['pending', 'processing', 'on-hold', 'completed', 'completed', 'completed']
PAYMENT_METHODS = ['cod', 'bacs', 'cheque']  # Cash on delivery focused


def generate_customer(customer_id: int) -> dict:
    """Generate a WooCommerce-style customer"""
    country = random.choice(['AU', 'AU', 'AU', 'US', 'GB', 'NZ'])

    if country == 'AU':
        fake_local = Faker('en_AU')
        states = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT']
    elif country == 'US':
        fake_local = Faker('en_US')
        states = ['CA', 'NY', 'TX', 'FL', 'WA']
    else:
        fake_local = Faker('en_GB')
        states = ['England', 'Scotland', 'Wales']

    first_name = fake_local.first_name()
    last_name = fake_local.last_name()

    return {
        'customer_id': str(customer_id),
        'email': f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}",
        'first_name': first_name,
        'last_name': last_name,
        'username': f"{first_name.lower()}{random.randint(1, 999)}",
        'date_created': (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat(),
        'date_modified': datetime.now().isoformat(),
        'billing': {
            'address_1': fake_local.street_address(),
            'city': fake_local.city(),
            'state': random.choice(states),
            'postcode': fake_local.postcode(),
            'country': country,
            'phone': fake_local.phone_number()
        },
        'shipping': {
            'address_1': fake_local.street_address(),
            'city': fake_local.city(),
            'state': random.choice(states),
            'postcode': fake_local.postcode(),
            'country': country
        }
    }


def generate_product(product_id: int) -> dict:
    """Generate a WooCommerce-style product"""
    category = random.choice(CATEGORIES)
    base_price = round(random.uniform(10, 500), 2)

    # Sometimes have a sale
    on_sale = random.random() < 0.3
    sale_price = round(base_price * random.uniform(0.7, 0.9), 2) if on_sale else None

    return {
        'product_id': str(product_id),
        'name': fake.catch_phrase(),
        'slug': fake.slug(),
        'sku': f"SKU-{product_id:04d}",
        'price': str(sale_price if sale_price else base_price),
        'regular_price': str(base_price),
        'sale_price': str(sale_price) if sale_price else '',
        'stock_quantity': random.randint(0, 100),
        'stock_status': 'instock' if random.random() > 0.1 else 'outofstock',
        'categories': [category],
        'date_created': (datetime.now() - timedelta(days=random.randint(30, 365))).isoformat(),
        'date_modified': datetime.now().isoformat()
    }


def generate_order(order_id: int, customers: list, products: list) -> dict:
    """Generate a WooCommerce-style order"""
    customer = random.choice(customers)

    # Generate 1-5 line items
    num_items = random.randint(1, 5)
    selected_products = random.sample(products, min(num_items, len(products)))

    line_items = []
    total = 0

    for product in selected_products:
        quantity = random.randint(1, 3)
        price = float(product['price'])
        line_total = quantity * price
        total += line_total

        line_items.append({
            'product_id': product['product_id'],
            'name': product['name'],
            'quantity': quantity,
            'price': str(price),
            'sku': product['sku']
        })

    order_date = datetime.now() - timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    return {
        'order_id': str(order_id),
        'customer_id': customer['customer_id'],
        'order_date': order_date.isoformat(),
        'status': random.choice(ORDER_STATUSES),
        'total': str(round(total, 2)),
        'currency': 'AUD',
        'payment_method': random.choice(PAYMENT_METHODS),
        'billing_address': {
            'first_name': customer['first_name'],
            'last_name': customer['last_name'],
            'address_1': customer['billing']['address_1'],
            'city': customer['billing']['city'],
            'state': customer['billing']['state'],
            'postcode': customer['billing']['postcode'],
            'country': customer['billing']['country'],
            'email': customer['email'],
            'phone': customer['billing']['phone']
        },
        'line_items': line_items
    }


def upload_to_s3(data: dict, prefix: str, entity_id: str, s3_client=None):
    """Upload JSON data to S3"""
    if s3_client is None:
        s3_client = boto3.client('s3')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    filename = f"{prefix}/{prefix[:-1]}_{entity_id}_{timestamp}.json"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=filename,
        Body=json.dumps(data, default=str),
        ContentType='application/json'
    )

    return filename


def save_locally(data: dict, prefix: str, entity_id: str, output_dir: str):
    """Save JSON data locally for testing"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')

    dir_path = os.path.join(output_dir, prefix)
    os.makedirs(dir_path, exist_ok=True)

    filename = f"{prefix[:-1]}_{entity_id}_{timestamp}.json"
    filepath = os.path.join(dir_path, filename)

    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)

    return filepath


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate sample e-commerce data')
    parser.add_argument('--upload', action='store_true', help='Upload to S3')
    parser.add_argument('--local', type=str, default='./sample_data', help='Local output directory')
    parser.add_argument('--customers', type=int, default=NUM_CUSTOMERS, help='Number of customers')
    parser.add_argument('--products', type=int, default=NUM_PRODUCTS, help='Number of products')
    parser.add_argument('--orders', type=int, default=NUM_ORDERS, help='Number of orders')

    args = parser.parse_args()

    print(f"Generating sample data...")
    print(f"  Customers: {args.customers}")
    print(f"  Products: {args.products}")
    print(f"  Orders: {args.orders}")
    print()

    s3_client = boto3.client('s3') if args.upload else None

    # Generate customers
    print("Generating customers...")
    customers = []
    for i in range(1, args.customers + 1):
        customer = generate_customer(i)
        customers.append(customer)

        if args.upload:
            upload_to_s3(customer, 'customers/', str(i), s3_client)
        else:
            save_locally(customer, 'customers/', str(i), args.local)
    print(f"  ✓ {len(customers)} customers generated")

    # Generate products
    print("Generating products...")
    products = []
    for i in range(1, args.products + 1):
        product = generate_product(i)
        products.append(product)

        if args.upload:
            upload_to_s3(product, 'products/', str(i), s3_client)
        else:
            save_locally(product, 'products/', str(i), args.local)
    print(f"  ✓ {len(products)} products generated")

    # Generate orders
    print("Generating orders...")
    for i in range(1, args.orders + 1):
        order = generate_order(i, customers, products)

        if args.upload:
            upload_to_s3(order, 'orders/', str(i), s3_client)
        else:
            save_locally(order, 'orders/', str(i), args.local)
    print(f"  ✓ {args.orders} orders generated")

    print()
    if args.upload:
        print(f"Data uploaded to s3://{S3_BUCKET}/")
    else:
        print(f"Data saved to {args.local}/")

    print("\nSummary:")
    print(f"  - {args.customers} customers")
    print(f"  - {args.products} products")
    print(f"  - {args.orders} orders")
    print(f"  - ~{args.orders * 2.5:.0f} line items (estimated)")


if __name__ == '__main__':
    main()
