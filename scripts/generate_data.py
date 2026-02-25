from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os

fake = Faker()

def generate_customers(num_customers=1000):
    """Generate fake customer data"""
    customers = []
    segments = ['Premium', 'Regular', 'Budget']
    emails_used = set()  # Track unique emails
    
    while len(customers) < num_customers:
        email = fake.email()
        
        # Only add if email is unique
        if email not in emails_used:
            customer = {
                'customer_name': fake.name(),
                'email': email,
                'phone': fake.phone_number(),
                'registration_date': fake.date_between(start_date='-2y', end_date='today'),
                'customer_segment': random.choice(segments)
            }
            customers.append(customer)
            emails_used.add(email)
    
    return pd.DataFrame(customers)

def generate_products(num_products=200):
    """Generate fake product data"""
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
        'Clothing': ['Men', 'Women', 'Kids', 'Accessories'],
        'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Tools'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics'],
        'Sports': ['Equipment', 'Apparel', 'Accessories', 'Nutrition']
    }
    
    products = []
    for i in range(num_products):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        product = {
            'product_name': fake.catch_phrase(),
            'category': category,
            'subcategory': subcategory,
            'unit_price': round(random.uniform(10, 500), 2),
            'supplier': fake.company()
        }
        products.append(product)
    
    return pd.DataFrame(products)

def generate_locations(num_locations=100):
    """Generate fake location data"""
    locations = []
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    for i in range(num_locations):
        location = {
            'city': fake.city(),
            'state': fake.state(),
            'country': 'USA',
            'region': random.choice(regions)
        }
        locations.append(location)
    
    return pd.DataFrame(locations)

def generate_dates(start_date='2024-01-01', end_date='2026-02-10'):
    """Generate date dimension data"""
    dates = []
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    while current <= end:
        date_record = {
            'full_date': current.date(),
            'day_of_week': current.strftime('%A'),
            'day_of_month': current.day,
            'month': current.month,
            'quarter': (current.month - 1) // 3 + 1,
            'year': current.year,
            'is_weekend': current.weekday() >= 5
        }
        dates.append(date_record)
        current += timedelta(days=1)
    
    return pd.DataFrame(dates)

def generate_sales(num_sales=10000, num_customers=1000, num_products=200, num_locations=100):
    """Generate fake sales transactions"""
    sales = []
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer']
    statuses = ['Completed', 'Pending', 'Cancelled', 'Refunded']
    
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2026, 2, 10)
    
    for i in range(num_sales):
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 500), 2)
        discount = round(random.uniform(0, 20), 2)
        subtotal = quantity * unit_price
        discount_amount = subtotal * (discount / 100)
        tax = (subtotal - discount_amount) * 0.08  # 8% tax
        total = subtotal - discount_amount + tax
        
        sale = {
            'customer_id': random.randint(1, num_customers),
            'product_id': random.randint(1, num_products),
            'location_id': random.randint(1, num_locations),
            'sale_date': fake.date_between(start_date=start_date, end_date=end_date),
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_percent': discount,
            'tax_amount': round(tax, 2),
            'total_amount': round(total, 2),
            'payment_method': random.choice(payment_methods),
            'order_status': random.choice(statuses)
        }
        sales.append(sale)
    
    return pd.DataFrame(sales)

if __name__ == "__main__":
    print("Generating sales transactions only...")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Only regenerate sales with correct IDs
    sales_df = generate_sales(10000, 1000, 200, 100)
    sales_df.to_csv('data/sales.csv', index=False)
    print(f"Generated {len(sales_df)} sales transactions")
    print("Sales data regenerated successfully!")