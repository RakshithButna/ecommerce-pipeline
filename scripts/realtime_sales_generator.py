import psycopg2
import random
import time
from datetime import datetime, date

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ecommerce_sales',
    'user': 'postgres',
    'password': 'raks123'
}

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

def get_random_ids():
    """Get random customer, product, location IDs from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get random customer
    cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
    customer_id = cursor.fetchone()[0]
    
    # Get random product
    cursor.execute("SELECT product_id, unit_price FROM products ORDER BY RANDOM() LIMIT 1")
    product_id, product_price = cursor.fetchone()
    
    # Get random location
    cursor.execute("SELECT location_id FROM location ORDER BY RANDOM() LIMIT 1")
    location_id = cursor.fetchone()[0]
    
    # Get today's date_id
    today = date.today()
    cursor.execute("SELECT date_id FROM date_dim WHERE full_date = %s", (today,))
    result = cursor.fetchone()
    
    if result is None:
        # If today's date doesn't exist, create it
        day_of_week = today.strftime('%A')
        is_weekend = today.weekday() >= 5
        cursor.execute("""
            INSERT INTO date_dim (full_date, day_of_week, day_of_month, month, quarter, year, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING date_id
        """, (today, day_of_week, today.day, today.month, (today.month - 1) // 3 + 1, today.year, is_weekend))
        date_id = cursor.fetchone()[0]
        conn.commit()
    else:
        date_id = result[0]
    
    cursor.close()
    conn.close()
    
    return customer_id, product_id, location_id, date_id, product_price

def generate_sale():
    """Generate and insert a single sale transaction"""
    customer_id, product_id, location_id, date_id, base_price = get_random_ids()
    
    # Generate sale details - convert everything to float
    quantity = random.randint(1, 5)
    unit_price = float(base_price)  # Convert Decimal to float
    discount = round(random.uniform(0, 15), 2)
    subtotal = quantity * unit_price
    discount_amount = subtotal * (discount / 100)
    tax = (subtotal - discount_amount) * 0.08
    total = subtotal - discount_amount + tax
    
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer']
    statuses = ['Completed', 'Pending', 'Cancelled', 'Refunded']
    
    # Insert into database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO sales_fact (customer_id, product_id, location_id, date_id, 
                               quantity, unit_price, discount_percent, tax_amount, 
                               total_amount, payment_method, order_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING sale_id
    """, (customer_id, product_id, location_id, date_id,
          quantity, unit_price, discount, round(tax, 2), 
          round(total, 2), random.choice(payment_methods), 
          random.choice(statuses)))
    
    sale_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    
    return sale_id, round(total, 2)

def run_realtime_generator(interval=3):
    """Continuously generate sales at specified interval (seconds)"""
    print("🚀 Real-Time Sales Generator Started!")
    print(f"⏱️  Generating new sales every {interval} seconds...")
    print("📊 Open dashboard at: http://localhost:8501")
    print("Press Ctrl+C to stop\n")
    
    sale_count = 0
    
    try:
        while True:
            sale_id, total_amount = generate_sale()
            sale_count += 1
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"✅ [{timestamp}] Sale #{sale_id} | ${total_amount:,.2f} | Total: {sale_count}")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Generator stopped. Total sales generated: {sale_count}")

if __name__ == "__main__":
    # Generate a new sale every 3 seconds
    run_realtime_generator(interval=3)