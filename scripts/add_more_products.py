import psycopg2
from faker import Faker
import random

fake = Faker()

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ecommerce_sales',
    'user': 'postgres',
    'password': 'raks123'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def generate_additional_products(num_products=180):
    """Generate additional realistic products"""
    
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories', 'Audio', 'Cameras'],
        'Clothing': ['Men', 'Women', 'Kids', 'Accessories', 'Shoes', 'Sportswear'],
        'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Tools', 'Bedding', 'Lighting'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics', 'Magazines'],
        'Sports': ['Equipment', 'Apparel', 'Accessories', 'Nutrition', 'Outdoor'],
        'Beauty': ['Skincare', 'Makeup', 'Haircare', 'Fragrance', 'Tools'],
        'Toys & Games': ['Action Figures', 'Board Games', 'Educational', 'Outdoor', 'Puzzles'],
        'Food & Beverage': ['Snacks', 'Drinks', 'Organic', 'Gourmet', 'Health']
    }
    
    products = []
    
    for i in range(num_products):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        product = {
            'product_name': fake.catch_phrase() + ' ' + subcategory,
            'category': category,
            'subcategory': subcategory,
            'unit_price': round(random.uniform(10, 500), 2),
            'supplier': fake.company()
        }
        products.append(product)
    
    return products

def insert_products(products):
    """Insert products into database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for product in products:
        cursor.execute("""
            INSERT INTO products (product_name, category, subcategory, unit_price, supplier)
            VALUES (%s, %s, %s, %s, %s)
        """, (product['product_name'], product['category'], product['subcategory'],
              product['unit_price'], product['supplier']))
    
    conn.commit()
    cursor.close()
    conn.close()

def main():
    print("="*50)
    print("Adding 180 More Products")
    print("="*50)
    
    # Check current count
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    current_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    print(f"Current products in database: {current_count}")
    
    # Generate and insert
    print("Generating 180 additional products...")
    products = generate_additional_products(180)
    
    print("Inserting into database...")
    insert_products(products)
    
    # Verify
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    new_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    print(f"✅ Complete! Total products now: {new_count}")

if __name__ == "__main__":
    main()