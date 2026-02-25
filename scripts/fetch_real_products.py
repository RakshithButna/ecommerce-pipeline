import requests
import psycopg2
import json

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ecommerce_sales',
    'user': 'postgres',
    'password': 'raks123'  # Replace with your password
}

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

def fetch_products_from_api():
    """Fetch products from Fake Store API"""
    print("Fetching products from Fake Store API...")
    
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    
    if response.status_code == 200:
        products = response.json()
        print(f"✅ Fetched {len(products)} products from API")
        return products
    else:
        print(f"❌ Error fetching products: {response.status_code}")
        return []

def clear_existing_products():
    """Clear existing products (optional - for clean slate)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Delete sales first (foreign key constraint)
    cursor.execute("DELETE FROM sales_fact")
    cursor.execute("DELETE FROM products")
    cursor.execute("ALTER SEQUENCE products_product_id_seq RESTART WITH 1")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Cleared existing products")

def map_category_to_subcategory(category):
    """Map API categories to our schema"""
    category_map = {
        "electronics": ("Electronics", "Accessories"),
        "jewelery": ("Clothing", "Accessories"),
        "men's clothing": ("Clothing", "Men"),
        "women's clothing": ("Clothing", "Women")
    }
    return category_map.get(category, ("Other", "General"))

def insert_products_to_db(products):
    """Insert products into database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted_count = 0
    
    for product in products:
        # Map category
        category, subcategory = map_category_to_subcategory(product['category'])
        
        cursor.execute("""
            INSERT INTO products (product_name, category, subcategory, unit_price, supplier)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            product['title'][:200],  # Limit to 200 chars
            category,
            subcategory,
            product['price'],
            'Fake Store Supplier'
        ))
        inserted_count += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Inserted {inserted_count} products into database")

def main():
    """Main function to fetch and load real products"""
    print("="*50)
    print("Real Product Data Loader")
    print("="*50)
    
    # Fetch products from API
    products = fetch_products_from_api()
    
    if not products:
        print("❌ No products fetched. Exiting.")
        return
    
    # Ask user if they want to clear existing data
    print("\n⚠️  This will replace your current products.")
    choice = input("Continue? (yes/no): ").lower()
    
    if choice == 'yes':
        clear_existing_products()
        insert_products_to_db(products)
        print("\n✅ Real products loaded successfully!")
        print(f"Total products in database: {len(products)}")
    else:
        print("❌ Operation cancelled")

if __name__ == "__main__":
    main()