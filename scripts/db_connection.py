import psycopg2
from psycopg2 import Error

def test_connection():
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host="localhost",
            database="ecommerce_sales",
            user="postgres",
            password="raks123"  # Replace with your actual password
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print("Successfully connected to PostgreSQL!")
        print("PostgreSQL version:", db_version)
        
        cursor.close()
        connection.close()
        print("Connection closed.")
        
    except Error as e:
        print("Error connecting to PostgreSQL:", e)

if __name__ == "__main__":
    test_connection()