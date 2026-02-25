"""
E-Commerce Sales Analytics Pipeline DAG
========================================
Automates:
  - Fetch products from API
  - Generate and load sales
  - Run data quality checks
  - Log summary
"""

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2
import requests
import logging
import random

# ──────────────────────────────────────────────────────────────────────────────
# Default Arguments
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    'owner': 'rakshith',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ──────────────────────────────────────────────────────────────────────────────
# Database Connection
# ──────────────────────────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="ecommerce_sales",
        user="postgres",
        password="raks123"
    )

# ══════════════════════════════════════════════════════════════════════════════
# TASK 1 — FETCH PRODUCTS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_products_from_api(**context):
    logging.info("Fetching products from Fake Store API...")

    response = requests.get("https://fakestoreapi.com/products", timeout=30)
    response.raise_for_status()
    api_products = response.json()

    conn = get_db_connection()
    cursor = conn.cursor()

    inserted = 0
    updated = 0

    for product in api_products:
        category_map = {
            "electronics": "Electronics",
            "jewelery": "Jewelry",
            "men's clothing": "Clothing",
            "women's clothing": "Clothing"
        }

        category = category_map.get(product.get('category', ''), 'Other')

        cursor.execute("""
            INSERT INTO products (product_name, category, unit_price, product_description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_name) DO UPDATE
            SET unit_price = EXCLUDED.unit_price,
                category = EXCLUDED.category
            RETURNING (xmax = 0) AS inserted
        """, (
            product['title'][:100],
            category,
            round(product['price'], 2),
            product.get('description', '')[:500]
        ))

        result = cursor.fetchone()
        if result and result[0]:
            inserted += 1
        else:
            updated += 1

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Products sync complete: {inserted} inserted, {updated} updated")
    return {'inserted': inserted, 'updated': updated}

# ══════════════════════════════════════════════════════════════════════════════
# TASK 2 — ETL (USING date_id CORRECTLY)
# ══════════════════════════════════════════════════════════════════════════════

def run_etl_pipeline(**context):
    logging.info("Starting ETL pipeline run...")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get today's date_id from date_dim
    today = date.today()

    cursor.execute("""
        SELECT date_id FROM date_dim
        WHERE full_date = %s
    """, (today,))
    result = cursor.fetchone()

    if not result:
        logging.warning("Today's date not found in date_dim")
        return {'transactions_loaded': 0}

    date_id = result[0]

    # Get dimension data
    cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 100")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT product_id, unit_price FROM products ORDER BY RANDOM() LIMIT 50")
    products = cursor.fetchall()

    cursor.execute("SELECT location_id FROM location ORDER BY RANDOM() LIMIT 20")
    location_ids = [row[0] for row in cursor.fetchall()]

    if not customer_ids or not products or not location_ids:
        logging.warning("Not enough dimension data found")
        return {'transactions_loaded': 0}

    num_transactions = random.randint(50, 200)
    transactions = []

    for _ in range(num_transactions):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        unit_price = float(product[1])
        discount = round(random.uniform(0, 0.25), 2)
        total_amount = round(quantity * unit_price * (1 - discount), 2)

        transactions.append((
            random.choice(customer_ids),
            product[0],
            random.choice(location_ids),
            date_id,
            quantity,
            unit_price,
            discount,
            0.00,  # tax_amount
            total_amount,
            random.choice(['completed', 'pending', 'refunded'])
        ))

    cursor.executemany("""
        INSERT INTO sales_fact
            (customer_id, product_id, location_id, date_id,
             quantity, unit_price, discount_percent,
             tax_amount, total_amount, order_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, transactions)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"ETL complete: {num_transactions} transactions loaded")
    return {'transactions_loaded': num_transactions}

# ══════════════════════════════════════════════════════════════════════════════
# TASK 3 — DATA QUALITY CHECKS (FIXED)
# ══════════════════════════════════════════════════════════════════════════════

def run_data_quality_checks(**context):
    logging.info("Running data quality checks...")

    conn = get_db_connection()
    cursor = conn.cursor()
    failures = []

    # Check 1: No null customers
    cursor.execute("SELECT COUNT(*) FROM sales_fact WHERE customer_id IS NULL")
    if cursor.fetchone()[0] > 0:
        failures.append("NULL customer_id detected")

    # Check 2: No negative totals
    cursor.execute("SELECT COUNT(*) FROM sales_fact WHERE total_amount < 0")
    if cursor.fetchone()[0] > 0:
        failures.append("Negative total_amount detected")

    # Check 3: Orphan sales
    cursor.execute("""
        SELECT COUNT(*)
        FROM sales_fact sf
        LEFT JOIN customers c ON sf.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    if cursor.fetchone()[0] > 0:
        failures.append("Orphan sales detected")

    # Check 4: Today's sales exist (JOIN date_dim)
    cursor.execute("""
        SELECT COUNT(*)
        FROM sales_fact sf
        JOIN date_dim d ON sf.date_id = d.date_id
        WHERE d.full_date = CURRENT_DATE
    """)
    if cursor.fetchone()[0] == 0:
        failures.append("No sales found for today")

    cursor.close()
    conn.close()

    if failures:
        raise ValueError("Data quality checks failed:\n" + "\n".join(failures))

    logging.info("All data quality checks passed!")
    return {'status': 'all_checks_passed'}

# ══════════════════════════════════════════════════════════════════════════════
# TASK 4 — SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def log_pipeline_summary(**context):
    ti = context['task_instance']
    etl_result = ti.xcom_pull(task_ids='run_etl_pipeline') or {}
    product_result = ti.xcom_pull(task_ids='fetch_products') or {}

    logging.info(f"""
    ═══════════════════════════════════════
    Pipeline Summary
    Products Inserted: {product_result.get('inserted', 0)}
    Products Updated : {product_result.get('updated', 0)}
    Transactions     : {etl_result.get('transactions_loaded', 0)}
    ═══════════════════════════════════════
    """)

# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id='ecommerce_sales_pipeline',
    default_args=default_args,
    description='E-Commerce ETL Pipeline',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'etl'],
) as dag:

    fetch_products = PythonOperator(
        task_id='fetch_products',
        python_callable=fetch_products_from_api
    )

    etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl_pipeline
    )

    quality_checks = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_data_quality_checks
    )

    summary = PythonOperator(
        task_id='log_pipeline_summary',
        python_callable=log_pipeline_summary
    )

    fetch_products >> etl_task >> quality_checks >> summary
