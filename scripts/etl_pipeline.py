def run_etl_pipeline(**context):
    """Generate new sales transactions and load them into the database."""
    import random
    from datetime import date

    logging.info("Starting ETL pipeline run...")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get dimension data
    cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 100")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT product_id, unit_price FROM products ORDER BY RANDOM() LIMIT 50")
    products = cursor.fetchall()

    cursor.execute("SELECT location_id FROM location ORDER BY RANDOM() LIMIT 20")
    location_ids = [row[0] for row in cursor.fetchall()]

    # Get today's date_id from date_dim
    cursor.execute("""
        SELECT date_id FROM date_dim
        WHERE full_date = CURRENT_DATE
    """)
    result = cursor.fetchone()

    if not result:
        raise ValueError("Today's date not found in date_dim table")

    date_id = result[0]

    if not customer_ids or not products or not location_ids:
        logging.warning("Not enough dimension data found, skipping ETL run")
        return {'transactions_loaded': 0}

    # Generate transactions
    num_transactions = random.randint(50, 200)
    transactions = []

    for _ in range(num_transactions):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        unit_price = float(product[1])
        discount_percent = round(random.uniform(0, 0.25), 2)
        tax_amount = round(quantity * unit_price * 0.08, 2)  # 8% tax
        total_amount = round(quantity * unit_price * (1 - discount_percent) + tax_amount, 2)

        transactions.append((
            random.choice(customer_ids),
            product[0],
            random.choice(location_ids),
            date_id,
            quantity,
            unit_price,
            discount_percent,
            tax_amount,
            total_amount,
            random.choice(['completed', 'completed', 'completed', 'pending', 'refunded'])
        ))

    cursor.executemany("""
        INSERT INTO sales_fact
            (customer_id, product_id, location_id, date_id,
             quantity, unit_price, discount_percent, tax_amount,
             total_amount, order_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, transactions)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"ETL complete: {num_transactions} transactions loaded")
    return {'transactions_loaded': num_transactions}
