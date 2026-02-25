-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS sales_fact CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS date_dim CASCADE;
DROP TABLE IF EXISTS location CASCADE;

-- Dimension Table: Customers
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(50),
    registration_date DATE,
    customer_segment VARCHAR(50)
);

-- Dimension Table: Products
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price DECIMAL(10, 2),
    supplier VARCHAR(100)
);

-- Dimension Table: Location
CREATE TABLE location (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(100)
);

-- Dimension Table: Date
CREATE TABLE date_dim (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INT,
    month INT,
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

-- Fact Table: Sales
CREATE TABLE sales_fact (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    location_id INT REFERENCES location(location_id),
    date_id INT REFERENCES date_dim(date_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2),
    discount_percent DECIMAL(5, 2),
    tax_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    order_status VARCHAR(50)
);

-- Create indexes for better query performance
CREATE INDEX idx_sales_customer ON sales_fact(customer_id);
CREATE INDEX idx_sales_product ON sales_fact(product_id);
CREATE INDEX idx_sales_date ON sales_fact(date_id);
CREATE INDEX idx_sales_location ON sales_fact(location_id);