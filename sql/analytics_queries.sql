-- E-commerce Sales Analytics Queries
-- These queries demonstrate the analytical capabilities of the data warehouse

-- 1. Total Sales by Month
SELECT 
    d.year,
    d.month,
    COUNT(*) as total_orders,
    SUM(s.quantity) as total_items_sold,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue
FROM sales_fact s
JOIN date_dim d ON s.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2. Top 10 Customers by Revenue
SELECT 
    c.customer_name,
    c.customer_segment,
    COUNT(*) as total_orders,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_spent
FROM sales_fact s
JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_segment
ORDER BY total_spent DESC
LIMIT 10;

-- 3. Sales by Product Category
SELECT 
    p.category,
    COUNT(*) as total_orders,
    SUM(s.quantity) as items_sold,
    ROUND(AVG(s.total_amount)::numeric, 2) as avg_order_value,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue
FROM sales_fact s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;

-- 4. Sales by Region
SELECT 
    l.region,
    COUNT(*) as total_orders,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(s.total_amount)::numeric, 2) as avg_order_value
FROM sales_fact s
JOIN location l ON s.location_id = l.location_id
GROUP BY l.region
ORDER BY total_revenue DESC;

-- 5. Weekend vs Weekday Sales
SELECT 
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(*) as total_orders,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(s.total_amount)::numeric, 2) as avg_order_value
FROM sales_fact s
JOIN date_dim d ON s.date_id = d.date_id
GROUP BY d.is_weekend
ORDER BY day_type;

-- 6. Top 5 Products by Revenue
SELECT 
    p.product_name,
    p.category,
    COUNT(*) as times_sold,
    SUM(s.quantity) as total_quantity,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue
FROM sales_fact s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 5;

-- 7. Customer Segment Analysis
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(*) as total_orders,
    ROUND(AVG(s.total_amount)::numeric, 2) as avg_order_value,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue
FROM sales_fact s
JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- 8. Payment Method Distribution
SELECT 
    s.payment_method,
    COUNT(*) as order_count,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(s.total_amount)::numeric, 2) as avg_transaction_value
FROM sales_fact s
GROUP BY s.payment_method
ORDER BY total_revenue DESC;

-- 9. Order Status Breakdown
SELECT 
    s.order_status,
    COUNT(*) as order_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) as percentage,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_value
FROM sales_fact s
GROUP BY s.order_status
ORDER BY order_count DESC;

-- 10. Monthly Sales Trend (Last 6 Months)
SELECT 
    TO_CHAR(d.full_date, 'YYYY-MM') as month,
    COUNT(*) as orders,
    ROUND(SUM(s.total_amount)::numeric, 2) as revenue
FROM sales_fact s
JOIN date_dim d ON s.date_id = d.date_id
WHERE d.full_date >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY TO_CHAR(d.full_date, 'YYYY-MM')
ORDER BY month;