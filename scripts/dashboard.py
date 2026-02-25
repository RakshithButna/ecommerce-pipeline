import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
import time
from dotenv import load_dotenv
import os

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="E-commerce Sales Analytics",
    page_icon="📊",
    layout="wide"
)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# Load data WITHOUT caching for real-time updates
def load_data(query):
    """Load data from database - NO CACHE for real-time"""
    conn = get_db_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Title with last updated time
col1, col2, col3 = st.columns([3, 1, 1])

with col1:
    st.title("📊 E-commerce Sales Analytics Dashboard")

with col2:
    live_mode = st.toggle("🔴 Live Mode", value=True)

with col3:
    manual_refresh = st.button("🔄 Refresh Now")

# Refresh Logic
if live_mode:
    st_autorefresh(interval=5000, key="live_refresh")

if manual_refresh:
    st.rerun()

st.markdown(f"**Last Updated: {datetime.now().strftime('%H:%M:%S')}**")

st.markdown("---")

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
date_query = "SELECT MIN(full_date) as min_date, MAX(full_date) as max_date FROM date_dim"
date_range = load_data(date_query)
min_date = pd.to_datetime(date_range['min_date'].iloc[0])
max_date = pd.to_datetime(date_range['max_date'].iloc[0])

# Default to last 30 days or all data if less
default_start = max_date - timedelta(days=30) if (max_date - min_date).days > 30 else min_date
start_date = st.sidebar.date_input("Start Date", default_start, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

# Region filter
region_query = "SELECT DISTINCT region FROM location ORDER BY region"
regions = load_data(region_query)
selected_regions = st.sidebar.multiselect(
    "Select Regions",
    options=regions['region'].tolist(),
    default=regions['region'].tolist()
)

# Build region filter
if selected_regions:
    region_list = ",".join("'" + r + "'" for r in selected_regions)
    region_filter = f"AND l.region IN ({region_list})"
else:
    region_filter = ""

# KPI Metrics
st.header("Key Performance Indicators")

kpi_query = f"""
SELECT 
    COUNT(DISTINCT s.sale_id) as total_orders,
    COUNT(DISTINCT s.customer_id) as total_customers,
    COALESCE(ROUND(SUM(s.total_amount)::numeric, 2), 0) as total_revenue,
    COALESCE(ROUND(AVG(s.total_amount)::numeric, 2), 0) as avg_order_value
FROM sales_fact s
JOIN date_dim d ON s.date_id = d.date_id
JOIN location l ON s.location_id = l.location_id
WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
{region_filter}
"""

kpi_data = load_data(kpi_query)

col1, col2, col3, col4 = st.columns(4)

with col1:
    revenue = kpi_data['total_revenue'].iloc[0] if kpi_data['total_revenue'].iloc[0] is not None else 0
    st.metric(
        label="💰 Total Revenue",
        value=f"${revenue:,.2f}"
    )

with col2:
    orders = kpi_data['total_orders'].iloc[0] if kpi_data['total_orders'].iloc[0] is not None else 0
    st.metric(
        label="🛒 Total Orders",
        value=f"{orders:,}"
    )

with col3:
    customers = kpi_data['total_customers'].iloc[0] if kpi_data['total_customers'].iloc[0] is not None else 0
    st.metric(
        label="👥 Total Customers",
        value=f"{customers:,}"
    )

with col4:
    avg_value = kpi_data['avg_order_value'].iloc[0] if kpi_data['avg_order_value'].iloc[0] is not None else 0
    st.metric(
        label="📈 Avg Order Value",
        value=f"${avg_value:,.2f}"
    )

st.markdown("---")

# Recent Sales Feed
st.header("🔴 Live Sales Feed (Last 10 Transactions)")

recent_sales_query = f"""
SELECT 
    s.sale_id,
    c.customer_name,
    p.product_name,
    s.quantity,
    ROUND(s.total_amount::numeric, 2) as amount,
    s.payment_method,
    s.order_status
FROM sales_fact s
JOIN customers c ON s.customer_id = c.customer_id
JOIN products p ON s.product_id = p.product_id
JOIN date_dim d ON s.date_id = d.date_id
JOIN location l ON s.location_id = l.location_id
WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
{region_filter}
ORDER BY s.sale_id DESC
LIMIT 10
"""

recent_sales = load_data(recent_sales_query)

if len(recent_sales) > 0:
    st.dataframe(
        recent_sales,
        use_container_width=True,
        hide_index=True,
        column_config={
            "sale_id": "Order ID",
            "customer_name": "Customer",
            "product_name": "Product",
            "quantity": st.column_config.NumberColumn("Qty", format="%d"),
            "amount": st.column_config.NumberColumn("Amount", format="$%.2f"),
            "payment_method": "Payment",
            "order_status": "Status"
        }
    )
else:
    st.info("No sales data available for the selected date range. Start the sales generator!")

st.markdown("---")

# Sales Trend Chart
st.header("📈 Sales Trend Over Time")

trend_query = f"""
SELECT 
    d.full_date,
    COUNT(*) as orders,
    ROUND(SUM(s.total_amount)::numeric, 2) as revenue
FROM sales_fact s
JOIN date_dim d ON s.date_id = d.date_id
JOIN location l ON s.location_id = l.location_id
WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
{region_filter}
GROUP BY d.full_date
ORDER BY d.full_date
"""

trend_data = load_data(trend_query)

if len(trend_data) > 0:
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=trend_data['full_date'],
        y=trend_data['revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color='#1f77b4', width=2),
        fill='tozeroy'
    ))

    fig_trend.update_layout(
        title="Daily Revenue Trend",
        xaxis_title="Date",
        yaxis_title="Revenue ($)",
        hovermode='x unified',
        height=400,
        transition_duration=500
    )

    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("No trend data available for the selected date range.")

# Category and Region Analysis
col1, col2 = st.columns(2)

with col1:
    st.subheader("📦 Revenue by Category")
    
    category_query = f"""
    SELECT 
        p.category,
        ROUND(SUM(s.total_amount)::numeric, 2) as revenue,
        COUNT(*) as orders
    FROM sales_fact s
    JOIN products p ON s.product_id = p.product_id
    JOIN date_dim d ON s.date_id = d.date_id
    JOIN location l ON s.location_id = l.location_id
    WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
    {region_filter}
    GROUP BY p.category
    ORDER BY revenue DESC
    """
    
    category_data = load_data(category_query)
    
    if len(category_data) > 0:
        fig_category = px.bar(
            category_data,
            x='category',
            y='revenue',
            color='revenue',
            color_continuous_scale='Blues',
            labels={'revenue': 'Revenue ($)', 'category': 'Category'}
        )
        fig_category.update_layout(height=400, showlegend=False, transition_duration=500)
        st.plotly_chart(fig_category, use_container_width=True)
    else:
        st.info("No category data available.")

with col2:
    st.subheader("🗺️ Revenue by Region")
    
    region_query_chart = f"""
    SELECT 
        l.region,
        ROUND(SUM(s.total_amount)::numeric, 2) as revenue,
        COUNT(*) as orders
    FROM sales_fact s
    JOIN location l ON s.location_id = l.location_id
    JOIN date_dim d ON s.date_id = d.date_id
    WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
    {region_filter}
    GROUP BY l.region
    ORDER BY revenue DESC
    """
    
    region_data = load_data(region_query_chart)
    
    if len(region_data) > 0:
        fig_region = px.pie(
            region_data,
            values='revenue',
            names='region',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        fig_region.update_layout(height=400, transition_duration=500)
        st.plotly_chart(fig_region, use_container_width=True)
    else:
        st.info("No region data available.")

st.markdown("---")

# Top Products
st.header("🏆 Top 10 Products by Revenue")

products_query = f"""
SELECT 
    p.product_name,
    p.category,
    COUNT(*) as times_sold,
    SUM(s.quantity) as total_quantity,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue
FROM sales_fact s
JOIN products p ON s.product_id = p.product_id
JOIN date_dim d ON s.date_id = d.date_id
JOIN location l ON s.location_id = l.location_id
WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
{region_filter}
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10
"""

products_data = load_data(products_query)

if len(products_data) > 0:
    st.dataframe(
        products_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "product_name": "Product Name",
            "category": "Category",
            "times_sold": st.column_config.NumberColumn("Times Sold", format="%d"),
            "total_quantity": st.column_config.NumberColumn("Total Quantity", format="%d"),
            "total_revenue": st.column_config.NumberColumn("Total Revenue", format="$%.2f")
        }
    )
else:
    st.info("No product data available.")

st.markdown("---")

# Customer Segments
st.header("👥 Customer Segment Analysis")

segment_query = f"""
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(*) as total_orders,
    ROUND(AVG(s.total_amount)::numeric, 2) as avg_order_value,
    ROUND(SUM(s.total_amount)::numeric, 2) as total_revenue
FROM sales_fact s
JOIN customers c ON s.customer_id = c.customer_id
JOIN date_dim d ON s.date_id = d.date_id
JOIN location l ON s.location_id = l.location_id
WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
{region_filter}
GROUP BY c.customer_segment
ORDER BY total_revenue DESC
"""

segment_data = load_data(segment_query)

if len(segment_data) > 0:
    fig_segment = go.Figure(data=[
        go.Bar(name='Total Revenue', x=segment_data['customer_segment'], y=segment_data['total_revenue'], yaxis='y', offsetgroup=1),
        go.Bar(name='Total Orders', x=segment_data['customer_segment'], y=segment_data['total_orders'], yaxis='y2', offsetgroup=2)
    ])

    fig_segment.update_layout(
        title='Revenue and Orders by Customer Segment',
        xaxis=dict(title='Customer Segment'),
        yaxis=dict(title='Revenue ($)', side='left'),
        yaxis2=dict(title='Number of Orders', overlaying='y', side='right'),
        barmode='group',
        height=400,
        transition_duration=500
    )

    st.plotly_chart(fig_segment, use_container_width=True)
else:
    st.info("No segment data available.")

# Footer
st.markdown("---")
st.caption(
    f"{'🔴 LIVE Dashboard | Auto-refreshes every 5 seconds' if live_mode else '⚪ Live Mode OFF | Manual refresh only'} | Built with Streamlit & PostgreSQL"
)