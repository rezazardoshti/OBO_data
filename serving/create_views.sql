CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.mart_sales_daily AS
SELECT *
FROM read_parquet('data/marts/analytics/mart_sales_daily/*.parquet');

CREATE OR REPLACE VIEW mart.mart_product_performance AS
SELECT *
FROM read_parquet('data/marts/analytics/mart_product_performance/*.parquet');

CREATE OR REPLACE VIEW mart.mart_customer_360 AS
SELECT *
FROM read_parquet('data/marts/analytics/mart_customer_360/*.parquet');

CREATE OR REPLACE VIEW mart.vw_sales_dashboard AS
SELECT
    full_date,
    year,
    month,
    quarter,
    orders_count,
    order_items_count,
    units_sold,
    gross_revenue,
    average_order_value,
    unique_customers
FROM mart.mart_sales_daily;

CREATE OR REPLACE VIEW mart.vw_product_dashboard AS
SELECT
    product_id,
    orders_count,
    units_sold,
    gross_revenue,
    unique_buyers,
    avg_selling_price
FROM mart.mart_product_performance;

CREATE OR REPLACE VIEW mart.vw_customer_dashboard AS
SELECT
    user_id,
    city,
    country,
    is_premium,
    marketing_opt_in,
    total_orders,
    total_revenue,
    average_order_value,
    customer_segment,
    days_since_last_order
FROM mart.mart_customer_360;