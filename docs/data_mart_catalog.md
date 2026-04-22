# Data Mart Catalog

## mart_sales_daily
Grain: 1 row = 1 date
KPIs:
- orders_count
- order_items_count
- units_sold
- gross_revenue
- average_order_value
- unique_customers

## mart_product_performance
Grain: 1 row = 1 product
KPIs:
- orders_count
- units_sold
- gross_revenue
- unique_buyers
- avg_selling_price

## mart_customer_360
Grain: 1 row = 1 customer
KPIs:
- first_order_ts
- last_order_ts
- total_orders
- total_revenue
- average_order_value
- customer_segment