# Star Schema

## Core Dimensions
- dim_date: 1 row = 1 calendar date
- dim_customer: 1 row = 1 customer
- dim_product: 1 row = 1 product

## Core Facts
- fct_orders: 1 row = 1 order
- fct_order_item: 1 row = 1 order item

## Join Logic
- fct_orders.customer_sk -> dim_customer.customer_sk
- fct_orders.date_sk -> dim_date.date_sk
- fct_order_item.product_sk -> dim_product.product_sk
- fct_order_item.order_sk -> fct_orders.order_sk