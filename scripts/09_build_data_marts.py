from pathlib import Path
import pandas as pd

from src.marts_io import read_parquet_dataset, write_parquet_dataset
from src.data_marts import (
    build_mart_sales_daily,
    build_mart_product_performance,
    build_mart_customer_360,
)
from src.quality_checks import assert_not_empty


def main():
    base = Path("data/marts/core")
    out = Path("data/marts/analytics")

    dim_date = read_parquet_dataset(base / "dim_date")
    dim_customer = read_parquet_dataset(base / "dim_customer")
    dim_product = read_parquet_dataset(base / "dim_product")
    fct_orders = read_parquet_dataset(base / "fct_orders")
    fct_order_item = read_parquet_dataset(base / "fct_order_item")

    mart_sales_daily = build_mart_sales_daily(fct_orders, fct_order_item, dim_date)
    mart_product_performance = build_mart_product_performance(fct_order_item, dim_product)
    mart_customer_360 = build_mart_customer_360(fct_orders, fct_order_item, dim_customer)

    assert_not_empty(mart_sales_daily, "mart_sales_daily")
    assert_not_empty(mart_product_performance, "mart_product_performance")
    assert_not_empty(mart_customer_360, "mart_customer_360")

    write_parquet_dataset(mart_sales_daily, out / "mart_sales_daily", "mart_sales_daily.parquet")
    write_parquet_dataset(mart_product_performance, out / "mart_product_performance", "mart_product_performance.parquet")
    write_parquet_dataset(mart_customer_360, out / "mart_customer_360", "mart_customer_360.parquet")

    print("Block 4 / Step 9 erfolgreich.")
    print("Data marts written to data/marts/analytics/")


if __name__ == "__main__":
    main()