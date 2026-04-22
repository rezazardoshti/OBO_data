from pathlib import Path
from src.marts_io import read_parquet_dataset
from src.star_schema import (
    build_dim_date_from_orders,
    build_dim_customer,
    build_dim_product,
    build_fct_orders,
    build_fct_order_item,
    save_core_tables,
)
from src.quality_checks import assert_not_empty, assert_unique


def main():
    stage_base = Path("data/stage")
    output_base = Path("data/marts")

    orders = read_parquet_dataset(stage_base / "source=erp" / "entity=orders")
    order_items = read_parquet_dataset(stage_base / "source=erp" / "entity=order_items")
    products = read_parquet_dataset(stage_base / "source=erp" / "entity=products")
    users = read_parquet_dataset(stage_base / "source=crm" / "entity=users")

    dim_date = build_dim_date_from_orders(orders)
    dim_customer = build_dim_customer(users)
    dim_product = build_dim_product(products)
    fct_orders = build_fct_orders(orders, dim_customer, dim_date)
    fct_order_item = build_fct_order_item(order_items, fct_orders, dim_product)

    assert_not_empty(dim_date, "dim_date")
    assert_not_empty(dim_customer, "dim_customer")
    assert_not_empty(dim_product, "dim_product")
    assert_not_empty(fct_orders, "fct_orders")
    assert_not_empty(fct_order_item, "fct_order_item")

    assert_unique(dim_date, "date_sk", "dim_date")
    assert_unique(dim_customer, "customer_sk", "dim_customer")
    assert_unique(dim_product, "product_sk", "dim_product")
    assert_unique(fct_orders, "order_sk", "fct_orders")
    assert_unique(fct_order_item, "order_item_sk", "fct_order_item")

    save_core_tables(output_base, dim_date, dim_customer, dim_product, fct_orders, fct_order_item)

    print("Block 4 / Step 8 erfolgreich.")
    print("Star schema tables written to data/marts/core/")


if __name__ == "__main__":
    main()