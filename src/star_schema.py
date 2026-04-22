from pathlib import Path
import pandas as pd

from src.marts_io import read_parquet_dataset, write_parquet_dataset
from src.surrogate_keys import build_sk_series


def build_dim_date_from_orders(orders: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(orders["order_ts"]).dt.normalize().drop_duplicates().sort_values()
    dim_date = pd.DataFrame({"full_date": dates})
    dim_date["date_sk"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    dim_date["weekday"] = dim_date["full_date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["full_date"].dt.weekday >= 5
    return dim_date[
        ["date_sk", "full_date", "year", "quarter", "month", "day", "weekday", "is_weekend"]
    ]


def build_dim_customer(users: pd.DataFrame) -> pd.DataFrame:
    df = users.copy()
    df["signup_ts"] = pd.to_datetime(df["signup_ts"], errors="coerce")
    df["customer_sk"] = build_sk_series(df, ["user_id"])
    df["signup_date"] = df["signup_ts"].dt.date
    keep_cols = [
        "customer_sk", "user_id", "signup_date", "city", "country", "age",
        "gender", "is_premium", "marketing_opt_in"
    ]
    return df[keep_cols].drop_duplicates(subset=["user_id"])


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    df = products.copy()
    df["product_sk"] = build_sk_series(df, ["product_id"])
    ordered_cols = ["product_sk"] + [c for c in df.columns if c != "product_sk"]
    return df[ordered_cols].drop_duplicates(subset=["product_id"])


def build_fct_orders(orders: pd.DataFrame, dim_customer: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    df = orders.copy()
    df["order_ts"] = pd.to_datetime(df["order_ts"], errors="coerce")
    df["order_date"] = df["order_ts"].dt.normalize()

    customer_map = dim_customer[["user_id", "customer_sk"]].drop_duplicates()
    date_map = dim_date[["full_date", "date_sk"]].rename(columns={"full_date": "order_date"})

    df = df.merge(customer_map, on="user_id", how="left")
    df = df.merge(date_map, on="order_date", how="left")

    df["order_sk"] = build_sk_series(df, ["order_id"])
    if "total_amount" in df.columns:
        df["gross_revenue"] = df["total_amount"]
    elif "order_amount" in df.columns:
        df["gross_revenue"] = df["order_amount"]
    else:
        df["gross_revenue"] = None

    keep_cols = [
        "order_sk", "order_id", "user_id", "customer_sk", "session_id",
        "order_ts", "date_sk", "gross_revenue"
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df[keep_cols].drop_duplicates(subset=["order_id"])


def build_fct_order_item(
    order_items: pd.DataFrame,
    fct_orders: pd.DataFrame,
    dim_product: pd.DataFrame
) -> pd.DataFrame:
    df = order_items.copy()

    order_map = fct_orders[["order_id", "order_sk", "customer_sk", "date_sk"]].drop_duplicates()
    product_map = dim_product[["product_id", "product_sk"]].drop_duplicates()

    df = df.merge(order_map, on="order_id", how="left")
    df = df.merge(product_map, on="product_id", how="left")

    if "quantity" not in df.columns:
        df["quantity"] = 1

    if "unit_price" in df.columns:
        df["gross_revenue"] = df["quantity"] * df["unit_price"]
    elif "price" in df.columns:
        df["gross_revenue"] = df["quantity"] * df["price"]
        df["unit_price"] = df["price"]
    else:
        df["gross_revenue"] = None
        df["unit_price"] = None

    df["order_item_sk"] = build_sk_series(df, ["order_item_id"])

    keep_cols = [
        "order_item_sk", "order_item_id", "order_id", "order_sk",
        "customer_sk", "product_id", "product_sk", "date_sk",
        "quantity", "unit_price", "gross_revenue"
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df[keep_cols].drop_duplicates(subset=["order_item_id"])


def save_core_tables(base_output: Path, dim_date, dim_customer, dim_product, fct_orders, fct_order_item):
    write_parquet_dataset(dim_date, base_output / "core" / "dim_date", "dim_date.parquet")
    write_parquet_dataset(dim_customer, base_output / "core" / "dim_customer", "dim_customer.parquet")
    write_parquet_dataset(dim_product, base_output / "core" / "dim_product", "dim_product.parquet")
    write_parquet_dataset(fct_orders, base_output / "core" / "fct_orders", "fct_orders.parquet")
    write_parquet_dataset(fct_order_item, base_output / "core" / "fct_order_item", "fct_order_item.parquet")