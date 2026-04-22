import pandas as pd


def build_mart_sales_daily(fct_orders: pd.DataFrame, fct_order_item: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    order_counts = (
        fct_orders.groupby("date_sk", dropna=False)
        .agg(orders_count=("order_id", "nunique"),
             unique_customers=("customer_sk", "nunique"))
        .reset_index()
    )

    item_metrics = (
        fct_order_item.groupby("date_sk", dropna=False)
        .agg(
            order_items_count=("order_item_id", "nunique"),
            units_sold=("quantity", "sum"),
            gross_revenue=("gross_revenue", "sum")
        )
        .reset_index()
    )

    mart = order_counts.merge(item_metrics, on="date_sk", how="outer")
    mart["average_order_value"] = mart["gross_revenue"] / mart["orders_count"]
    mart = mart.merge(dim_date[["date_sk", "full_date", "year", "month", "quarter"]], on="date_sk", how="left")
    return mart.sort_values(["full_date"]).reset_index(drop=True)


def build_mart_product_performance(fct_order_item: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:
    mart = (
        fct_order_item.groupby(["product_sk", "product_id"], dropna=False)
        .agg(
            orders_count=("order_id", "nunique"),
            units_sold=("quantity", "sum"),
            gross_revenue=("gross_revenue", "sum"),
            unique_buyers=("customer_sk", "nunique")
        )
        .reset_index()
    )
    mart["avg_selling_price"] = mart["gross_revenue"] / mart["units_sold"]
    mart = mart.merge(dim_product, on=["product_sk", "product_id"], how="left")
    return mart.sort_values(["gross_revenue"], ascending=False).reset_index(drop=True)


def build_mart_customer_360(fct_orders: pd.DataFrame, fct_order_item: pd.DataFrame, dim_customer: pd.DataFrame) -> pd.DataFrame:
    orders = fct_orders.copy()
    orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce")

    customer_orders = (
        orders.groupby("customer_sk", dropna=False)
        .agg(
            first_order_ts=("order_ts", "min"),
            last_order_ts=("order_ts", "max"),
            total_orders=("order_id", "nunique")
        )
        .reset_index()
    )

    customer_revenue = (
        fct_order_item.groupby("customer_sk", dropna=False)
        .agg(
            total_revenue=("gross_revenue", "sum"),
            total_units=("quantity", "sum")
        )
        .reset_index()
    )

    mart = dim_customer.merge(customer_orders, on="customer_sk", how="left")
    mart = mart.merge(customer_revenue, on="customer_sk", how="left")

    mart["average_order_value"] = mart["total_revenue"] / mart["total_orders"]
    mart["days_since_last_order"] = (
        pd.Timestamp.today().normalize() - pd.to_datetime(mart["last_order_ts"])
    ).dt.days

    def segment(row):
        if pd.isna(row["total_orders"]) or row["total_orders"] == 0:
            return "no_order"
        if row["total_orders"] == 1:
            return "new"
        if row["total_orders"] >= 5:
            return "loyal"
        return "active"

    mart["customer_segment"] = mart.apply(segment, axis=1)
    return mart