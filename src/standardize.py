from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        re.sub(r"_+", "_", col.strip().lower().replace(" ", "_"))
        for col in df.columns
    ]
    return df


def clean_string_series(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "null": pd.NA})
    )


def normalize_text_value(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA
    return str(value).strip().lower()


def title_case_value(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA
    return str(value).strip().title()


def upper_value(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA
    return str(value).strip().upper()


def standardize_common_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    object_like_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in object_like_cols:
        df[col] = clean_string_series(df[col])
    return df


def safe_to_datetime(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
    return df


def safe_to_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def add_metadata_columns(
    df: pd.DataFrame,
    source: str,
    entity: str,
    raw_file_path: Path,
) -> pd.DataFrame:
    df = df.copy()
    df["record_source"] = source
    df["record_entity"] = entity
    df["raw_file_path"] = str(raw_file_path)
    df["load_timestamp"] = pd.Timestamp.utcnow()
    return df


def quality_metrics(df_before: pd.DataFrame, df_after: pd.DataFrame, entity: str) -> dict[str, Any]:
    null_counts = {
        col: int(df_after[col].isna().sum())
        for col in df_after.columns
    }

    return {
        "entity": entity,
        "rows_before": int(len(df_before)),
        "rows_after": int(len(df_after)),
        "duplicates_removed": int(len(df_before) - len(df_before.drop_duplicates())),
        "columns_after": list(df_after.columns),
        "null_counts_after": null_counts,
    }


def standardize_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_common_strings(df)

    df = safe_to_datetime(df, ["order_date", "order_ts", "created_at"])
    df = safe_to_numeric(df, ["total_amount", "subtotal_amount", "discount_amount", "shipping_amount"])

    if "payment_method" in df.columns:
        payment_map = {
            "paypal": "PayPal",
            "pay pal": "PayPal",
            "google": "Google",
            "google pay": "Google Pay",
            "credit card": "Credit Card",
            "card": "Credit Card",
        }
        df["payment_method"] = df["payment_method"].map(
            lambda x: payment_map.get(normalize_text_value(x), title_case_value(x))
        )

    if "currency" in df.columns:
        df["currency"] = df["currency"].map(lambda x: upper_value(x))

    df = df.drop_duplicates()
    return df


def standardize_order_items(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_common_strings(df)
    df = safe_to_numeric(df, ["quantity", "unit_price", "line_amount", "discount_amount"])
    df = df.drop_duplicates()
    return df


def standardize_products(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_common_strings(df)
    df = safe_to_numeric(df, ["price", "cost", "stock_quantity"])

    if "product_name" in df.columns:
        df["product_name"] = df["product_name"].astype("string").str.strip()

    if "category" in df.columns:
        df["category"] = df["category"].map(lambda x: title_case_value(x))

    df = df.drop_duplicates()
    return df


def standardize_inventory_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_common_strings(df)
    df = safe_to_datetime(df, ["snapshot_date", "snapshot_ts"])
    df = safe_to_numeric(df, ["stock_on_hand", "reserved_qty", "available_qty"])
    df = df.drop_duplicates()
    return df


def standardize_users(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_common_strings(df)
    df = safe_to_datetime(df, ["created_at", "signup_date", "birth_date"])

    if "email" in df.columns:
        df["email"] = df["email"].astype("string").str.strip().str.lower()

    if "country" in df.columns:
        df["country"] = df["country"].map(lambda x: upper_value(x))

    if "marketing_channel" in df.columns:
        df["marketing_channel"] = df["marketing_channel"].map(lambda x: title_case_value(x))

    df = df.drop_duplicates()
    return df


def standardize_sessions(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_common_strings(df)
    df = safe_to_datetime(df, ["session_start", "session_end", "session_start_ts", "session_end_ts"])

    if "device" in df.columns:
        device_map = {
            "desktop": "Desktop",
            "mobile": "Mobile",
            "tablet": "Tablet",
        }
        df["device"] = df["device"].map(
            lambda x: device_map.get(normalize_text_value(x), title_case_value(x))
        )

    if "traffic_source" in df.columns:
        df["traffic_source"] = df["traffic_source"].map(lambda x: title_case_value(x))

    df = df.drop_duplicates()
    return df


def standardize_clickstream(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_common_strings(df)
    df = safe_to_datetime(df, ["event_timestamp", "event_ts"])

    if "event_type" in df.columns:
        event_map = {
            "pageview": "page_view",
            "page_view": "page_view",
            "productview": "product_view",
            "product_view": "product_view",
            "addtocart": "add_to_cart",
            "add_to_cart": "add_to_cart",
            "checkout": "checkout",
            "purchase": "purchase",
        }
        df["event_type"] = df["event_type"].map(
            lambda x: event_map.get(normalize_text_value(x), normalize_text_value(x))
        )

    df = df.drop_duplicates()
    return df


LOG_PATTERN = re.compile(
    r"^(?P<log_ts>\S+\s+\S+)\s+\[(?P<level>[A-Z]+)\]\s+(?P<message>.*)$"
)


def standardize_application_logs(raw_lines: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for line in raw_lines:
        line = line.rstrip("\n")
        match = LOG_PATTERN.match(line)
        if match:
            rows.append(
                {
                    "log_timestamp": match.group("log_ts"),
                    "log_level": match.group("level"),
                    "message": match.group("message"),
                    "raw_log_line": line,
                }
            )
        else:
            rows.append(
                {
                    "log_timestamp": pd.NA,
                    "log_level": pd.NA,
                    "message": line,
                    "raw_log_line": line,
                }
            )

    df = pd.DataFrame(rows)
    df = safe_to_datetime(df, ["log_timestamp"])
    df = standardize_common_strings(df)

    if "log_level" in df.columns:
        df["log_level"] = df["log_level"].map(lambda x: upper_value(x))

    df = df.drop_duplicates()
    return df


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)