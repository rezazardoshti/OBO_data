from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import load_config
from src.io_utils import ensure_dir
from src.dv_utils import (
    add_hash_key,
    add_hashdiff,
    ensure_columns,
    read_latest_stage_entity,
    standardize_metadata,
    write_parquet_dataset,
)


def build_hub_customer(users: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(users, ["user_id"], "users")

    hub = users[["user_id"]].copy()
    hub = hub[hub["user_id"].notna()]
    hub["customer_bk"] = hub["user_id"].astype(str).str.strip()
    hub = hub[hub["customer_bk"] != ""]
    hub = hub[["customer_bk"]].drop_duplicates()

    hub = add_hash_key(hub, "hk_customer_h", ["customer_bk"])
    hub = standardize_metadata(hub, "stage.users")

    return hub[["hk_customer_h", "customer_bk", "load_ts", "record_source"]]


def build_hub_order(orders: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(orders, ["order_id"], "orders")

    hub = orders[["order_id"]].copy()
    hub = hub[hub["order_id"].notna()]
    hub["order_bk"] = hub["order_id"].astype(str).str.strip()
    hub = hub[hub["order_bk"] != ""]
    hub = hub[["order_bk"]].drop_duplicates()

    hub = add_hash_key(hub, "hk_order_h", ["order_bk"])
    hub = standardize_metadata(hub, "stage.orders")

    return hub[["hk_order_h", "order_bk", "load_ts", "record_source"]]


def build_hub_product(products: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(products, ["product_id"], "products")

    hub = products[["product_id"]].copy()
    hub = hub[hub["product_id"].notna()]
    hub["product_bk"] = hub["product_id"].astype(str).str.strip()
    hub = hub[hub["product_bk"] != ""]
    hub = hub[["product_bk"]].drop_duplicates()

    hub = add_hash_key(hub, "hk_product_h", ["product_bk"])
    hub = standardize_metadata(hub, "stage.products")

    return hub[["hk_product_h", "product_bk", "load_ts", "record_source"]]


def build_hub_session(sessions: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(sessions, ["session_id"], "sessions")

    hub = sessions[["session_id"]].copy()
    hub = hub[hub["session_id"].notna()]
    hub["session_bk"] = hub["session_id"].astype(str).str.strip()
    hub = hub[hub["session_bk"] != ""]
    hub = hub[["session_bk"]].drop_duplicates()

    hub = add_hash_key(hub, "hk_session_h", ["session_bk"])
    hub = standardize_metadata(hub, "stage.sessions")

    return hub[["hk_session_h", "session_bk", "load_ts", "record_source"]]


def build_link_order_customer(orders: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(orders, ["order_id", "user_id"], "orders")

    link = orders[["order_id", "user_id"]].copy()
    link = link.dropna(subset=["order_id", "user_id"])
    link["order_bk"] = link["order_id"].astype(str).str.strip()
    link["customer_bk"] = link["user_id"].astype(str).str.strip()
    link = link[(link["order_bk"] != "") & (link["customer_bk"] != "")]
    link = link[["order_bk", "customer_bk"]].drop_duplicates()

    link = add_hash_key(link, "hk_order_h", ["order_bk"])
    link = add_hash_key(link, "hk_customer_h", ["customer_bk"])
    link = add_hash_key(link, "hk_order_customer_l", ["order_bk", "customer_bk"])
    link = standardize_metadata(link, "stage.orders")

    return link[
        ["hk_order_customer_l", "hk_order_h", "hk_customer_h", "load_ts", "record_source"]
    ]


def build_link_order_session(orders: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(orders, ["order_id", "session_id"], "orders")

    link = orders[["order_id", "session_id"]].copy()
    link = link.dropna(subset=["order_id", "session_id"])
    link["order_bk"] = link["order_id"].astype(str).str.strip()
    link["session_bk"] = link["session_id"].astype(str).str.strip()
    link = link[(link["order_bk"] != "") & (link["session_bk"] != "")]
    link = link[["order_bk", "session_bk"]].drop_duplicates()

    link = add_hash_key(link, "hk_order_h", ["order_bk"])
    link = add_hash_key(link, "hk_session_h", ["session_bk"])
    link = add_hash_key(link, "hk_order_session_l", ["order_bk", "session_bk"])
    link = standardize_metadata(link, "stage.orders")

    return link[
        ["hk_order_session_l", "hk_order_h", "hk_session_h", "load_ts", "record_source"]
    ]


def build_link_order_product(order_items: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(order_items, ["order_id", "product_id"], "order_items")

    link = order_items[["order_id", "product_id"]].copy()
    link = link.dropna(subset=["order_id", "product_id"])
    link["order_bk"] = link["order_id"].astype(str).str.strip()
    link["product_bk"] = link["product_id"].astype(str).str.strip()
    link = link[(link["order_bk"] != "") & (link["product_bk"] != "")]
    link = link[["order_bk", "product_bk"]].drop_duplicates()

    link = add_hash_key(link, "hk_order_h", ["order_bk"])
    link = add_hash_key(link, "hk_product_h", ["product_bk"])
    link = add_hash_key(link, "hk_order_product_l", ["order_bk", "product_bk"])
    link = standardize_metadata(link, "stage.order_items")

    return link[
        ["hk_order_product_l", "hk_order_h", "hk_product_h", "load_ts", "record_source"]
    ]


def build_link_session_customer(sessions: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(sessions, ["session_id", "user_id"], "sessions")

    link = sessions[["session_id", "user_id"]].copy()
    link = link.dropna(subset=["session_id", "user_id"])
    link["session_bk"] = link["session_id"].astype(str).str.strip()
    link["customer_bk"] = link["user_id"].astype(str).str.strip()
    link = link[(link["session_bk"] != "") & (link["customer_bk"] != "")]
    link = link[["session_bk", "customer_bk"]].drop_duplicates()

    link = add_hash_key(link, "hk_session_h", ["session_bk"])
    link = add_hash_key(link, "hk_customer_h", ["customer_bk"])
    link = add_hash_key(link, "hk_session_customer_l", ["session_bk", "customer_bk"])
    link = standardize_metadata(link, "stage.sessions")

    return link[
        ["hk_session_customer_l", "hk_session_h", "hk_customer_h", "load_ts", "record_source"]
    ]


def build_sat_customer_attributes(users: pd.DataFrame) -> pd.DataFrame:
    required = ["user_id", "city", "country", "age", "gender", "is_premium", "marketing_opt_in"]
    ensure_columns(users, required, "users")

    sat = users[required].copy()
    sat = sat.dropna(subset=["user_id"])
    sat["customer_bk"] = sat["user_id"].astype(str).str.strip()
    sat = sat[sat["customer_bk"] != ""]

    sat = add_hash_key(sat, "hk_customer_h", ["customer_bk"])
    attr_cols = ["city", "country", "age", "gender", "is_premium", "marketing_opt_in"]
    sat = add_hashdiff(sat, "hashdiff", attr_cols)
    sat = standardize_metadata(sat, "stage.users")

    sat = sat[
        [
            "hk_customer_h",
            "hashdiff",
            "city",
            "country",
            "age",
            "gender",
            "is_premium",
            "marketing_opt_in",
            "load_ts",
            "record_source",
        ]
    ].drop_duplicates()

    return sat


def build_sat_order_details(orders: pd.DataFrame) -> pd.DataFrame:
    # Passe die Liste an deine echten Stage-Spalten an
    candidate_cols = [
        "order_id",
        "order_ts",
        "status",
        "amount",
        "currency",
        "payment_method",
        "channel",
    ]
    available = [c for c in candidate_cols if c in orders.columns]
    if "order_id" not in available:
        raise ValueError("orders benötigt mindestens 'order_id' für sat_order_details")

    sat = orders[available].copy()
    sat = sat.dropna(subset=["order_id"])
    sat["order_bk"] = sat["order_id"].astype(str).str.strip()
    sat = sat[sat["order_bk"] != ""]

    sat = add_hash_key(sat, "hk_order_h", ["order_bk"])
    attr_cols = [c for c in available if c != "order_id"]
    sat = add_hashdiff(sat, "hashdiff", attr_cols if attr_cols else ["order_bk"])
    sat = standardize_metadata(sat, "stage.orders")

    out_cols = ["hk_order_h", "hashdiff"] + attr_cols + ["load_ts", "record_source"]
    sat = sat[out_cols].drop_duplicates()

    return sat


def build_sat_product_attributes(products: pd.DataFrame) -> pd.DataFrame:
    candidate_cols = [
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "currency",
        "is_active",
    ]
    available = [c for c in candidate_cols if c in products.columns]
    if "product_id" not in available:
        raise ValueError("products benötigt mindestens 'product_id' für sat_product_attributes")

    sat = products[available].copy()
    sat = sat.dropna(subset=["product_id"])
    sat["product_bk"] = sat["product_id"].astype(str).str.strip()
    sat = sat[sat["product_bk"] != ""]

    sat = add_hash_key(sat, "hk_product_h", ["product_bk"])
    attr_cols = [c for c in available if c != "product_id"]
    sat = add_hashdiff(sat, "hashdiff", attr_cols if attr_cols else ["product_bk"])
    sat = standardize_metadata(sat, "stage.products")

    out_cols = ["hk_product_h", "hashdiff"] + attr_cols + ["load_ts", "record_source"]
    sat = sat[out_cols].drop_duplicates()

    return sat


def build_sat_session_attributes(sessions: pd.DataFrame) -> pd.DataFrame:
    candidate_cols = [
        "session_id",
        "session_start_ts",
        "session_end_ts",
        "device_type",
        "traffic_source",
        "utm_source",
        "utm_medium",
        "utm_campaign",
    ]
    available = [c for c in candidate_cols if c in sessions.columns]
    if "session_id" not in available:
        raise ValueError("sessions benötigt mindestens 'session_id' für sat_session_attributes")

    sat = sessions[available].copy()
    sat = sat.dropna(subset=["session_id"])
    sat["session_bk"] = sat["session_id"].astype(str).str.strip()
    sat = sat[sat["session_bk"] != ""]

    sat = add_hash_key(sat, "hk_session_h", ["session_bk"])
    attr_cols = [c for c in available if c != "session_id"]
    sat = add_hashdiff(sat, "hashdiff", attr_cols if attr_cols else ["session_bk"])
    sat = standardize_metadata(sat, "stage.sessions")

    out_cols = ["hk_session_h", "hashdiff"] + attr_cols + ["load_ts", "record_source"]
    sat = sat[out_cols].drop_duplicates()

    return sat


def main() -> None:
    cfg = load_config()

    stage_dir: Path = cfg["paths"]["stage_dir"]
    vault_dir: Path = cfg["paths"]["vault_dir"]
    audit_dir: Path = cfg["paths"]["audit_dir"]

    ensure_dir(vault_dir)
    ensure_dir(audit_dir)

    users = read_latest_stage_entity(stage_dir, "users")
    orders = read_latest_stage_entity(stage_dir, "orders")
    products = read_latest_stage_entity(stage_dir, "products")
    sessions = read_latest_stage_entity(stage_dir, "sessions")
    order_items = read_latest_stage_entity(stage_dir, "order_items")

    datasets = {
        "hub_customer": build_hub_customer(users),
        "hub_order": build_hub_order(orders),
        "hub_product": build_hub_product(products),
        "hub_session": build_hub_session(sessions),
        "link_order_customer": build_link_order_customer(orders),
        "link_order_session": build_link_order_session(orders),
        "link_order_product": build_link_order_product(order_items),
        "link_session_customer": build_link_session_customer(sessions),
        "sat_customer_attributes": build_sat_customer_attributes(users),
        "sat_order_details": build_sat_order_details(orders),
        "sat_product_attributes": build_sat_product_attributes(products),
        "sat_session_attributes": build_sat_session_attributes(sessions),
    }

    written_files = {}
    summary_rows = []

    for name, df in datasets.items():
        out_file = write_parquet_dataset(df, vault_dir, name)
        written_files[name] = str(out_file)
        summary_rows.append(
            {
                "dataset_name": name,
                "row_count": int(len(df)),
                "column_count": int(len(df.columns)),
                "output_file": str(out_file),
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = audit_dir / "data_vault_load_summary.csv"
    summary_df.to_csv(summary_csv, index=False)

    summary_json = audit_dir / "data_vault_load_summary.json"
    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(
            {
                "written_files": written_files,
                "dataset_counts": {row["dataset_name"]: row["row_count"] for row in summary_rows},
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print("Block 3 / Data Vault erfolgreich.")
    print(f"Summary CSV : {summary_csv}")
    print(f"Summary JSON: {summary_json}")
    for name, path in written_files.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()