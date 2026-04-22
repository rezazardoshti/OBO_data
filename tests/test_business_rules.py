from __future__ import annotations

import hashlib
import pandas as pd


def normalize_value(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def hash_columns(values) -> str:
    payload = "||".join(normalize_value(v) for v in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_hub_customer_matches_stage_users(stage_users, hub_customer):
    expected_bk = (
        stage_users["user_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    expected_bk = set(expected_bk[expected_bk != ""])

    actual_bk = set(hub_customer["customer_bk"].astype(str).str.strip())

    assert actual_bk == expected_bk


def test_hub_order_matches_stage_orders(stage_orders, hub_order):
    expected_bk = (
        stage_orders["order_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    expected_bk = set(expected_bk[expected_bk != ""])

    actual_bk = set(hub_order["order_bk"].astype(str).str.strip())

    assert actual_bk == expected_bk


def test_hub_product_matches_stage_products(stage_products, hub_product):
    expected_bk = (
        stage_products["product_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    expected_bk = set(expected_bk[expected_bk != ""])

    actual_bk = set(hub_product["product_bk"].astype(str).str.strip())

    assert actual_bk == expected_bk


def test_hub_session_matches_stage_sessions(stage_sessions, hub_session):
    expected_bk = (
        stage_sessions["session_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    expected_bk = set(expected_bk[expected_bk != ""])

    actual_bk = set(hub_session["session_bk"].astype(str).str.strip())

    assert actual_bk == expected_bk


def test_link_order_customer_matches_stage(stage_orders, link_order_customer):
    expected = stage_orders[["order_id", "user_id"]].dropna().copy()
    expected["order_bk"] = expected["order_id"].astype(str).str.strip()
    expected["customer_bk"] = expected["user_id"].astype(str).str.strip()
    expected = expected[
        (expected["order_bk"] != "") &
        (expected["customer_bk"] != "")
    ][["order_bk", "customer_bk"]].drop_duplicates()

    expected["hk_order_h"] = expected["order_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_customer_h"] = expected["customer_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_order_customer_l"] = expected.apply(
        lambda r: hash_columns([r["order_bk"], r["customer_bk"]]),
        axis=1,
    )

    expected_set = set(
        zip(
            expected["hk_order_customer_l"],
            expected["hk_order_h"],
            expected["hk_customer_h"],
        )
    )
    actual_set = set(
        zip(
            link_order_customer["hk_order_customer_l"],
            link_order_customer["hk_order_h"],
            link_order_customer["hk_customer_h"],
        )
    )

    assert actual_set == expected_set


def test_link_order_session_matches_stage(stage_orders, link_order_session):
    if "session_id" not in stage_orders.columns:
        return

    expected = stage_orders[["order_id", "session_id"]].dropna().copy()
    expected["order_bk"] = expected["order_id"].astype(str).str.strip()
    expected["session_bk"] = expected["session_id"].astype(str).str.strip()
    expected = expected[
        (expected["order_bk"] != "") &
        (expected["session_bk"] != "")
    ][["order_bk", "session_bk"]].drop_duplicates()

    expected["hk_order_h"] = expected["order_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_session_h"] = expected["session_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_order_session_l"] = expected.apply(
        lambda r: hash_columns([r["order_bk"], r["session_bk"]]),
        axis=1,
    )

    expected_set = set(
        zip(
            expected["hk_order_session_l"],
            expected["hk_order_h"],
            expected["hk_session_h"],
        )
    )
    actual_set = set(
        zip(
            link_order_session["hk_order_session_l"],
            link_order_session["hk_order_h"],
            link_order_session["hk_session_h"],
        )
    )

    assert actual_set == expected_set


def test_link_order_product_matches_stage(stage_order_items, link_order_product):
    expected = stage_order_items[["order_id", "product_id"]].dropna().copy()
    expected["order_bk"] = expected["order_id"].astype(str).str.strip()
    expected["product_bk"] = expected["product_id"].astype(str).str.strip()
    expected = expected[
        (expected["order_bk"] != "") &
        (expected["product_bk"] != "")
    ][["order_bk", "product_bk"]].drop_duplicates()

    expected["hk_order_h"] = expected["order_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_product_h"] = expected["product_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_order_product_l"] = expected.apply(
        lambda r: hash_columns([r["order_bk"], r["product_bk"]]),
        axis=1,
    )

    expected_set = set(
        zip(
            expected["hk_order_product_l"],
            expected["hk_order_h"],
            expected["hk_product_h"],
        )
    )
    actual_set = set(
        zip(
            link_order_product["hk_order_product_l"],
            link_order_product["hk_order_h"],
            link_order_product["hk_product_h"],
        )
    )

    assert actual_set == expected_set


def test_link_session_customer_matches_stage(stage_sessions, link_session_customer):
    expected = stage_sessions[["session_id", "user_id"]].dropna().copy()
    expected["session_bk"] = expected["session_id"].astype(str).str.strip()
    expected["customer_bk"] = expected["user_id"].astype(str).str.strip()
    expected = expected[
        (expected["session_bk"] != "") &
        (expected["customer_bk"] != "")
    ][["session_bk", "customer_bk"]].drop_duplicates()

    expected["hk_session_h"] = expected["session_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_customer_h"] = expected["customer_bk"].apply(lambda x: hash_columns([x]))
    expected["hk_session_customer_l"] = expected.apply(
        lambda r: hash_columns([r["session_bk"], r["customer_bk"]]),
        axis=1,
    )

    expected_set = set(
        zip(
            expected["hk_session_customer_l"],
            expected["hk_session_h"],
            expected["hk_customer_h"],
        )
    )
    actual_set = set(
        zip(
            link_session_customer["hk_session_customer_l"],
            link_session_customer["hk_session_h"],
            link_session_customer["hk_customer_h"],
        )
    )

    assert actual_set == expected_set


def test_sat_customer_hashdiff_matches_stage(stage_users, sat_customer_attributes):
    required = ["user_id", "city", "country", "age", "gender", "is_premium", "marketing_opt_in"]
    for col in required:
        assert col in stage_users.columns, f"Spalte fehlt in stage_users: {col}"

    expected = stage_users[required].dropna(subset=["user_id"]).copy()
    expected["customer_bk"] = expected["user_id"].astype(str).str.strip()
    expected = expected[expected["customer_bk"] != ""].copy()

    expected["hk_customer_h"] = expected["customer_bk"].apply(lambda x: hash_columns([x]))
    expected["hashdiff"] = expected.apply(
        lambda r: hash_columns([
            r["city"],
            r["country"],
            r["age"],
            r["gender"],
            r["is_premium"],
            r["marketing_opt_in"],
        ]),
        axis=1,
    )

    expected_set = set(
        zip(
            expected["hk_customer_h"],
            expected["hashdiff"],
        )
    )
    actual_set = set(
        zip(
            sat_customer_attributes["hk_customer_h"],
            sat_customer_attributes["hashdiff"],
        )
    )

    assert actual_set == expected_set