from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import load_config
from src.io_utils import ensure_dir


def latest_entity_file(stage_dir: Path, source: str, entity: str) -> Path:
    entity_root = stage_dir / f"source={source}" / f"entity={entity}"
    candidates = sorted(entity_root.rglob("*.parquet"))
    if not candidates:
        raise FileNotFoundError(
            f"No parquet files found for {source}/{entity} in {entity_root}"
        )
    return candidates[-1]


def check_not_null(df: pd.DataFrame, column: str) -> dict:
    if column not in df.columns:
        return {
            "rule": f"{column}_not_null",
            "failed_rows": None,
            "passed": False,
        }

    failed = int(df[column].isna().sum())
    return {
        "rule": f"{column}_not_null",
        "failed_rows": failed,
        "passed": failed == 0,
    }


def check_unique(df: pd.DataFrame, column: str) -> dict:
    if column not in df.columns:
        return {
            "rule": f"{column}_unique",
            "failed_rows": None,
            "passed": False,
        }

    dupes = int(df[column].duplicated().sum())
    return {
        "rule": f"{column}_unique",
        "failed_rows": dupes,
        "passed": dupes == 0,
    }


def check_non_negative(df: pd.DataFrame, column: str) -> dict:
    if column not in df.columns:
        return {
            "rule": f"{column}_non_negative",
            "failed_rows": None,
            "passed": False,
        }

    failed = int((df[column].fillna(0) < 0).sum())
    return {
        "rule": f"{column}_non_negative",
        "failed_rows": failed,
        "passed": failed == 0,
    }


def main() -> None:
    cfg = load_config()

    stage_dir = cfg["paths"].get("stage_dir", Path("data/stage"))
    audit_dir = cfg["paths"]["audit_dir"]
    ensure_dir(audit_dir)

    checks: list[dict] = []

    # Angepasst an dein aktuelles echtes Stage-Schema
    entities = [
        ("erp", "orders", ["order_id"], ["gross_amount"]),
        ("erp", "order_items", ["order_item_id"], ["quantity", "unit_price"]),
        ("erp", "products", ["product_id"], []),
        ("crm", "users", ["user_id"], []),
        ("web", "sessions", ["session_id"], []),
        ("web", "clickstream_events", ["event_id"], []),
    ]

    for source, entity, key_cols, numeric_cols in entities:
        try:
            parquet_path = latest_entity_file(stage_dir, source, entity)
        except FileNotFoundError:
            checks.append(
                {
                    "source": source,
                    "entity": entity,
                    "rule": "file_exists",
                    "failed_rows": None,
                    "passed": False,
                }
            )
            continue

        df = pd.read_parquet(parquet_path)

        for col in key_cols:
            checks.append(
                {"source": source, "entity": entity, **check_not_null(df, col)}
            )
            checks.append(
                {"source": source, "entity": entity, **check_unique(df, col)}
            )

        for col in numeric_cols:
            checks.append(
                {"source": source, "entity": entity, **check_non_negative(df, col)}
            )

    checks_df = pd.DataFrame(checks)
    out_csv = audit_dir / "block2_stage_rule_checks.csv"
    checks_df.to_csv(out_csv, index=False)

    summary = {
        "total_checks": int(len(checks_df)),
        "passed_checks": int(checks_df["passed"].fillna(False).sum()),
        "failed_checks": int((~checks_df["passed"].fillna(False)).sum()),
    }

    out_json = audit_dir / "block2_stage_rule_checks_summary.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("Stage Quality Checks erfolgreich.")
    print(f"Checks CSV : {out_csv}")
    print(f"Checks JSON: {out_json}")
    print(summary)


if __name__ == "__main__":
    main()