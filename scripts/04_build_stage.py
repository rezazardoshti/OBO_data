from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

from src.config import load_config
from src.io_utils import ensure_dir
from src.standardize import (
    add_metadata_columns,
    quality_metrics,
    standardize_application_logs,
    standardize_clickstream,
    standardize_inventory_snapshots,
    standardize_order_items,
    standardize_orders,
    standardize_products,
    standardize_sessions,
    standardize_users,
)


def discover_input_files(raw_dir: Path) -> list[tuple[str, str, Path]]:
    discovered: list[tuple[str, str, Path]] = []

    for file_path in sorted(raw_dir.rglob("*")):
        if not file_path.is_file():
            continue

        parts = file_path.parts
        source = next((p.split("=", 1)[1] for p in parts if p.startswith("source=")), None)
        entity = next((p.split("=", 1)[1] for p in parts if p.startswith("entity=")), None)

        if source and entity:
            discovered.append((source, entity, file_path))

    return discovered


def read_input_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path)

    if suffix == ".jsonl":
        return pd.read_json(file_path, lines=True)

    raise ValueError(f"Unsupported structured file type: {file_path}")


def build_stage_output_path(stage_dir: Path, source: str, entity: str, load_date: str) -> Path:
    target_dir = stage_dir / f"source={source}" / f"entity={entity}" / f"load_date={load_date}"
    ensure_dir(target_dir)
    return target_dir / f"{entity}.parquet"


def process_structured_entity(source: str, entity: str, file_path: Path) -> tuple[pd.DataFrame, dict]:
    df_raw = read_input_file(file_path)

    if entity == "orders":
        df_stage = standardize_orders(df_raw)
    elif entity == "order_items":
        df_stage = standardize_order_items(df_raw)
    elif entity == "products":
        df_stage = standardize_products(df_raw)
    elif entity == "inventory_snapshots":
        df_stage = standardize_inventory_snapshots(df_raw)
    elif entity == "users":
        df_stage = standardize_users(df_raw)
    elif entity == "sessions":
        df_stage = standardize_sessions(df_raw)
    elif entity == "clickstream_events":
        df_stage = standardize_clickstream(df_raw)
    else:
        raise ValueError(f"Unknown entity: {entity}")

    df_stage = add_metadata_columns(
        df=df_stage,
        source=source,
        entity=entity,
        raw_file_path=file_path,
    )

    metrics = quality_metrics(df_raw, df_stage, entity)
    metrics["source"] = source
    metrics["raw_file_path"] = str(file_path)
    return df_stage, metrics


def process_log_entity(source: str, entity: str, file_path: Path) -> tuple[pd.DataFrame, dict]:
    with open(file_path, "r", encoding="utf-8") as f:
        raw_lines = f.readlines()

    df_before = pd.DataFrame({"raw_log_line": raw_lines})
    df_stage = standardize_application_logs(raw_lines)
    df_stage = add_metadata_columns(
        df=df_stage,
        source=source,
        entity=entity,
        raw_file_path=file_path,
    )

    metrics = quality_metrics(df_before, df_stage, entity)
    metrics["source"] = source
    metrics["raw_file_path"] = str(file_path)
    return df_stage, metrics


def main() -> None:
    cfg = load_config()

    raw_dir = cfg["paths"]["raw_dir"]
    audit_dir = cfg["paths"]["audit_dir"]

    # Fallback, falls stage_dir noch nicht in YAML existiert
    stage_dir = cfg["paths"].get("stage_dir", Path("data/stage"))

    ensure_dir(stage_dir)
    ensure_dir(audit_dir)

    load_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    discovered = discover_input_files(raw_dir)

    quality_rows: list[dict] = []
    written_files: list[str] = []

    for source, entity, file_path in discovered:
        if file_path.suffix.lower() == ".log":
            df_stage, metrics = process_log_entity(source, entity, file_path)
        else:
            df_stage, metrics = process_structured_entity(source, entity, file_path)

        output_path = build_stage_output_path(stage_dir, source, entity, load_date)
        df_stage.to_parquet(output_path, index=False)

        quality_rows.append(metrics)
        written_files.append(str(output_path))

        print(f"[OK] {source}/{entity} -> {output_path}")

    report_df = pd.DataFrame(quality_rows)
    report_csv = audit_dir / "block2_stage_load_report.csv"
    report_df.to_csv(report_csv, index=False)

    summary = {
        "load_date": load_date,
        "entities_processed": len(quality_rows),
        "written_files": written_files,
    }
    summary_json = audit_dir / "block2_stage_quality_summary.json"
    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("\nBlock 2 erfolgreich.")
    print(f"Stage dir : {stage_dir}")
    print(f"Report    : {report_csv}")
    print(f"Summary   : {summary_json}")


if __name__ == "__main__":
    main()