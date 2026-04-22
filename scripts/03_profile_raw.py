from pathlib import Path
import json
import pandas as pd

from src.config import load_config
from src.io_utils import ensure_dir


def detect_format(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix == ".jsonl":
        return "jsonl"
    if suffix == ".log":
        return "log"
    return "unknown"


def load_dataframe(file_path: Path, file_format: str) -> pd.DataFrame:
    if file_format == "csv":
        return pd.read_csv(file_path)
    if file_format == "jsonl":
        return pd.read_json(file_path, lines=True)
    if file_format == "log":
        # unstrukturierte Logs werden als 1 Spalte geladen
        with open(file_path, "r", encoding="utf-8") as f:
            lines = [line.rstrip("\n") for line in f]
        return pd.DataFrame({"raw_log_line": lines})
    raise ValueError(f"Unsupported file format: {file_format}")


def profile_dataframe(df: pd.DataFrame, entity_name: str, file_path: Path) -> dict:
    profile = {
        "entity": entity_name,
        "file_path": str(file_path),
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": list(df.columns),
        "null_counts": {col: int(df[col].isna().sum()) for col in df.columns},
        "dtypes": {col: str(df[col].dtype) for col in df.columns},
        "duplicate_row_count": int(df.duplicated().sum()),
        "sample_rows": df.head(5).to_dict(orient="records")
    }
    return profile


def entity_name_from_path(file_path: Path) -> str:
    # erwartet Pfade wie source=erp/entity=orders/ingest_date=.../orders.csv
    for part in file_path.parts:
        if part.startswith("entity="):
            return part.split("=", 1)[1]
    return file_path.stem


def main():
    cfg = load_config()

    raw_dir = cfg["paths"]["raw_dir"]
    audit_dir = cfg["paths"]["audit_dir"]
    ensure_dir(audit_dir)

    all_profiles = []

    files = [p for p in raw_dir.rglob("*") if p.is_file()]
    for file_path in sorted(files):
        file_format = detect_format(file_path)
        if file_format == "unknown":
            continue

        entity = entity_name_from_path(file_path)
        df = load_dataframe(file_path, file_format)
        profile = profile_dataframe(df, entity, file_path)
        profile["file_format"] = file_format

        all_profiles.append(profile)

    out_json = audit_dir / "raw_profiling_report.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(all_profiles, f, indent=2, ensure_ascii=False, default=str)

    summary_rows = []
    for p in all_profiles:
        summary_rows.append({
            "entity": p["entity"],
            "file_path": p["file_path"],
            "file_format": p["file_format"],
            "row_count": p["row_count"],
            "column_count": p["column_count"],
            "duplicate_row_count": p["duplicate_row_count"]
        })

    summary_df = pd.DataFrame(summary_rows)
    out_csv = audit_dir / "raw_profiling_summary.csv"
    summary_df.to_csv(out_csv, index=False)

    print("Block 1 / Step 3 erfolgreich.")
    print(f"Detailed profiling: {out_json}")
    print(f"Summary profiling : {out_csv}")


if __name__ == "__main__":
    main()