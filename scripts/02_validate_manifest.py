from pathlib import Path
import json
import pandas as pd

from src.config import load_config
from src.hash_utils import sha256_file
from src.io_utils import ensure_dir


def main():
    cfg = load_config()

    raw_dir = cfg["paths"]["raw_dir"]
    audit_dir = cfg["paths"]["audit_dir"]
    extracted_dir = cfg["paths"]["extracted_dir"]

    ensure_dir(audit_dir)

    manifest_path = extracted_dir / cfg["files"]["manifest_relative_path"]
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest_df = pd.read_csv(manifest_path)

    validation_rows = []

    for _, row in manifest_df.iterrows():
        relative_path = row["relative_path"]
        expected_size = int(row["file_size_bytes"])
        expected_sha = row["sha256"]

        # Im Manifest steht data/raw/...
        # Wir prüfen daher gegen data/raw/... im Projekt
        raw_relative = Path(relative_path).relative_to("data/raw")
        target_file = raw_dir / raw_relative

        exists = target_file.exists()
        actual_size = target_file.stat().st_size if exists else None
        actual_sha = sha256_file(target_file) if exists else None

        validation_rows.append({
            "relative_path": relative_path,
            "target_file": str(target_file),
            "exists": exists,
            "expected_size": expected_size,
            "actual_size": actual_size,
            "size_match": expected_size == actual_size if exists else False,
            "expected_sha256": expected_sha,
            "actual_sha256": actual_sha,
            "sha_match": expected_sha == actual_sha if exists else False
        })

    result_df = pd.DataFrame(validation_rows)
    result_df["valid"] = result_df["exists"] & result_df["size_match"] & result_df["sha_match"]

    output_csv = audit_dir / "manifest_validation_report.csv"
    result_df.to_csv(output_csv, index=False)

    summary = {
        "total_files": int(len(result_df)),
        "valid_files": int(result_df["valid"].sum()),
        "invalid_files": int((~result_df["valid"]).sum())
    }

    output_json = audit_dir / "manifest_validation_summary.json"
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("Block 1 / Step 2 erfolgreich.")
    print(f"Report : {output_csv}")
    print(f"Summary: {output_json}")
    print(summary)


if __name__ == "__main__":
    main()