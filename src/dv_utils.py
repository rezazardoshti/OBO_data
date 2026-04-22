from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import pandas as pd


def normalize_value(value) -> str:
    """
    Normalisiert Werte für Hashing:
    - None/NaN -> ''
    - trim
    - alles als String
    """
    if pd.isna(value):
        return ""
    return str(value).strip()


def hash_columns(values: Iterable) -> str:
    """
    Erzeugt einen stabilen SHA-256 Hash über eine Liste von Werten.
    """
    normalized = [normalize_value(v) for v in values]
    payload = "||".join(normalized)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def add_hash_key(df: pd.DataFrame, target_col: str, source_cols: list[str]) -> pd.DataFrame:
    """
    Fügt eine Hash-Key-Spalte hinzu.
    """
    out = df.copy()
    out[target_col] = out[source_cols].apply(lambda row: hash_columns(row.tolist()), axis=1)
    return out


def add_hashdiff(df: pd.DataFrame, target_col: str, attribute_cols: list[str]) -> pd.DataFrame:
    """
    Fügt eine Hashdiff-Spalte für Satellite-Attribute hinzu.
    """
    out = df.copy()
    out[target_col] = out[attribute_cols].apply(lambda row: hash_columns(row.tolist()), axis=1)
    return out


def latest_file_in_entity_dir(base_dir: Path, entity: str) -> Path:
    """
    Erwartet:
    base_dir/source=.../entity=<entity>/load_date=.../*.parquet
    oder
    base_dir/entity=<entity>/load_date=.../*.parquet

    Gibt die zuletzt sortierte Parquet-Datei zurück.
    """
    candidates = sorted(base_dir.rglob(f"entity={entity}/**/*.parquet"))
    if not candidates:
        # Fallback, falls Stage anders abgelegt wurde
        candidates = sorted(base_dir.rglob(f"*{entity}*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"Keine Stage-Datei für Entity '{entity}' gefunden unter {base_dir}")
    return candidates[-1]


def read_latest_stage_entity(stage_dir: Path, entity: str) -> pd.DataFrame:
    file_path = latest_file_in_entity_dir(stage_dir, entity)
    return pd.read_parquet(file_path)


def ensure_columns(df: pd.DataFrame, required_cols: list[str], entity_name: str) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Entity '{entity_name}' fehlt erforderliche Spalten: {missing}. "
            f"Vorhanden: {list(df.columns)}"
        )


def standardize_metadata(df: pd.DataFrame, record_source: str) -> pd.DataFrame:
    out = df.copy()
    out["load_ts"] = pd.Timestamp.utcnow().tz_localize(None)
    out["record_source"] = record_source
    return out


def write_parquet_dataset(df: pd.DataFrame, target_root: Path, dataset_name: str) -> Path:
    load_date = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    out_dir = target_root / dataset_name / f"load_date={load_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{dataset_name}.parquet"
    df.to_parquet(out_file, index=False)
    return out_file