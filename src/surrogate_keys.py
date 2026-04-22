import hashlib
import pandas as pd


def make_surrogate_key(*values) -> str:
    normalized = "||".join("" if pd.isna(v) else str(v).strip() for v in values)
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def build_sk_series(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    return df[columns].apply(lambda row: make_surrogate_key(*row.tolist()), axis=1)