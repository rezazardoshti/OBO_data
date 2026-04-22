import pandas as pd


def assert_not_empty(df: pd.DataFrame, name: str) -> None:
    if df.empty:
        raise ValueError(f"{name} is empty")


def assert_unique(df: pd.DataFrame, column: str, name: str) -> None:
    if df[column].duplicated().any():
        dupes = int(df[column].duplicated().sum())
        raise ValueError(f"{name}.{column} has {dupes} duplicate values")


def assert_required_columns(df: pd.DataFrame, required_cols: list[str], name: str) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")