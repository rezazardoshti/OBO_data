from pathlib import Path
import pandas as pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def latest_file_in_dir(base_dir: Path, pattern: str = "*.parquet") -> Path:
    files = sorted(base_dir.rglob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found under {base_dir} with pattern {pattern}")
    return files[-1]


def read_parquet_dataset(base_dir: Path) -> pd.DataFrame:
    files = sorted(base_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {base_dir}")
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def write_parquet_dataset(df: pd.DataFrame, target_dir: Path, file_name: str) -> Path:
    ensure_dir(target_dir)
    output_path = target_dir / file_name
    df.to_parquet(output_path, index=False)
    return output_path