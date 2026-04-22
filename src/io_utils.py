import shutil
import zipfile
from pathlib import Path


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def extract_zip(zip_path: Path, target_dir: Path) -> None:
    ensure_dir(target_dir)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)