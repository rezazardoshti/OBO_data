import hashlib
from pathlib import Path


def sha256_file(file_path: Path, chunk_size: int = 8192) -> str:
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha.update(chunk)
    return sha.hexdigest()