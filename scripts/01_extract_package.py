from pathlib import Path
import shutil

from src.config import load_config
from src.io_utils import ensure_dir, extract_zip, copy_tree


def main():
    cfg = load_config()

    package_zip = cfg["paths"]["package_zip"]
    landing_dir = cfg["paths"]["landing_dir"]
    raw_dir = cfg["paths"]["raw_dir"]
    audit_dir = cfg["paths"]["audit_dir"]
    work_dir = cfg["paths"]["work_dir"]
    extracted_dir = cfg["paths"]["extracted_dir"]

    # Verzeichnisse anlegen
    for p in [landing_dir, raw_dir, audit_dir, work_dir, extracted_dir]:
        ensure_dir(p)

    # ZIP in Landing kopieren
    landing_zip = landing_dir / package_zip.name
    shutil.copy2(package_zip, landing_zip)

    # ZIP entpacken
    extract_zip(landing_zip, extracted_dir)

    # Erwarteter Root-Ordner
    package_root = extracted_dir / "ecommerce_raw_professional"
    if not package_root.exists():
        raise FileNotFoundError(f"Package root not found: {package_root}")

    # raw-Daten unverändert nach data/raw kopieren
    source_raw = package_root / "data" / "raw"
    if not source_raw.exists():
        raise FileNotFoundError(f"Raw source folder not found: {source_raw}")

    copy_tree(source_raw, raw_dir)

    # docs ebenfalls separat sichern
    docs_dir = package_root / "docs"
    if docs_dir.exists():
        local_docs = Path("docs")
        ensure_dir(local_docs)
        for file in docs_dir.iterdir():
            if file.is_file():
                shutil.copy2(file, local_docs / file.name)

    print("Block 1 / Step 1 erfolgreich.")
    print(f"Landing ZIP      : {landing_zip}")
    print(f"Extracted Package: {package_root}")
    print(f"Raw Zone         : {raw_dir}")
    print(f"Docs copied to   : docs/")


if __name__ == "__main__":
    main()