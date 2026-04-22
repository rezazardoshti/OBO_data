from __future__ import annotations

from pathlib import Path
import hashlib
import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
STAGE_DIR = DATA_DIR / "stage"
VAULT_DIR = DATA_DIR / "vault"


def latest_parquet_in(folder: Path) -> Path:
    files = sorted(folder.rglob("*.parquet"))
    assert files, f"Keine Parquet-Datei gefunden unter: {folder}"
    return files[-1]


def latest_dataset_file(base_dir: Path, dataset_name: str) -> Path:
    folder = base_dir / dataset_name
    assert folder.exists(), f"Ordner fehlt: {folder}"
    return latest_parquet_in(folder)


def read_latest_dataset(base_dir: Path, dataset_name: str) -> pd.DataFrame:
    return pd.read_parquet(latest_dataset_file(base_dir, dataset_name))


def normalize_value(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def hash_columns(values) -> str:
    payload = "||".join(normalize_value(v) for v in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def infer_latest_stage_entity(entity: str) -> pd.DataFrame:
    candidates = sorted(STAGE_DIR.rglob(f"entity={entity}/**/*.parquet"))
    if not candidates:
        candidates = sorted(STAGE_DIR.rglob(f"*{entity}*.parquet"))
    assert candidates, f"Keine Stage-Datei für Entity '{entity}' gefunden unter {STAGE_DIR}"
    return pd.read_parquet(candidates[-1])


@pytest.fixture(scope="session")
def hub_customer():
    return read_latest_dataset(VAULT_DIR, "hub_customer")


@pytest.fixture(scope="session")
def hub_order():
    return read_latest_dataset(VAULT_DIR, "hub_order")


@pytest.fixture(scope="session")
def hub_product():
    return read_latest_dataset(VAULT_DIR, "hub_product")


@pytest.fixture(scope="session")
def hub_session():
    return read_latest_dataset(VAULT_DIR, "hub_session")


@pytest.fixture(scope="session")
def link_order_customer():
    return read_latest_dataset(VAULT_DIR, "link_order_customer")


@pytest.fixture(scope="session")
def link_order_session():
    return read_latest_dataset(VAULT_DIR, "link_order_session")


@pytest.fixture(scope="session")
def link_order_product():
    return read_latest_dataset(VAULT_DIR, "link_order_product")


@pytest.fixture(scope="session")
def link_session_customer():
    return read_latest_dataset(VAULT_DIR, "link_session_customer")


@pytest.fixture(scope="session")
def sat_customer_attributes():
    return read_latest_dataset(VAULT_DIR, "sat_customer_attributes")


@pytest.fixture(scope="session")
def sat_order_details():
    return read_latest_dataset(VAULT_DIR, "sat_order_details")


@pytest.fixture(scope="session")
def sat_product_attributes():
    return read_latest_dataset(VAULT_DIR, "sat_product_attributes")


@pytest.fixture(scope="session")
def sat_session_attributes():
    return read_latest_dataset(VAULT_DIR, "sat_session_attributes")


@pytest.fixture(scope="session")
def stage_users():
    return infer_latest_stage_entity("users")


@pytest.fixture(scope="session")
def stage_orders():
    return infer_latest_stage_entity("orders")


@pytest.fixture(scope="session")
def stage_products():
    return infer_latest_stage_entity("products")


@pytest.fixture(scope="session")
def stage_sessions():
    return infer_latest_stage_entity("sessions")


@pytest.fixture(scope="session")
def stage_order_items():
    return infer_latest_stage_entity("order_items")