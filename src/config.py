from pathlib import Path
import yaml


def load_config(config_path: str = "conf/base_config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Pfade als Path-Objekte normalisieren
    cfg["paths"] = {k: Path(v) for k, v in cfg["paths"].items()}
    return cfg