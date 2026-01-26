from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def contracts_root() -> Path:
    return Path(__file__).resolve().parents[1] / "contracts"


def step_schema_root() -> Path:
    return Path(__file__).resolve().parents[1] / "schemas" / "steps"


def protocols_root() -> Path:
    return Path(__file__).resolve().parents[1] / "protocols"
