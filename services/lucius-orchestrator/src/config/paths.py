from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def docs_root() -> Path:
    return project_root() / "docs"


def contracts_root() -> Path:
    return docs_root() / "contracts"


def step_schema_root() -> Path:
    return docs_root() / "schemas" / "steps"
