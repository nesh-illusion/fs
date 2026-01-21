from __future__ import annotations

import sys
from pathlib import Path

SERVICES_ROOT = Path(__file__).resolve().parents[1] / "services"
service_paths = [
    SERVICES_ROOT,
    SERVICES_ROOT / "lucius-orchestrator" / "src",
    SERVICES_ROOT / "lucius-dispatcher" / "src",
    SERVICES_ROOT / "distributed-ocr",
    SERVICES_ROOT / "vector-ingestion",
    SERVICES_ROOT / "semantic-intelligence",
]

for service_path in service_paths:
    if str(service_path) not in sys.path:
        sys.path.insert(0, str(service_path))
