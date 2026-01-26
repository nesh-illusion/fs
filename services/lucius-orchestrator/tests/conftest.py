from __future__ import annotations

import sys
from pathlib import Path
import os

SERVICE_ROOT = Path(__file__).resolve().parents[1]
SERVICES_ROOT = SERVICE_ROOT.parent

paths = [
    SERVICE_ROOT / "src",
    SERVICES_ROOT / "distributed-ocr" / "src",
    SERVICES_ROOT,
]

for path in paths:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

os.environ.setdefault("LUCIUS_TEMPORAL_ENABLED", "false")
