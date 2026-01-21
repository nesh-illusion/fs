from __future__ import annotations

import sys
from pathlib import Path

SERVICE_ROOT = Path(__file__).resolve().parents[1]
SERVICES_ROOT = SERVICE_ROOT.parent

paths = [
    SERVICE_ROOT / "src",
    SERVICES_ROOT / "lucius-orchestrator" / "src",
    SERVICES_ROOT,
]

for path in paths:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
