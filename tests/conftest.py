from __future__ import annotations

import sys
from pathlib import Path

SERVICES_ROOT = Path(__file__).resolve().parents[1] / "services"
if str(SERVICES_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICES_ROOT))

for service in ("ocr", "embedding", "sis"):
    service_path = SERVICES_ROOT / service
    if str(service_path) not in sys.path:
        sys.path.insert(0, str(service_path))
