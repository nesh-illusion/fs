import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from config.paths import docs_root


@dataclass
class ProtocolStep:
    step_id: str
    step_type: str
    service: str
    payload_schema_ref: str


@dataclass
class Protocol:
    protocol_id: str
    request_types: List[str]
    steps: List[ProtocolStep]


class ProtocolRegistry:
    def __init__(self, registry_path: Optional[Path] = None):
        self.registry_path = registry_path or (docs_root() / "protocols" / "protocol-registry.v1.example.json")
        self._by_request_type: Dict[str, Protocol] = {}
        self._load()

    def resolve(self, request_type: str) -> Optional[Protocol]:
        return self._by_request_type.get(request_type)

    def _load(self) -> None:
        with self.registry_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        protocols = data.get("protocols", [])
        for proto in protocols:
            steps = [
                ProtocolStep(
                    step_id=step["step_id"],
                    step_type=step["step_type"],
                    service=step["service"],
                    payload_schema_ref=step["payload_schema_ref"],
                )
                for step in proto.get("steps", [])
            ]
            protocol = Protocol(
                protocol_id=proto["protocol_id"],
                request_types=proto.get("request_types", []),
                steps=steps,
            )
            for request_type in protocol.request_types:
                self._by_request_type[request_type] = protocol
