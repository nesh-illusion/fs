import json
from pathlib import Path
from typing import Any, Dict, Optional

from lucius_orchestrator.config.paths import contracts_root, step_schema_root


try:
    import jsonschema
except ImportError:  # pragma: no cover - dependency not installed yet
    jsonschema = None


class SchemaValidator:
    def __init__(self, contracts_dir: Optional[Path] = None, steps_dir: Optional[Path] = None):
        self.contracts_dir = contracts_dir or contracts_root()
        self.steps_dir = steps_dir or step_schema_root()
        self._cache: Dict[str, Dict[str, Any]] = {}

    def validate_envelope(self, envelope: Dict[str, Any]) -> None:
        schema = self._load_schema(self.contracts_dir / "command-envelope.v1.schema.json")
        self._validate(envelope, schema)

    def validate_step_payload(self, payload: Dict[str, Any], schema_ref: str) -> None:
        schema_path = self._resolve_schema_ref(schema_ref)
        schema = self._load_schema(schema_path)
        self._validate(payload, schema)

    def validate_protocol_payloads(
        self, payload: Dict[str, Any], steps: Dict[str, str]
    ) -> None:
        if isinstance(payload, dict) and "steps" in payload:
            payload_by_step = payload["steps"]
        else:
            payload_by_step = None

        for step_id, schema_ref in steps.items():
            step_payload = payload_by_step.get(step_id, {}) if payload_by_step else payload
            self.validate_step_payload(step_payload, schema_ref)

    def _resolve_schema_ref(self, schema_ref: str) -> Path:
        return self.steps_dir / Path(schema_ref).name

    def _load_schema(self, path: Path) -> Dict[str, Any]:
        cache_key = str(path)
        if cache_key in self._cache:
            return self._cache[cache_key]
        with path.open("r", encoding="utf-8") as handle:
            schema = json.load(handle)
        self._cache[cache_key] = schema
        return schema

    def _validate(self, instance: Dict[str, Any], schema: Dict[str, Any]) -> None:
        if jsonschema is None:
            raise RuntimeError("jsonschema dependency is not installed")
        jsonschema.validate(instance=instance, schema=schema)
