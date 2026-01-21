from lucius_orchestrator.validation.idempotency import canonical_json, idempotency_hash


def test_canonical_json_strips_nulls_and_sorts():
    payload = {"b": 1, "a": None, "c": {"z": 2, "y": None}}
    assert canonical_json(payload) == '{"b":1,"c":{"z":2}}'


def test_idempotency_hash_stable_for_same_payload():
    payload = {"tenant_id": "t1", "request_type": "OCR"}
    assert idempotency_hash(payload) == idempotency_hash(payload)
