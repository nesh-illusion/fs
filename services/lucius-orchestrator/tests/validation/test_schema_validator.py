import pytest

from validation.validator import SchemaValidator


def test_envelope_validation_accepts_minimal_payload():
    validator = SchemaValidator()
    envelope = {
        "tenant_id": "t1",
        "request_type": "OCR",
        "input_ref": "https://example.com/input.pdf",
        "output_ref": "https://example.com/output.txt",
        "payload": {},
        "schema_version": "v1",
    }
    validator.validate_envelope(envelope)


def test_step_payload_validation_rejects_unknown_enum():
    validator = SchemaValidator()
    with pytest.raises(Exception):
        validator.validate_step_payload({"output_format": "XML"}, "schemas/steps/ocr.v1.json")
