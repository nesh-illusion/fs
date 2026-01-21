from lucius_orchestrator.ledger.memory_store import MemoryJobIndexStore
from lucius_orchestrator.ledger.models import JobIndex


def test_job_index_lookup_round_trip():
    store = MemoryJobIndexStore()
    record = JobIndex(
        job_id="job123",
        tenant_id="tenantA",
        tenant_bucket="tenantA#202601",
        created_at="2026-01-16T00:00:00Z",
    )
    store.put(record)
    found = store.get("job123")
    assert found is not None
    assert found.tenant_bucket == "tenantA#202601"
