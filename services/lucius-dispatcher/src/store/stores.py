from config.settings import AppSettings
from ledger.memory_store import (
    MemoryIdempotencyStore,
    MemoryJobIndexStore,
    MemoryJobsStore,
    MemoryOutboxStore,
    MemoryStepsStore,
    MemoryTenantInflightStore,
)
from ledger.table_storage import (
    TableIdempotencyStore,
    TableJobIndexStore,
    TableJobsStore,
    TableOutboxStore,
    TableStepsStore,
    TableTenantInflightStore,
)


def build_stores(settings: AppSettings):
    if settings.storage_backend == "memory":
        return (
            MemoryJobsStore(),
            MemoryStepsStore(),
            MemoryOutboxStore(),
            MemoryIdempotencyStore(),
            MemoryJobIndexStore(),
            MemoryTenantInflightStore(),
        )

    if settings.storage_backend != "table":
        raise RuntimeError(f"Unsupported storage backend: {settings.storage_backend}")

    if not settings.table_connection_string:
        raise RuntimeError("LUCIUS_TABLE_CONNECTION is required for table storage")

    from azure.data.tables import TableServiceClient

    service_client = TableServiceClient.from_connection_string(settings.table_connection_string)
    return (
        TableJobsStore(service_client, settings.jobs_table),
        TableStepsStore(service_client, settings.steps_table),
        TableOutboxStore(service_client, settings.outbox_table),
        TableIdempotencyStore(service_client, settings.idempotency_table),
        TableJobIndexStore(service_client, settings.job_index_table),
        TableTenantInflightStore(service_client, settings.inflight_table),
    )
