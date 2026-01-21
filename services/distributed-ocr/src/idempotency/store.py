import os
from typing import Protocol

from models.directive_model import Directive


class IdempotencyStore(Protocol):
    def seen(self, directive: Directive) -> bool:
        ...

    def mark_seen(self, directive: Directive) -> None:
        ...


class MemoryIdempotencyStore:
    def __init__(self) -> None:
        self._seen = set()

    def _key(self, directive: Directive) -> str:
        return f"{directive.jobId}:{directive.stepId}:{directive.attempt_no}:{directive.lease_id}"

    def seen(self, directive: Directive) -> bool:
        return self._key(directive) in self._seen

    def mark_seen(self, directive: Directive) -> None:
        self._seen.add(self._key(directive))


class TableIdempotencyStore:
    def __init__(self, connection_string: str, table_name: str) -> None:
        try:
            from azure.data.tables import TableServiceClient
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("azure-data-tables dependency is not installed") from exc
        service = TableServiceClient.from_connection_string(connection_string)
        self._table = service.get_table_client(table_name)

    def _pk(self, directive: Directive) -> str:
        return directive.jobId

    def _rk(self, directive: Directive) -> str:
        return f"{directive.stepId}:{directive.attempt_no}:{directive.lease_id}"

    def seen(self, directive: Directive) -> bool:
        try:
            self._table.get_entity(partition_key=self._pk(directive), row_key=self._rk(directive))
            return True
        except Exception:
            return False

    def mark_seen(self, directive: Directive) -> None:
        entity = {
            "PartitionKey": self._pk(directive),
            "RowKey": self._rk(directive),
            "jobId": directive.jobId,
            "stepId": directive.stepId,
            "attempt_no": directive.attempt_no,
            "lease_id": directive.lease_id,
        }
        self._table.create_entity(entity)


_MEMORY_STORE = MemoryIdempotencyStore()


def build_idempotency_store() -> IdempotencyStore:
    backend = os.getenv("IDEMPOTENCY_BACKEND", "memory").lower()
    if backend == "table":
        connection = os.getenv("IDEMPOTENCY_TABLE_CONNECTION")
        table = os.getenv("IDEMPOTENCY_TABLE_NAME", "platform-idempotency")
        if not connection:
            raise RuntimeError("IDEMPOTENCY_TABLE_CONNECTION is required for table backend")
        return TableIdempotencyStore(connection, table)
    return _MEMORY_STORE
