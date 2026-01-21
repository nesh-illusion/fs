import argparse
import time

from lucius_orchestrator.config.settings import AppSettings

from .publisher import NoopPublisher, ServiceBusPublisher
from .service import ReconciliationConfig, ReconciliationService
from .stores import build_stores


def _publisher(settings: AppSettings):
    if settings.service_bus_connection:
        return ServiceBusPublisher(settings.service_bus_connection)
    return NoopPublisher()


def _build_service() -> ReconciliationService:
    settings = AppSettings.from_env()
    jobs_store, steps_store, outbox_store, _, _, _ = build_stores(settings)
    config = ReconciliationConfig()
    return ReconciliationService(jobs_store, steps_store, outbox_store, config)


def dispatch_once(partition_key: str, limit: int, publish) -> None:
    service = _build_service()
    service._config.outbox_batch_size = limit
    service.dispatch_partition(partition_key, publish)


def sweep_once(job_id: str, tenant_id: str, tenant_bucket: str) -> None:
    service = _build_service()
    service.sweep_job(job_id, tenant_id, tenant_bucket)


def _partitions_for_shard(shard_index: int, shard_count: int) -> list[str]:
    return [f"p{lane}" for lane in range(16) if lane % shard_count == shard_index]


def loop(partition_key: str | None, interval: float, publish, shard_index: int | None, shard_count: int | None) -> None:
    service = _build_service()
    while True:
        if shard_index is not None and shard_count is not None:
            for partition in _partitions_for_shard(shard_index, shard_count):
                service.dispatch_partition(partition, publish)
        else:
            if partition_key is None:
                raise RuntimeError("partition_key is required when no shard is configured")
            service.dispatch_partition(partition_key, publish)
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="LUCIUS reconciliation runner")
    sub = parser.add_subparsers(dest="command", required=True)

    dispatch_parser = sub.add_parser("dispatch")
    dispatch_parser.add_argument("--partition", required=True)
    dispatch_parser.add_argument("--limit", type=int, default=100)

    sweep_parser = sub.add_parser("sweep")
    sweep_parser.add_argument("--job-id", required=True)
    sweep_parser.add_argument("--tenant", required=True)
    sweep_parser.add_argument("--bucket", required=True)

    loop_parser = sub.add_parser("loop")
    loop_group = loop_parser.add_mutually_exclusive_group(required=True)
    loop_group.add_argument("--partition")
    loop_group.add_argument("--shard-index", type=int)
    loop_parser.add_argument("--shard-count", type=int)
    loop_parser.add_argument("--interval", type=float, default=10.0)

    args = parser.parse_args()

    if args.command == "dispatch":
        settings = AppSettings.from_env()
        dispatch_once(args.partition, args.limit, _publisher(settings))
    elif args.command == "sweep":
        sweep_once(args.job_id, args.tenant, args.bucket)
    elif args.command == "loop":
        settings = AppSettings.from_env()
        if args.shard_index is not None and args.shard_count is None:
            raise RuntimeError("shard-count is required with shard-index")
        loop(args.partition, args.interval, _publisher(settings), args.shard_index, args.shard_count)


if __name__ == "__main__":
    main()
