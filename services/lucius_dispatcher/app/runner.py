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


def loop(partition_key: str, interval: float, publish) -> None:
    service = _build_service()
    while True:
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
    loop_parser.add_argument("--partition", required=True)
    loop_parser.add_argument("--interval", type=float, default=10.0)

    args = parser.parse_args()

    if args.command == "dispatch":
        settings = AppSettings.from_env()
        dispatch_once(args.partition, args.limit, _publisher(settings))
    elif args.command == "sweep":
        sweep_once(args.job_id, args.tenant, args.bucket)
    elif args.command == "loop":
        settings = AppSettings.from_env()
        loop(args.partition, args.interval, _publisher(settings))


if __name__ == "__main__":
    main()
