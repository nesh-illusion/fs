import asyncio
import json
from dataclasses import dataclass
from typing import Protocol

from ledger.models import OutboxEntry


class Publisher(Protocol):
    async def publish(self, entry: OutboxEntry) -> None:
        ...


@dataclass
class NoopPublisher:
    async def publish(self, entry: OutboxEntry) -> None:
        return None


@dataclass
class AsyncServiceBusPublisher:
    connection_string: str
    timeout_seconds: float

    async def publish(self, entry: OutboxEntry) -> None:
        try:
            from azure.servicebus.aio import ServiceBusClient
            from azure.servicebus import ServiceBusMessage
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("azure-servicebus dependency is not installed") from exc

        message = ServiceBusMessage(json.dumps(entry.payload))
        message.message_id = f"{entry.job_id}:{entry.step_id}:{entry.attempt_no}"
        async with ServiceBusClient.from_connection_string(self.connection_string) as client:
            sender = client.get_topic_sender(topic_name=entry.topic)
            async with sender:
                await asyncio.wait_for(sender.send_messages(message), timeout=self.timeout_seconds)
