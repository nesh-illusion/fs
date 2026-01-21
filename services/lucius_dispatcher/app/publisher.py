import json
from dataclasses import dataclass
from typing import Any, Dict, Protocol

from lucius_orchestrator.ledger.models import OutboxEntry


class Publisher(Protocol):
    def publish(self, entry: OutboxEntry) -> None:
        ...


@dataclass
class NoopPublisher:
    def publish(self, entry: OutboxEntry) -> None:
        return None


@dataclass
class ServiceBusPublisher:
    connection_string: str

    def publish(self, entry: OutboxEntry) -> None:
        try:
            from azure.servicebus import ServiceBusClient, ServiceBusMessage
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("azure-servicebus dependency is not installed") from exc

        message_body: Dict[str, Any] = entry.payload
        message = ServiceBusMessage(json.dumps(message_body))
        with ServiceBusClient.from_connection_string(self.connection_string) as client:
            sender = client.get_topic_sender(topic_name=entry.topic)
            with sender:
                sender.send_messages(message)
