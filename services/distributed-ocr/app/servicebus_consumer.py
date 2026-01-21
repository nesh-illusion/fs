import json
import os
import time

from app.consumer import handle_message


def _message_body(message) -> str:
    body = getattr(message, "body", None)
    if body is None:
        return str(message)
    if isinstance(body, bytes):
        return body.decode("utf-8")
    if isinstance(body, str):
        return body
    return b"".join(body).decode("utf-8")


def consume_forever(connection_string: str, topic: str, subscription: str) -> None:
    try:
        from azure.servicebus import ServiceBusClient
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("azure-servicebus dependency is not installed") from exc

    max_delivery = int(os.getenv("SERVICEBUS_MAX_DELIVERY", "5"))
    backoff_seconds = float(os.getenv("SERVICEBUS_RETRY_BACKOFF", "2"))

    with ServiceBusClient.from_connection_string(connection_string) as client:
        receiver = client.get_subscription_receiver(topic_name=topic, subscription_name=subscription)
        with receiver:
            while True:
                messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                if not messages:
                    time.sleep(1)
                    continue
                for message in messages:
                    try:
                        payload = json.loads(_message_body(message))
                        handle_message(payload)
                        receiver.complete_message(message)
                    except Exception as exc:
                        delivery = getattr(message, "delivery_count", 0)
                        if delivery >= max_delivery:
                            receiver.dead_letter_message(
                                message,
                                reason="MAX_DELIVERY_EXCEEDED",
                                error_description=str(exc),
                            )
                        else:
                            receiver.abandon_message(message)
                            time.sleep(backoff_seconds)
