import json
import os
import time

from app.consumer import handle_message
from app.servicebus_consumer import consume_forever


def main() -> None:
    payload = os.getenv("DIRECTIVE_JSON")
    if payload:
        handle_message(json.loads(payload))
        return

    connection = os.getenv("SERVICEBUS_CONNECTION")
    topic = os.getenv("SERVICEBUS_TOPIC")
    subscription = os.getenv("SERVICEBUS_SUBSCRIPTION")
    if connection and topic and subscription:
        consume_forever(connection, topic, subscription)
        return

    while True:
        time.sleep(5)
