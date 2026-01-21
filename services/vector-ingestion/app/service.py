from .consumer import handle_message


SERVICE_NAME = "embedding"


def handle(payload: dict) -> None:
    handle_message(payload)
