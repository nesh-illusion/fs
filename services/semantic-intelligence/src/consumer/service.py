from consumer import handle_message


SERVICE_NAME = "sis"


def handle(payload: dict) -> None:
    handle_message(payload)
