from consumer import handle_message


SERVICE_NAME = "ocr"


def handle(payload: dict) -> None:
    handle_message(payload)
