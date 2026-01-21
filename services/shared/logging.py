import json
import logging
import os
from typing import Any, Dict


def get_logger(name: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger(name)


def log_event(logger: logging.Logger, message: str, **fields: Any) -> None:
    payload = {"event": message, **fields}
    logger.info(json.dumps(payload, sort_keys=True))
