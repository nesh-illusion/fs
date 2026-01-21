from dataclasses import dataclass


@dataclass
class ErrorDetail:
    code: str
    message: str


class FailureClass:
    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"
