from typing import Any


def read_input(input_ref: Any) -> bytes:
    raise NotImplementedError("implement storage read based on input_ref")


def write_workspace(workspace_ref: Any, path: str, data: bytes) -> None:
    raise NotImplementedError("implement storage write based on workspace_ref")
