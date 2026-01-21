FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir pytest fastapi jsonschema httpx requests

ENV PYTHONPATH=/app/services:/app/services/ocr:/app/tests

CMD ["pytest", "-q", "-p", "no:cacheprovider", "tests/lucius_orchestrator/integration"]
