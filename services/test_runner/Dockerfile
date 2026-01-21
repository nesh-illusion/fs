FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir pytest fastapi jsonschema httpx requests

ENV PYTHONPATH=/app/services:/app/services/distributed-ocr:/app/services/vector-ingestion:/app/services/semantic-intelligence:/app/services/lucius-orchestrator/src:/app/services/lucius-dispatcher/src

CMD ["pytest", "-q", "-p", "no:cacheprovider", "services/lucius-orchestrator/tests/integration"]
