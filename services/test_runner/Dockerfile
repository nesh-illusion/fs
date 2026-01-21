FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir pytest fastapi jsonschema httpx requests

ENV PYTHONPATH=/app/services:/app/services/lucius-orchestrator/src:/app/services/lucius-dispatcher/src:/app/services/distributed-ocr/src:/app/services/vector-ingestion/src:/app/services/semantic-intelligence/src

CMD ["pytest", "-q", "-p", "no:cacheprovider", "services/lucius-orchestrator/tests/integration"]
