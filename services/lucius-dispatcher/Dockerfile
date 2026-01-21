FROM python:3.11-slim

WORKDIR /app
COPY services/lucius-orchestrator /app/lucius-orchestrator
COPY services/lucius-dispatcher /app/lucius-dispatcher
COPY services/shared /app/shared

RUN pip install --no-cache-dir /app/lucius-dispatcher

ENV PYTHONPATH=/app/lucius-dispatcher/src:/app/lucius-orchestrator/src:/app/shared

CMD ["lucius-reconciliation"]
