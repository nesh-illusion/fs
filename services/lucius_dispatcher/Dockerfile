FROM python:3.11-slim

WORKDIR /app
COPY services/lucius_orchestrator /app/lucius_orchestrator
COPY services/lucius_dispatcher /app/lucius_dispatcher

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir /app/lucius_dispatcher

CMD ["lucius-reconciliation"]
