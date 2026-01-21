FROM python:3.11-slim

WORKDIR /app

COPY services/lucius_orchestrator /app/lucius_orchestrator
COPY services/lucius_dispatcher /app/lucius_dispatcher
COPY services/lucius_orchestrator/requirements.txt /app/requirements.txt

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["uvicorn", "lucius_orchestrator.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
