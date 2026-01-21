FROM python:3.11-slim

WORKDIR /app

COPY services/lucius-orchestrator /app/lucius-orchestrator
COPY services/lucius-dispatcher /app/lucius-dispatcher
COPY services/shared /app/shared
COPY services/lucius-orchestrator/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

ENV PYTHONPATH=/app/lucius-orchestrator/src:/app/lucius-dispatcher/src:/app/shared

CMD ["uvicorn", "api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
