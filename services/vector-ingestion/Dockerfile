FROM python:3.11-slim

WORKDIR /app
COPY services/vector-ingestion/pyproject.toml /app/
COPY services/shared /app/shared
RUN pip install --no-cache-dir .
COPY services/vector-ingestion/src /app/src

ENV PYTHONPATH=/app/src:/app/shared

CMD ["lucius-embedding"]
