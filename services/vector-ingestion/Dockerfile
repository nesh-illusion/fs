FROM python:3.11-slim

WORKDIR /app
COPY services/vector-ingestion/pyproject.toml /app/
COPY services/shared /app/shared
RUN pip install --no-cache-dir .
COPY services/vector-ingestion/app /app/app

ENV PYTHONPATH=/app

CMD ["lucius-embedding"]
