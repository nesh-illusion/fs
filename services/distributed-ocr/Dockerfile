FROM python:3.11-slim

WORKDIR /app
COPY services/distributed-ocr/pyproject.toml /app/
COPY services/shared /app/shared
RUN pip install --no-cache-dir .
COPY services/distributed-ocr/src /app/src

ENV PYTHONPATH=/app/src:/app/shared

CMD ["lucius-ocr"]
