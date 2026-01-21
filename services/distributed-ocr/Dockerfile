FROM python:3.11-slim

WORKDIR /app
COPY services/distributed-ocr/pyproject.toml /app/
COPY services/shared /app/shared
RUN pip install --no-cache-dir .
COPY services/distributed-ocr/app /app/app

ENV PYTHONPATH=/app

CMD ["lucius-ocr"]
