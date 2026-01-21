FROM python:3.11-slim

WORKDIR /app
COPY services/sis/pyproject.toml /app/
COPY services/shared /app/shared
RUN pip install --no-cache-dir .
COPY services/sis/app /app/app

ENV PYTHONPATH=/app

CMD ["lucius-sis"]
