FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY nifi/setup_nifi_minio_kafka_flow.py .
COPY services/nifi_init/app.py .

CMD ["python", "app.py"]
