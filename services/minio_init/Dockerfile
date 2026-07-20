FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY services/minio_init/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/minio_init/app.py .

CMD ["python", "app.py"]
