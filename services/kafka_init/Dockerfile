FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY services/kafka_init/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/kafka_init/app.py .

CMD ["python", "app.py"]
