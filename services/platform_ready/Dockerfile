FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY services/platform_ready/app.py .

CMD ["python", "app.py"]
