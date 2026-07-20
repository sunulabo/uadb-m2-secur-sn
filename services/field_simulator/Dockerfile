FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY services/field_simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/field_simulator/app.py .

CMD ["python", "app.py"]
