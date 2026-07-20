FROM node:22-alpine AS frontend
WORKDIR /build
COPY dashboard/frontend/package*.json ./
RUN npm install
COPY dashboard/frontend ./
RUN npm run build

FROM python:3.11-slim
WORKDIR /opt/secur-sn
COPY services/dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY dashboard /opt/secur-sn/dashboard
COPY spark /opt/secur-sn/spark
COPY hive /opt/secur-sn/hive
COPY --from=frontend /build/dist /opt/secur-sn/dashboard/frontend/dist
ENV PYTHONPATH=/opt/secur-sn
EXPOSE 8050
CMD ["uvicorn", "dashboard.app.main:app", "--host", "0.0.0.0", "--port", "8050"]
