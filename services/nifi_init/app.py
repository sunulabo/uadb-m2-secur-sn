#!/usr/bin/env python3
"""Service d'initialisation du flow NiFi Secur-SN."""

from __future__ import annotations

import os
import time

from setup_nifi_minio_kafka_flow import MinioNifiClient, create_flow


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def configure_flow_with_retry(
    client: MinioNifiClient,
    bootstrap: str,
    bucket: str,
    endpoint: str,
    region: str,
    access_key: str,
    secret_key: str,
    timeout_seconds: int,
    retry_seconds: int,
) -> str:
    """Attend la stabilisation de l'API NiFi, meme apres son premier HTTP 200."""
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            return create_flow(client, bootstrap, bucket, endpoint, region, access_key, secret_key)
        except Exception as exc:  # pragma: no cover - depend du demarrage NiFi Docker
            last_error = str(exc)
            print(f"NiFi pas encore stabilise ({last_error}), nouvelle tentative dans {retry_seconds}s", flush=True)
            time.sleep(retry_seconds)
    raise RuntimeError(f"Configuration du flow NiFi impossible apres attente: {last_error}")


def main() -> int:
    url = os.getenv("NIFI_API_URL", "http://nifi:8081/nifi-api")
    bootstrap = os.getenv("KAFKA_INTERNAL_BOOTSTRAP_SERVERS", "kafka:19092")
    bucket = os.getenv("MINIO_LANDING_BUCKET", "secur-sn-landing")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    region = os.getenv("MINIO_REGION", "us-east-1")
    access_key = os.getenv("MINIO_ROOT_USER", "securadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "securadmin123")

    client = MinioNifiClient(url)
    # Une recuperation NiFi apres un arret brutal peut aussi reconstruire ses depots persistants.
    client.wait_until_ready(timeout_seconds=env_int("NIFI_READY_TIMEOUT_SECONDS", 1200))
    group_id = configure_flow_with_retry(
        client,
        bootstrap,
        bucket,
        endpoint,
        region,
        access_key,
        secret_key,
        env_int("NIFI_FLOW_TIMEOUT_SECONDS", 900),
        env_int("NIFI_FLOW_RETRY_SECONDS", 10),
    )

    print(f"Flow NiFi MinIO Kafka pret: {group_id}", flush=True)
    print(f"NiFi API: {url}", flush=True)
    print(f"MinIO bucket: {bucket}", flush=True)
    print(f"Kafka brokers: {bootstrap}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
