#!/usr/bin/env python3
"""Initialise les buckets MinIO Secur-SN."""

from __future__ import annotations

import os
import time
from urllib.parse import urlparse

from minio import Minio


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def bucket_names() -> list[str]:
    defaults = [
        os.getenv("MINIO_LANDING_BUCKET", "secur-sn-landing"),
        os.getenv("MINIO_QUARANTINE_BUCKET", "secur-sn-quarantine"),
    ]
    extra = os.getenv("MINIO_EXTRA_BUCKETS", "")
    return [bucket for bucket in defaults + [b.strip() for b in extra.split(",")] if bucket]


def minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    parsed = urlparse(endpoint)
    host = parsed.netloc or parsed.path
    return Minio(
        host,
        access_key=os.getenv("MINIO_ROOT_USER", "securadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "securadmin123"),
        secure=parsed.scheme == "https",
    )


def wait_for_minio(client: Minio, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            client.list_buckets()
            print("MinIO pret.", flush=True)
            return
        except Exception as exc:  # pragma: no cover - depends on live MinIO
            last_error = str(exc)
            print("MinIO indisponible, nouvelle tentative dans 2s...", flush=True)
            time.sleep(2)
    raise RuntimeError(f"MinIO indisponible apres {timeout_seconds}s: {last_error}")


def main() -> int:
    client = minio_client()
    wait_for_minio(client, env_int("MINIO_READY_TIMEOUT_SECONDS", 120))
    for bucket in bucket_names():
        if client.bucket_exists(bucket):
            print(f"Bucket deja existant: {bucket}", flush=True)
        else:
            client.make_bucket(bucket)
            print(f"Bucket cree: {bucket}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
