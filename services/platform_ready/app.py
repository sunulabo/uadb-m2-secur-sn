#!/usr/bin/env python3
"""Attend la disponibilite effective de la plateforme avant Airflow."""

from __future__ import annotations

import json
import os
import socket
import time
from collections.abc import Callable
from pathlib import Path
from urllib.request import urlopen


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def wait_tcp(host: str, port: int, timeout: float = 3.0) -> None:
    with socket.create_connection((host, port), timeout=timeout):
        return


def wait_http(url: str, timeout: float = 5.0) -> None:
    with urlopen(url, timeout=timeout) as response:
        if not 200 <= response.status < 300:
            raise RuntimeError(f"HTTP {response.status}")


def wait_spark_streaming() -> None:
    wait_http("http://spark-streaming:4040/api/v1/applications")
    marker = Path("/opt/secur-sn/data/runtime/spark_streaming_ready.json")
    if not marker.exists():
        raise RuntimeError("marqueur des requetes streaming absent")
    status = json.loads(marker.read_text(encoding="utf-8"))
    if status.get("status") != "ready":
        raise RuntimeError("requetes streaming non pretes")


def checks() -> list[tuple[str, Callable[[], None]]]:
    return [
        ("Kafka", lambda: wait_tcp("kafka", 19092)),
        ("MinIO", lambda: wait_http("http://minio:9000/minio/health/live")),
        ("NiFi", lambda: wait_http("http://nifi:8081/nifi-api/flow/about")),
        ("Spark Structured Streaming", wait_spark_streaming),
        ("HDFS NameNode", lambda: wait_http("http://namenode:9870/jmx")),
        ("HBase Thrift", lambda: wait_tcp("hbase", 9090)),
        ("Hive Metastore", lambda: wait_tcp("hive-metastore", 9083)),
        ("HiveServer2", lambda: wait_tcp("hive-server", 10000)),
    ]


def main() -> int:
    timeout_seconds = env_int("PLATFORM_READY_TIMEOUT_SECONDS", 1200)
    retry_seconds = env_int("PLATFORM_READY_RETRY_SECONDS", 5)
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        failures: list[str] = []
        for name, check in checks():
            try:
                check()
            except Exception as exc:  # pragma: no cover - depend des services Docker
                failures.append(f"{name}: {type(exc).__name__}")

        if not failures:
            print("Plateforme Secur-SN prete : Airflow peut demarrer.", flush=True)
            return 0

        print(
            f"Plateforme pas encore prete ({', '.join(failures)}), nouvelle tentative dans {retry_seconds}s",
            flush=True,
        )
        time.sleep(retry_seconds)

    raise RuntimeError("Plateforme Secur-SN indisponible avant expiration du delai")


if __name__ == "__main__":
    raise SystemExit(main())
