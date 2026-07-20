#!/usr/bin/env python3
"""Initialise les topics Kafka Secur-SN."""

from __future__ import annotations

import os
import time
from typing import Iterable

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


DEFAULT_TOPICS = (
    "secur_incidents_raw",
    "secur_meteo",
    "secur_incidents_dlq",
    "secur_meteo_dlq",
)
DEFAULT_BOOTSTRAP = "kafka:19092"
DEFAULT_TOPIC_PARTITIONS = 3
DEFAULT_TOPIC_REPLICATION_FACTOR = 1
DEFAULT_TOPIC_MIN_ISR = "1"
DEFAULT_READY_TIMEOUT_SECONDS = 600


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def topic_names() -> list[str]:
    raw = os.getenv("KAFKA_TOPICS", ",".join(DEFAULT_TOPICS))
    return [topic.strip() for topic in raw.split(",") if topic.strip()]


def admin_client() -> AdminClient:
    bootstrap = os.getenv("KAFKA_INTERNAL_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    return AdminClient(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "secur-sn-kafka-init",
            "socket.timeout.ms": 5000,
            "request.timeout.ms": 5000,
        }
    )


def wait_for_cluster(client: AdminClient, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            metadata = client.list_topics(timeout=5)
            print(f"Kafka pret: {len(metadata.brokers)} broker(s) detecte(s)", flush=True)
            return
        except Exception as exc:  # pragma: no cover - depends on live Kafka
            last_error = str(exc)
            print("Kafka indisponible, nouvelle tentative dans 2s...", flush=True)
            time.sleep(2)
    raise RuntimeError(f"Kafka indisponible apres {timeout_seconds}s: {last_error}")


def existing_topics(client: AdminClient) -> set[str]:
    metadata = client.list_topics(timeout=10)
    return set(metadata.topics)


def create_missing_topics(client: AdminClient, names: Iterable[str]) -> None:
    partitions = env_int("KAFKA_TOPIC_PARTITIONS", DEFAULT_TOPIC_PARTITIONS)
    replication_factor = env_int("KAFKA_TOPIC_REPLICATION_FACTOR", DEFAULT_TOPIC_REPLICATION_FACTOR)
    min_isr = os.getenv("KAFKA_TOPIC_MIN_ISR", DEFAULT_TOPIC_MIN_ISR)
    retention_ms = os.getenv("KAFKA_TOPIC_RETENTION_MS", "604800000")

    existing = existing_topics(client)
    topics = [
        NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config={
                "min.insync.replicas": min_isr,
                "retention.ms": retention_ms,
            },
        )
        for name in names
        if name not in existing
    ]

    if not topics:
        print("Tous les topics Secur-SN existent deja.", flush=True)
        return

    futures = client.create_topics(topics, request_timeout=30)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic cree: {topic}", flush=True)
        except KafkaException as exc:
            # Race tolerated: another init run may have created it first.
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"Topic deja existant: {topic}", flush=True)
            else:
                raise


def main() -> int:
    timeout = env_int("KAFKA_READY_TIMEOUT_SECONDS", DEFAULT_READY_TIMEOUT_SECONDS)
    client = admin_client()
    wait_for_cluster(client, timeout)
    create_missing_topics(client, topic_names())
    print("Topics disponibles:", ", ".join(sorted(existing_topics(client))), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
