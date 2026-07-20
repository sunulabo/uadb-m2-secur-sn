#!/usr/bin/env python3
"""Producteur Kafka d'incidents GAMA/CETUD simules."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import load_env_file, project_path, sample_incident, write_jsonl


def create_kafka_producer(bootstrap_servers: str) -> Optional[Any]:
    """Cree un KafkaProducer si kafka-python est disponible."""
    try:
        from kafka import KafkaProducer
    except Exception:
        return None
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=50,
        )
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="secur_incidents_raw")
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--seed", type=int, default=221)
    args = parser.parse_args()

    load_env_file()
    bootstrap = args.bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    max_messages = args.max_messages or int(os.getenv("PRODUCER_MAX_MESSAGES", "30"))
    interval = args.interval if args.interval is not None else float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
    rng = random.Random(args.seed)

    producer = create_kafka_producer(bootstrap)
    rows = []
    for index in range(max_messages):
        record = sample_incident(index=index, rng=rng)
        rows.append(record)
        if producer:
            producer.send(args.topic, record)
        print(json.dumps(record, ensure_ascii=False))
        if interval > 0 and index < max_messages - 1:
            time.sleep(interval)

    if producer:
        producer.flush()
        producer.close()
    else:
        print("Kafka indisponible ou kafka-python absent: ecriture fallback JSONL.", file=sys.stderr)

    write_jsonl(project_path("data", "raw", "incidents_demo.jsonl"), rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
