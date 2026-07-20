#!/usr/bin/env python3
"""Simulateur terrain: ecrit en continu des JSONL bruts dans MinIO."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urlparse

from minio import Minio


ZONES = ["DAKAR_PLATEAU", "PIKINE", "GUEDIAWAYE", "THIES", "MBOUR", "KAOLACK", "SAINT_LOUIS", "ZIGUINCHOR"]
INCIDENT_TYPES = ["ACCIDENT_LEGER", "ACCIDENT_GRAVE", "ACCIDENT_MORTEL", "EMBOUTEILLAGE_CRITIQUE"]
VEHICLE_TYPES = ["MOTO_JAKARTA", "CAR_RAPIDE", "CAMION", "VOITURE", "BUS", "CHARRETTE"]


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


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


def wait_for_bucket(client: Minio, bucket: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            if client.bucket_exists(bucket):
                print(f"Bucket terrain pret: {bucket}", flush=True)
                return
        except Exception as exc:  # pragma: no cover - depends on live MinIO
            last_error = str(exc)
        print("Bucket MinIO indisponible, nouvelle tentative dans 2s...", flush=True)
        time.sleep(2)
    raise RuntimeError(f"Bucket {bucket} indisponible apres {timeout_seconds}s: {last_error}")


def put_jsonl(client: Minio, bucket: str, object_name: str, payload: dict[str, object]) -> None:
    raw = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    client.put_object(
        bucket,
        object_name,
        BytesIO(raw),
        length=len(raw),
        content_type="application/x-ndjson",
    )


def incident_record(index: int, timestamp: str) -> dict[str, object]:
    return {
        "facteur_heure": 1.0,
        "heure": int(datetime.now(timezone.utc).strftime("%H")),
        "incident_id": f"FIELD-{index:06d}",
        "latitude": float(f"14.{700000 + (index % 99999):06d}"),
        "longitude": -float(f"17.{300000 + (index % 99999):06d}"),
        "nb_victimes": index % 5,
        "nom_victime": f"VICTIME_FIELD_{index:06d}",
        "tel_temoin": f"+221 77 {100 + (index % 900):03d} {10 + (index % 89):02d} {10 + ((index * 7) % 89):02d}",
        "timestamp": timestamp,
        "type_incident": INCIDENT_TYPES[index % len(INCIDENT_TYPES)],
        "type_vehicule": VEHICLE_TYPES[index % len(VEHICLE_TYPES)],
        "zone": ZONES[index % len(ZONES)],
    }


def meteo_record(index: int, timestamp: str) -> dict[str, object]:
    rain = (index * 3) % 18
    return {
        "pluie_mm": float(rain),
        "route_mouillee": rain > 0,
        "temperature": float(24 + (index % 14)),
        "timestamp": timestamp,
        "visibilite": "MOYENNE" if rain > 0 else "BONNE",
        "zone": ZONES[index % len(ZONES)],
    }


def main() -> int:
    bucket = os.getenv("MINIO_LANDING_BUCKET", "secur-sn-landing")
    interval = env_int("FIELD_SIMULATOR_INTERVAL_SECONDS", 10)
    max_batches = env_int("FIELD_SIMULATOR_MAX_BATCHES", 0)
    client = minio_client()
    wait_for_bucket(client, bucket, env_int("MINIO_READY_TIMEOUT_SECONDS", 120))

    index = 0
    while True:
        if max_batches > 0 and index >= max_batches:
            print(f"Simulation terminee apres {max_batches} lots.", flush=True)
            return 0

        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        day_path = now.strftime("%Y/%m/%d")
        zone = ZONES[index % len(ZONES)]

        put_jsonl(client, bucket, f"incidents/{day_path}/incident_{index}.jsonl", incident_record(index, timestamp))
        put_jsonl(client, bucket, f"meteo/{day_path}/meteo_{index}.jsonl", meteo_record(index, timestamp))
        print(f"Lot {index} publie dans MinIO: incidents + meteo ({zone})", flush=True)

        index += 1
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
