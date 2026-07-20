#!/usr/bin/env python3
"""Genere un historique batch pour Hive, ML et dashboard."""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import (
    aggregate_alerts,
    build_alert,
    project_path,
    sample_incident,
    sample_meteo,
    write_csv,
    write_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=120)
    parser.add_argument("--seed", type=int, default=221)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    meteo_by_zone = {}
    meteo_rows = []
    incidents = []
    alerts = []

    for index in range(args.count):
        incident = sample_incident(index=index, rng=rng)
        meteo = meteo_by_zone.get(incident["zone"])
        if meteo is None or index % 8 == 0:
            meteo = sample_meteo(index=index, rng=rng)
            meteo["zone"] = incident["zone"]
            meteo_by_zone[incident["zone"]] = meteo
            meteo_rows.append(meteo)
        incidents.append(incident)
        alerts.append(build_alert(incident, meteo))

    hotspots = aggregate_alerts(alerts)
    write_jsonl(project_path("data", "samples", "incidents_sample.jsonl"), incidents[:20])
    write_jsonl(project_path("data", "samples", "meteo_sample.jsonl"), meteo_rows[:20])
    write_jsonl(project_path("data", "processed", "alerts_fallback.jsonl"), alerts)
    write_jsonl(project_path("data", "processed", "hotspots_fallback.jsonl"), hotspots)
    write_csv(project_path("data", "processed", "incidents_historique.csv"), alerts)
    write_csv(project_path("data", "processed", "hotspots_historique.csv"), hotspots)

    print(f"{len(incidents)} incidents, {len(alerts)} alertes et {len(hotspots)} hotspots generes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
