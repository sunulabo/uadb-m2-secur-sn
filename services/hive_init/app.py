#!/usr/bin/env python3
"""Initialise puis actualise le catalogue Hive des sorties HDFS Gold."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Mapping

from catalog import catalog_statements, hive_settings, repair_statements


def execute_statements(cursor: Any, statements: list[str]) -> None:
    for statement in statements:
        cursor.execute(statement)


def sync_catalog(settings: Mapping[str, str]) -> None:
    from pyhive import hive

    connection = hive.Connection(host=settings["hive_host"], port=int(settings["hive_port"]), username="hive")
    cursor = connection.cursor()
    try:
        execute_statements(cursor, catalog_statements(settings))
        execute_statements(cursor, repair_statements())
    finally:
        cursor.close()
        connection.close()


def run() -> int:
    settings = hive_settings()
    refresh_seconds = float(settings["catalog_refresh_seconds"])
    logging.info("Hive init actif: HDFS Gold -> tables externes et vues")
    while True:
        try:
            sync_catalog(settings)
            logging.info("Catalogue Hive actualise")
            time.sleep(refresh_seconds)
        except Exception as exc:  # pragma: no cover - depend de Hive live
            logging.warning("Hive indisponible (%s), nouvelle tentative dans 10s", exc)
            time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(run())
