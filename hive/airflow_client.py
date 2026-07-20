"""Lectures Hive utilisees par le DAG Airflow, sans fichier fallback local."""

from __future__ import annotations

import os
from typing import Mapping


def hive_connection_settings(environment: Mapping[str, str] | None = None) -> dict[str, str]:
    env = os.environ if environment is None else environment
    return {
        "host": env.get("HIVE_DOCKER_HOST", "hive-server"),
        "port": env.get("HIVE_PORT", "10000"),
        "username": env.get("HIVE_USERNAME", "hive"),
        "database": "secur_sn",
    }


def _connection(settings: Mapping[str, str]):
    from pyhive import hive

    return hive.Connection(
        host=settings["host"],
        port=int(settings["port"]),
        username=settings["username"],
        database=settings["database"],
    )


def refresh_analytics_partitions() -> None:
    """Rend les derniers dossiers HDFS Gold visibles dans Hive."""
    connection = _connection(hive_connection_settings())
    try:
        cursor = connection.cursor()
        cursor.execute("MSCK REPAIR TABLE hotspots_24h_historique")
    finally:
        connection.close()


def analytics_risk_summary() -> dict[str, float | int]:
    """Lit la vue 24 h qui pilote la mise a jour ou le reentrainement ML."""
    connection = _connection(hive_connection_settings())
    try:
        cursor = connection.cursor()
        cursor.execute(
            """SELECT
              COALESCE(SUM(CASE WHEN niveau_risque = 'ROUGE' THEN 1 ELSE 0 END), 0),
              COALESCE(COUNT(*), 0),
              COALESCE(MAX(score_risque), 0.0)
            FROM vue_hotspots_24h"""
        )
        red_cells, total_cells, max_score = cursor.fetchone()
        return {"red_cells": int(red_cells), "total_cells": int(total_cells), "max_score": float(max_score)}
    finally:
        connection.close()
