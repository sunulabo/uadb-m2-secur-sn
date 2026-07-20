"""Definition du catalogue Hive branche sur la zone Gold HDFS."""

from __future__ import annotations

import os
from typing import Mapping


DEFAULT_CATALOG_SETTINGS = {
    "catalog_refresh_seconds": "60",
    "hive_host": "hive-server",
    "hive_port": "10000",
    "hdfs_gold_root": "/secur-sn/gold",
    "hdfs_namenode_uri": "hdfs://namenode:8020",
}


def hive_settings(environment: Mapping[str, str] | None = None) -> dict[str, str]:
    """Retourne les parametres du catalogue sans importer un client Hive."""
    env = os.environ if environment is None else environment
    return {
        "catalog_refresh_seconds": env.get(
            "HIVE_CATALOG_REFRESH_SECONDS", DEFAULT_CATALOG_SETTINGS["catalog_refresh_seconds"]
        ),
        "hive_host": env.get("HIVE_DOCKER_HOST", DEFAULT_CATALOG_SETTINGS["hive_host"]),
        "hive_port": env.get("HIVE_PORT", DEFAULT_CATALOG_SETTINGS["hive_port"]),
        "hdfs_gold_root": env.get("HDFS_GOLD_ROOT", DEFAULT_CATALOG_SETTINGS["hdfs_gold_root"]),
        "hdfs_namenode_uri": env.get("HDFS_NAMENODE_URI", DEFAULT_CATALOG_SETTINGS["hdfs_namenode_uri"]),
    }


def gold_location(settings: Mapping[str, str], prefix: str) -> str:
    namenode_uri = settings["hdfs_namenode_uri"].rstrip("/")
    root = settings["hdfs_gold_root"].strip("/")
    clean_prefix = prefix.strip("/")
    if not namenode_uri.startswith("hdfs://"):
        raise ValueError(f"HDFS_NAMENODE_URI invalide: {namenode_uri}")
    if not root:
        raise ValueError("HDFS_GOLD_ROOT ne peut pas etre vide")
    return f"{namenode_uri}/{root}/{clean_prefix}" if clean_prefix else f"{namenode_uri}/{root}"

def catalog_statements(settings: Mapping[str, str]) -> list[str]:
    """Construit le DDL des tables Parquet externes et des vues de l'examen."""
    hotspots_24h = gold_location(settings, "hotspots_24h")
    return [
        "CREATE DATABASE IF NOT EXISTS secur_sn",
        "USE secur_sn",
        f"""CREATE EXTERNAL TABLE IF NOT EXISTS hotspots_24h_historique (
          hotspot_24h_id STRING,
          zone STRING,
          grid_2km_id STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          nb_incidents BIGINT,
          nb_victimes BIGINT,
          heure_critique INT,
          score_risque DOUBLE,
          niveau_risque STRING,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          processed_at TIMESTAMP
        )
        PARTITIONED BY (batch_id INT, snapshot_date STRING)
        STORED AS PARQUET
        LOCATION '{hotspots_24h}'""",
        """CREATE OR REPLACE VIEW vue_hotspots_24h AS
        WITH derniers_hotspots AS (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY hotspot_24h_id ORDER BY batch_id DESC, processed_at DESC
          ) AS rang
          FROM hotspots_24h_historique
        )
        SELECT hotspot_24h_id, zone, grid_2km_id, latitude, longitude,
               nb_incidents, nb_victimes, heure_critique, score_risque,
               niveau_risque, window_start, window_end
        FROM derniers_hotspots
        WHERE rang = 1
        ORDER BY score_risque DESC""",
        """CREATE OR REPLACE VIEW vue_recommandations_patrouilles AS
        SELECT zone, niveau_risque, score_risque, heure_critique,
               CASE WHEN niveau_risque = 'ROUGE' THEN 'Patrouille immediate et renfort CETUD'
                    WHEN niveau_risque = 'ORANGE' THEN 'Patrouille preventive ciblee'
                    ELSE 'Surveillance reguliere' END AS recommandation
        FROM vue_hotspots_24h
        WHERE niveau_risque IN ('ORANGE', 'ROUGE')
        ORDER BY score_risque DESC""",
    ]


def repair_statements() -> list[str]:
    return [
        "USE secur_sn",
        "MSCK REPAIR TABLE hotspots_24h_historique",
    ]
