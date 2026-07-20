"""Ecriture HBase directe depuis les executors Spark.

Ce module ne depend ni de Kafka ni de PySpark. Il est donc testable localement
et importe par chaque executor uniquement au moment de l'ecriture Thrift.
"""

from __future__ import annotations

import os
import time
from typing import Any, Iterable, Mapping
from urllib.error import HTTPError
from urllib.request import Request, urlopen


FORBIDDEN_PII_FIELDS = {
    "incident_id",
    "nom_victime",
    "tel_temoin",
    "telephone_temoin",
    "numero_temoin",
    "phone_temoin",
    "nom_complet_victime",
    "prenom_victime",
    "cin_victime",
}

TABLES = {
    "secur:incidents_temps_reel": {
        "meta": {"time_to_live": 86400, "max_versions": 1},
        "stats": {"time_to_live": 86400, "max_versions": 1},
        "risk": {"time_to_live": 86400, "max_versions": 1},
    },
    "secur:hotspots": {
        "meta": {"max_versions": 1},
        "stats": {"max_versions": 1},
        "risk": {"max_versions": 1},
    },
    "secur:stats_zone": {
        "meta": {"max_versions": 1},
        "stats": {"max_versions": 1},
        "risk": {"max_versions": 1},
    },
}

DEFAULT_HBASE_SETTINGS = {
    "hbase_host": "hbase",
    "hbase_port": "9090",
    "hbase_rest_url": "http://hbase:8080",
    "hbase_rest_timeout_seconds": "30",
    "ready_timeout_seconds": "600",
    "retry_seconds": "5",
}

TABLE_NAMESPACE = "secur"


def hbase_settings(environment: Mapping[str, str] | None = None) -> dict[str, str]:
    """Retourne la configuration HBase commune au driver et aux executors."""
    env = os.environ if environment is None else environment
    return {
        "hbase_host": env.get("HBASE_DOCKER_HOST", env.get("HBASE_HOST", DEFAULT_HBASE_SETTINGS["hbase_host"])),
        "hbase_port": env.get("HBASE_THRIFT_PORT", DEFAULT_HBASE_SETTINGS["hbase_port"]),
        "hbase_rest_url": env.get("HBASE_REST_URL", DEFAULT_HBASE_SETTINGS["hbase_rest_url"]),
        "hbase_rest_timeout_seconds": env.get(
            "HBASE_REST_TIMEOUT_SECONDS", DEFAULT_HBASE_SETTINGS["hbase_rest_timeout_seconds"]
        ),
        "ready_timeout_seconds": env.get(
            "HBASE_READY_TIMEOUT_SECONDS", DEFAULT_HBASE_SETTINGS["ready_timeout_seconds"]
        ),
        "retry_seconds": env.get("HBASE_RETRY_SECONDS", DEFAULT_HBASE_SETTINGS["retry_seconds"]),
    }


def normalized_key(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def assert_safe_payload(payload: Mapping[str, Any]) -> None:
    """Bloque les PII et leurs alias avant toute ecriture HBase."""
    leaked = sorted(key for key in payload if normalized_key(str(key)) in FORBIDDEN_PII_FIELDS)
    if leaked:
        raise ValueError(f"PII interdite dans la sortie HBase: {', '.join(leaked)}")


def as_bytes(value: Any) -> bytes:
    return str(value).encode("utf-8")


def non_null_cells(cells: Mapping[bytes, Any]) -> dict[bytes, bytes]:
    return {key: as_bytes(value) for key, value in cells.items() if value is not None}


def incident_cells(payload: Mapping[str, Any]) -> dict[bytes, bytes]:
    assert_safe_payload(payload)
    return non_null_cells(
        {
            b"meta:zone": payload.get("zone"),
            b"meta:grid_2km_id": payload.get("grid_2km_id"),
            b"meta:event_ts": payload.get("event_ts"),
            b"meta:processed_at": payload.get("processed_at"),
            b"meta:type_incident": payload.get("type_incident"),
            b"meta:type_vehicule": payload.get("type_vehicule"),
            b"meta:latitude": payload.get("latitude"),
            b"meta:longitude": payload.get("longitude"),
            b"stats:nb_victimes": payload.get("nb_victimes"),
            b"stats:heure": payload.get("heure"),
            b"risk:score_gravite": payload.get("score_gravite"),
            b"risk:score_vehicule": payload.get("score_vehicule"),
            b"risk:score_meteo": payload.get("score_meteo"),
            b"risk:score_risque": payload.get("score_risque"),
        }
    )


def hotspot_cells(payload: Mapping[str, Any]) -> dict[bytes, bytes]:
    assert_safe_payload(payload)
    return non_null_cells(
        {
            b"meta:zone": payload.get("zone"),
            b"meta:grid_2km_id": payload.get("grid_2km_id"),
            b"meta:window_start": payload.get("window_start"),
            b"meta:timestamp": payload.get("timestamp"),
            b"meta:processed_at": payload.get("processed_at"),
            b"meta:latitude": payload.get("latitude"),
            b"meta:longitude": payload.get("longitude"),
            b"stats:nb_incidents": payload.get("nb_incidents"),
            b"stats:nb_victimes": payload.get("nb_victimes"),
            b"stats:heure_critique": payload.get("heure_critique"),
            b"stats:batch_id": payload.get("batch_id"),
            b"risk:score_risque": payload.get("score_risque"),
            b"risk:niveau_risque": payload.get("niveau_risque"),
            b"risk:recommandation": payload.get("recommandation"),
        }
    )


def _row_dict(row: Any) -> dict[str, Any]:
    return row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)


def _connect(settings: Mapping[str, str]):
    import happybase

    connection = happybase.Connection(
        host=settings["hbase_host"],
        port=int(settings["hbase_port"]),
        timeout=10000,
        autoconnect=False,
    )
    connection.open()
    return connection


def hbase_namespace_url(settings: Mapping[str, str]) -> str:
    """Retourne l'endpoint REST officiel de creation du namespace Secur-SN."""
    return f"{settings['hbase_rest_url'].rstrip('/')}/namespaces/{TABLE_NAMESPACE}"


def ensure_namespace(settings: Mapping[str, str], opener=None) -> None:
    """Cree le namespace avant les tables; Thrift v1 ne fournit pas cette operation."""
    open_request = urlopen if opener is None else opener
    endpoint = hbase_namespace_url(settings)
    timeout = float(settings["hbase_rest_timeout_seconds"])
    try:
        with open_request(Request(endpoint, method="GET"), timeout=timeout):
            return
    except HTTPError as exc:
        # HBase 2.1 REST renvoie 500 (au lieu de 404) lorsqu'un namespace est absent.
        if exc.code not in {404, 500}:
            raise

    try:
        with open_request(Request(endpoint, data=b"", method="POST"), timeout=timeout):
            return
    except HTTPError as exc:
        if exc.code != 409:
            raise


def ensure_tables(settings: Mapping[str, str]) -> None:
    """Cree les tables une fois depuis le driver avant de lancer les requetes."""
    ensure_namespace(settings)
    connection = _connect(settings)
    try:
        existing = {
            name.decode("utf-8") if isinstance(name, bytes) else str(name)
            for name in connection.tables()
        }
        for name, families in TABLES.items():
            if name not in existing:
                connection.create_table(name, families)
    finally:
        connection.close()


def wait_for_hbase(settings: Mapping[str, str]) -> None:
    """Attend Thrift et initialise le schema sans faire avancer Kafka."""
    deadline = time.monotonic() + float(settings["ready_timeout_seconds"])
    last_error = ""
    while time.monotonic() < deadline:
        try:
            ensure_tables(settings)
            return
        except Exception as exc:  # pragma: no cover - depend du service Docker
            last_error = str(exc)
            print(f"HBase pas encore pret ({type(exc).__name__}: {last_error}), nouvelle tentative...", flush=True)
            time.sleep(float(settings["retry_seconds"]))
    raise RuntimeError(f"HBase indisponible apres attente: {last_error}")


def write_incident_partition(rows: Iterable[Any], settings: Mapping[str, str]) -> None:
    """Ecrit une partition Spark dans la table temps reel, cle idempotente."""
    connection = _connect(settings)
    try:
        batch = connection.table("secur:incidents_temps_reel").batch(batch_size=100)
        count = 0
        for row in rows:
            payload = _row_dict(row)
            incident_secure = str(payload.get("incident_secure", "")).strip()
            if not incident_secure:
                raise ValueError("incident_secure manquant pour HBase")
            batch.put(as_bytes(incident_secure), incident_cells(payload))
            count += 1
        if count:
            batch.send()
    finally:
        connection.close()


def _decode_int(row: Mapping[bytes, bytes], key: bytes) -> int:
    value = row.get(key)
    return int(value.decode("utf-8")) if value else 0


def _update_zone_stats(connection: Any, payload: Mapping[str, Any]) -> None:
    """Met a jour le cumul par zone; le flux hotspot est volontairement mono-partition."""
    hotspot_id = str(payload.get("hotspot_id", "")).strip()
    zone = str(payload.get("zone", "")).strip()
    if not hotspot_id or not zone:
        raise ValueError("hotspot_id ou zone manquant pour HBase")

    table = connection.table("secur:stats_zone")
    row_key = as_bytes(zone)
    previous = table.row(row_key)
    processed = {item for item in previous.get(b"meta:hotspot_ids", b"").decode("utf-8").split(",") if item}
    if hotspot_id in processed:
        return

    processed.add(hotspot_id)
    table.put(
        row_key,
        non_null_cells(
            {
                b"meta:zone": zone,
                b"meta:last_hotspot_id": hotspot_id,
                b"meta:last_timestamp": payload.get("timestamp"),
                b"meta:hotspot_ids": ",".join(sorted(processed)),
                b"stats:cumulative_incidents": _decode_int(previous, b"stats:cumulative_incidents")
                + int(payload.get("nb_incidents", 0)),
                b"stats:cumulative_victimes": _decode_int(previous, b"stats:cumulative_victimes")
                + int(payload.get("nb_victimes", 0)),
                b"stats:hotspot_count": _decode_int(previous, b"stats:hotspot_count") + 1,
                b"risk:last_score_risque": payload.get("score_risque"),
                b"risk:last_niveau_risque": payload.get("niveau_risque"),
            }
        ),
    )


def write_hotspot_partition(rows: Iterable[Any], settings: Mapping[str, str]) -> None:
    """Ecrit les hotspots et leurs statistiques par zone depuis Spark."""
    connection = _connect(settings)
    try:
        hotspots = connection.table("secur:hotspots")
        for row in rows:
            payload = _row_dict(row)
            hotspot_id = str(payload.get("hotspot_id", "")).strip()
            if not hotspot_id:
                raise ValueError("hotspot_id manquant pour HBase")
            hotspots.put(as_bytes(hotspot_id), hotspot_cells(payload))
            _update_zone_stats(connection, payload)
    finally:
        connection.close()
