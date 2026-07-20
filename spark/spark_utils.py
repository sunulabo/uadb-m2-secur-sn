"""Fonctions communes pour validation, privacy, scoring et fichiers demo."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_PII_FIELDS = {"incident_id", "nom_victime", "tel_temoin"}
PII_FIELD_ALIASES = FORBIDDEN_PII_FIELDS | {
    "telephone_temoin",
    "numero_temoin",
    "phone_temoin",
    "nom_complet_victime",
    "prenom_victime",
    "cin_victime",
}
PII_METRIC_LABELS = {
    "incident_id": "identifiant_brut",
    "nom_victime": "nom_personne",
    "tel_temoin": "telephone_contact",
    "telephone_temoin": "telephone_contact",
    "numero_temoin": "telephone_contact",
    "phone_temoin": "telephone_contact",
    "nom_complet_victime": "nom_personne",
    "prenom_victime": "nom_personne",
    "cin_victime": "piece_identite",
}

ZONES = {
    "DAKAR_PLATEAU": (14.6928, -17.4467),
    "PIKINE": (14.7645, -17.3907),
    "GUEDIAWAYE": (14.7760, -17.3985),
    "THIES": (14.7910, -16.9359),
    "MBOUR": (14.4201, -16.9696),
    "KAOLACK": (14.1652, -16.0758),
    "SAINT_LOUIS": (16.0326, -16.4818),
    "ZIGUINCHOR": (12.5680, -16.2733),
}

INCIDENT_TYPES = [
    "ACCIDENT_LEGER",
    "ACCIDENT_GRAVE",
    "ACCIDENT_MORTEL",
    "EMBOUTEILLAGE_CRITIQUE",
]

VEHICLE_TYPES = [
    "MOTO_JAKARTA",
    "CAR_RAPIDE",
    "CAMION",
    "VOITURE",
    "BUS",
    "CHARRETTE",
]


def project_path(*parts: str) -> Path:
    """Retourne un chemin absolu dans le projet."""
    return PROJECT_ROOT.joinpath(*parts)


def load_env_file(env_path: Optional[Path] = None) -> None:
    """Charge un fichier .env simple sans dependance externe."""
    path = env_path or project_path(".env")
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def parse_timestamp(value: Any) -> datetime:
    """Parse un timestamp ISO et force UTC si necessaire."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("timestamp invalide")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def now_utc_iso() -> str:
    """Retourne l'heure UTC au format ISO stable."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_pandera_schema() -> Any:
    """Construit le schema Pandera si la dependance est disponible.

    Le fallback manuel reste actif pour les environnements de soutenance legers.
    """
    try:
        import pandera as pa
        from pandera import Check, Column, DataFrameSchema
    except Exception:
        return None

    return DataFrameSchema(
        {
            "incident_id": Column(str, nullable=False),
            "nom_victime": Column(str, nullable=False),
            "tel_temoin": Column(str, nullable=False),
            "zone": Column(str, Check.isin(list(ZONES.keys())), nullable=False),
            "type_incident": Column(str, Check.isin(INCIDENT_TYPES), nullable=False),
            "type_vehicule": Column(str, Check.isin(VEHICLE_TYPES), nullable=False),
            "latitude": Column(float, Check.in_range(12.0, 17.5), nullable=False),
            "longitude": Column(float, Check.in_range(-18.5, -11.0), nullable=False),
            "nb_victimes": Column(int, Check.ge(0), nullable=False),
            "heure": Column(int, Check.in_range(0, 23), nullable=False),
            "facteur_heure": Column(float, Check.in_range(0.5, 2.5), nullable=False),
            "timestamp": Column(str, nullable=False),
        }
    )


def validate_incident(record: Dict[str, Any]) -> Dict[str, Any]:
    """Valide et normalise un incident brut."""
    required = {
        "incident_id",
        "nom_victime",
        "tel_temoin",
        "zone",
        "type_incident",
        "type_vehicule",
        "latitude",
        "longitude",
        "nb_victimes",
        "heure",
        "facteur_heure",
        "timestamp",
    }
    missing = sorted(required.difference(record))
    if missing:
        raise ValueError(f"Champs manquants: {', '.join(missing)}")

    zone = str(record["zone"]).upper()
    if zone not in ZONES:
        raise ValueError(f"Zone inconnue: {zone}")

    incident_type = str(record["type_incident"]).upper()
    if incident_type not in INCIDENT_TYPES:
        raise ValueError(f"Type incident invalide: {incident_type}")

    vehicle_type = str(record["type_vehicule"]).upper()
    if vehicle_type not in VEHICLE_TYPES:
        raise ValueError(f"Type vehicule invalide: {vehicle_type}")

    latitude = float(record["latitude"])
    longitude = float(record["longitude"])
    if not (12.0 <= latitude <= 17.5 and -18.5 <= longitude <= -11.0):
        raise ValueError("Coordonnees hors du Senegal")

    nb_victimes = int(record["nb_victimes"])
    if nb_victimes < 0:
        raise ValueError("nb_victimes doit etre positif")

    heure = int(record["heure"])
    if heure < 0 or heure > 23:
        raise ValueError("heure doit etre entre 0 et 23")

    facteur_heure = float(record["facteur_heure"])
    if facteur_heure < 0.5 or facteur_heure > 2.5:
        raise ValueError("facteur_heure doit etre entre 0.5 et 2.5")

    timestamp = parse_timestamp(record["timestamp"]).isoformat().replace("+00:00", "Z")

    normalized = dict(record)
    normalized.update(
        {
            "zone": zone,
            "type_incident": incident_type,
            "type_vehicule": vehicle_type,
            "latitude": latitude,
            "longitude": longitude,
            "nb_victimes": nb_victimes,
            "heure": heure,
            "facteur_heure": facteur_heure,
            "timestamp": timestamp,
        }
    )
    return normalized


def secure_incident_id(record: Dict[str, Any], salt: Optional[str] = None) -> str:
    """Calcule un identifiant SHA-256 sale pour remplacer l'identifiant brut."""
    effective_salt = salt or os.getenv("SECUR_SECRET_SALT", "change_me_for_demo")
    source = f"{effective_salt}:{record.get('incident_id')}:{record.get('timestamp')}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def anonymize_incident(record: Dict[str, Any], salt: Optional[str] = None) -> Dict[str, Any]:
    """Supprime les PII et ajoute `incident_secure`."""
    normalized = validate_incident(record)
    safe = {key: value for key, value in normalized.items() if key not in FORBIDDEN_PII_FIELDS}
    safe["incident_secure"] = secure_incident_id(normalized, salt=salt)
    return safe


def contains_pii_keys(record: Dict[str, Any]) -> bool:
    """Detecte les cles PII interdites dans un dictionnaire."""
    return any(key in FORBIDDEN_PII_FIELDS for key in record.keys())


def assert_no_pii(record: Dict[str, Any]) -> None:
    """Leve une erreur si une sortie contient une cle PII interdite."""
    leaked = sorted(FORBIDDEN_PII_FIELDS.intersection(record.keys()))
    if leaked:
        raise ValueError(f"PII detectees en sortie: {', '.join(leaked)}")


def detect_pii_fields(record: Dict[str, Any]) -> List[str]:
    """Detecte les cles sensibles connues dans un enregistrement."""
    detected = []
    for key in record.keys():
        normalized = str(key).strip().lower().replace("-", "_").replace(" ", "_")
        if normalized in PII_FIELD_ALIASES:
            detected.append(str(key))
    return sorted(set(detected))


def privacy_metric_label(field_name: str) -> str:
    """Retourne un libelle non sensible pour les rapports privacy."""
    normalized = str(field_name).strip().lower().replace("-", "_").replace(" ", "_")
    return PII_METRIC_LABELS.get(normalized, "champ_sensible")


def score_gravite(type_incident: str) -> float:
    """Score lie a la severite de l'incident."""
    incident_type = type_incident.upper()
    if "MORTEL" in incident_type:
        return 5.0
    if "GRAVE" in incident_type:
        return 3.0
    if "LEGER" in incident_type:
        return 1.0
    return 0.5


def score_vehicule(type_vehicule: str) -> float:
    """Score lie au type de vehicule."""
    vehicle_type = type_vehicule.upper()
    weights = {
        "MOTO_JAKARTA": 1.5,
        "CAR_RAPIDE": 1.3,
        "CAMION": 1.2,
        "VOITURE": 1.0,
        "BUS": 0.9,
        "CHARRETTE": 0.8,
    }
    return weights.get(vehicle_type, 1.0)


def score_meteo(meteo: Optional[Dict[str, Any]]) -> float:
    """Score lie aux conditions meteorologiques."""
    if not meteo:
        return 1.0
    pluie = float(meteo.get("pluie_mm", 0.0) or 0.0)
    route_mouillee = bool(meteo.get("route_mouillee", False))
    visibilite = str(meteo.get("visibilite", "BONNE")).upper()
    if pluie > 20:
        return 1.5
    if route_mouillee or pluie > 0 or visibilite in {"FAIBLE", "MAUVAISE"}:
        return 1.2
    return 1.0


def compute_risk_score(incident: Dict[str, Any], meteo: Optional[Dict[str, Any]] = None) -> float:
    """Calcule le score de risque individuel."""
    safe_incident = validate_incident(incident) if contains_pii_keys(incident) else incident
    score = (
        score_gravite(str(safe_incident["type_incident"]))
        * score_vehicule(str(safe_incident["type_vehicule"]))
        * score_meteo(meteo)
        * float(safe_incident.get("facteur_heure", 1.0))
    )
    return round(score, 3)


def risk_level(score: float) -> str:
    """Classe un score en niveau VERT, ORANGE ou ROUGE."""
    if score > 20:
        return "ROUGE"
    if score > 10:
        return "ORANGE"
    return "VERT"


def recommendation_for_alert(alert: Dict[str, Any]) -> str:
    """Produit une recommandation operationnelle courte."""
    level = alert.get("niveau_risque", "VERT")
    zone = alert.get("zone", "ZONE")
    hour = int(alert.get("heure_critique", alert.get("heure", 0)) or 0)
    if level == "ROUGE":
        return f"Renforcer les patrouilles a {zone} autour de {hour:02d}h et activer prevention routiere."
    if level == "ORANGE":
        return f"Positionner une equipe mobile a {zone} et surveiller la meteo avant {hour:02d}h."
    return f"Maintenir la surveillance standard a {zone} et suivre les signaux faibles."


def build_alert(record: Dict[str, Any], meteo: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Transforme un incident brut en alerte anonymisee."""
    safe = anonymize_incident(record) if contains_pii_keys(record) else dict(record)
    score = compute_risk_score(record, meteo)
    alert = {
        "incident_secure": safe["incident_secure"],
        "zone": safe["zone"],
        "type_incident": safe["type_incident"],
        "type_vehicule": safe["type_vehicule"],
        "latitude": safe["latitude"],
        "longitude": safe["longitude"],
        "nb_victimes": safe["nb_victimes"],
        "heure": safe["heure"],
        "timestamp": safe["timestamp"],
        "score_gravite": score_gravite(safe["type_incident"]),
        "score_vehicule": score_vehicule(safe["type_vehicule"]),
        "score_meteo": score_meteo(meteo),
        "facteur_heure": safe.get("facteur_heure", 1.0),
        "score_risque": score,
        "niveau_risque": risk_level(score),
    }
    alert["recommandation"] = recommendation_for_alert(alert)
    assert_no_pii(alert)
    return alert


def aggregate_alerts(alerts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Agrege les alertes par zone pour les hot-spots."""
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for alert in alerts:
        assert_no_pii(alert)
        groups[str(alert["zone"])].append(alert)

    results: List[Dict[str, Any]] = []
    for zone, rows in groups.items():
        total_score = sum(float(row["score_risque"]) for row in rows)
        max_row = max(rows, key=lambda row: float(row["score_risque"]))
        nb_incidents = len(rows)
        nb_victimes = sum(int(row.get("nb_victimes", 0)) for row in rows)
        score_density = total_score * max(1, nb_incidents) / 3
        level = risk_level(score_density)
        hotspot = {
            "hotspot_id": f"{zone}#{parse_timestamp(max_row['timestamp']).strftime('%Y%m%d%H')}",
            "zone": zone,
            "latitude": round(sum(float(row["latitude"]) for row in rows) / nb_incidents, 6),
            "longitude": round(sum(float(row["longitude"]) for row in rows) / nb_incidents, 6),
            "nb_incidents": nb_incidents,
            "nb_victimes": nb_victimes,
            "heure_critique": int(max_row.get("heure", 0)),
            "score_risque": round(score_density, 3),
            "niveau_risque": level,
            "timestamp": max_row["timestamp"],
        }
        hotspot["recommandation"] = recommendation_for_alert(hotspot)
        assert_no_pii(hotspot)
        results.append(hotspot)
    return sorted(results, key=lambda row: row["score_risque"], reverse=True)


def sample_incident(index: int = 0, rng: Optional[random.Random] = None) -> Dict[str, Any]:
    """Genere un incident brut avec PII pour simuler GAMA/CETUD."""
    rand = rng or random.Random()
    zone = rand.choice(list(ZONES.keys()))
    base_lat, base_lon = ZONES[zone]
    timestamp = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    timestamp -= timedelta(hours=rand.randint(0, 72))
    hour = rand.choice([7, 8, 12, 17, 18, 19, 22, rand.randint(0, 23)])
    return {
        "incident_id": f"INC-{timestamp.strftime('%Y%m%d%H')}-{index:06d}",
        "nom_victime": f"VICTIME_{rand.randint(100000, 999999)}",
        "tel_temoin": f"+221 77 {rand.randint(100, 999)} {rand.randint(10, 99)} {rand.randint(10, 99)}",
        "zone": zone,
        "type_incident": rand.choices(INCIDENT_TYPES, weights=[42, 32, 12, 14], k=1)[0],
        "type_vehicule": rand.choices(VEHICLE_TYPES, weights=[35, 20, 15, 15, 10, 5], k=1)[0],
        "latitude": round(base_lat + rand.uniform(-0.025, 0.025), 6),
        "longitude": round(base_lon + rand.uniform(-0.025, 0.025), 6),
        "nb_victimes": rand.choices([0, 1, 2, 3, 4, 5], weights=[8, 42, 25, 15, 7, 3], k=1)[0],
        "heure": hour,
        "facteur_heure": 1.5 if hour in {7, 8, 17, 18, 19} else 1.0,
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
    }


def sample_meteo(index: int = 0, rng: Optional[random.Random] = None) -> Dict[str, Any]:
    """Genere une observation meteo par zone."""
    rand = rng or random.Random()
    zone = rand.choice(list(ZONES.keys()))
    pluie = round(rand.choices([0.0, 2.5, 8.0, 18.0, 25.0, 34.0], weights=[45, 20, 15, 10, 6, 4], k=1)[0], 1)
    timestamp = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    timestamp -= timedelta(hours=index % 24)
    return {
        "zone": zone,
        "temperature": round(rand.uniform(25.0, 37.0), 1),
        "pluie_mm": pluie,
        "visibilite": "FAIBLE" if pluie > 20 else ("MOYENNE" if pluie > 0 else "BONNE"),
        "route_mouillee": pluie > 0,
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
    }


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Lit un fichier JSON Lines."""
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]], append: bool = False) -> None:
    """Ecrit une collection de dictionnaires en JSON Lines."""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with path.open(mode, encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    """Ecrit une collection de dictionnaires en CSV."""
    rows_list = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows_list:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows_list[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_list)


def read_records_for_privacy(path: Path) -> List[Dict[str, Any]]:
    """Lit JSONL, JSON ou CSV pour les controles privacy."""
    if not path.exists():
        return []
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if suffix == ".json":
        content = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(content, list):
            return [row for row in content if isinstance(row, dict)]
        if isinstance(content, dict):
            return [content]
        return []

    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def compute_privacy_metrics(
    raw_paths: Optional[Iterable[Path]] = None,
    processed_paths: Optional[Iterable[Path]] = None,
) -> Dict[str, Any]:
    """Produit des metriques Privacy by Design sans stocker de valeurs PII."""
    effective_raw_paths = list(
        raw_paths
        or [
            project_path("data", "raw", "incidents_demo.jsonl"),
            project_path("data", "samples", "incidents_sample.jsonl"),
        ]
    )
    effective_processed_paths = list(
        processed_paths
        or [
            project_path("data", "processed", "alerts_fallback.jsonl"),
            project_path("data", "processed", "hotspots_fallback.jsonl"),
            project_path("data", "processed", "incidents_historique.csv"),
            project_path("data", "processed", "hotspots_historique.csv"),
            project_path("data", "processed", "hbase_hotspots_mock.jsonl"),
            project_path("reports", "hive_vue_hotspots_preview.csv"),
            project_path("reports", "recommandations_patrouilles.csv"),
        ]
    )

    raw_records_scanned = 0
    raw_records_with_pii = 0
    raw_pii_fields: Dict[str, int] = defaultdict(int)
    processed_records_scanned = 0
    processed_pii_leaks = 0
    processed_files_with_leaks: List[str] = []
    processed_pii_fields: Dict[str, int] = defaultdict(int)

    for path in effective_raw_paths:
        for record in read_records_for_privacy(path):
            raw_records_scanned += 1
            detected = detect_pii_fields(record)
            if detected:
                raw_records_with_pii += 1
            for field in detected:
                raw_pii_fields[privacy_metric_label(field)] += 1

    for path in effective_processed_paths:
        file_has_leak = False
        for record in read_records_for_privacy(path):
            processed_records_scanned += 1
            detected = detect_pii_fields(record)
            if detected:
                processed_pii_leaks += 1
                file_has_leak = True
            for field in detected:
                processed_pii_fields[privacy_metric_label(field)] += 1
        if file_has_leak:
            processed_files_with_leaks.append(str(path.relative_to(PROJECT_ROOT)))

    blocked_fields = sum(raw_pii_fields.values())
    status = "OK" if processed_pii_leaks == 0 else "ALERTE"
    return {
        "generated_at": now_utc_iso(),
        "status": status,
        "raw_records_scanned": raw_records_scanned,
        "raw_records_with_pii": raw_records_with_pii,
        "raw_pii_fields_detected": dict(sorted(raw_pii_fields.items())),
        "blocked_pii_fields": blocked_fields,
        "processed_records_scanned": processed_records_scanned,
        "processed_pii_leaks": processed_pii_leaks,
        "processed_pii_fields_detected": dict(sorted(processed_pii_fields.items())),
        "processed_files_with_leaks": processed_files_with_leaks,
        "pii_policy": {
            "forbidden_output_field_count": len(FORBIDDEN_PII_FIELDS),
            "technical_identifier": "incident_secure",
            "hashing": "SHA-256 sale",
        },
    }


def write_privacy_metrics(output: Optional[Path] = None) -> Dict[str, Any]:
    """Calcule et ecrit le rapport privacy JSON."""
    metrics = compute_privacy_metrics()
    target = output or project_path("reports", "privacy_metrics.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    return metrics
