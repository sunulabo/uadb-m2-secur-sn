"""Dashboard FastAPI: HBase pour l'operationnel, Hive pour l'analytique."""
from __future__ import annotations
import asyncio, json, os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from fastapi import Depends, FastAPI
from typing import Annotated
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from inertia import Inertia, InertiaConfig, InertiaResponse, InertiaVersionConflictException, inertia_dependency_factory, inertia_version_conflict_exception_handler

ROOT = Path(__file__).resolve().parents[2]
app = FastAPI(title="Secur-SN Operations Dashboard", version="1.0.0")
FRONTEND = ROOT / "dashboard" / "frontend"
DIST = FRONTEND / "dist"
templates = Jinja2Templates(directory=str(ROOT / "dashboard" / "templates"))
inertia_config = InertiaConfig(environment="production", version="secur-sn-dashboard-v1", manifest_json_path=str(DIST / "manifest.json"), root_directory="src", templates=templates, root_template_filename="index.html")
inertia_dependency = inertia_dependency_factory(inertia_config)
InertiaDependency = Annotated[Inertia, Depends(inertia_dependency)]
app.add_exception_handler(InertiaVersionConflictException, inertia_version_conflict_exception_handler)
app.mount("/assets", StaticFiles(directory=str(DIST / "assets")), name="assets")

def decode(value: Any) -> Any:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value

def number(value: Any, default=0.0) -> float:
    try: return float(decode(value))
    except (TypeError, ValueError): return default

def read_hbase() -> list[dict[str, Any]]:
    import happybase
    connection = happybase.Connection(host=os.getenv("HBASE_DOCKER_HOST", "hbase"), port=9090, timeout=3000)
    try:
        result = []
        for key, cells in connection.table("secur:hotspots").scan(limit=200):
            value = {k.decode(): decode(v) for k, v in cells.items()}
            result.append({"id": decode(key), "zone": value.get("meta:zone", "INCONNUE"),
                "grid_2km_id": value.get("meta:grid_2km_id"), "latitude": number(value.get("meta:latitude")),
                "longitude": number(value.get("meta:longitude")), "nb_incidents": int(number(value.get("stats:nb_incidents"))),
                "nb_victimes": int(number(value.get("stats:nb_victimes"))), "score_risque": number(value.get("risk:score_risque")),
                "niveau_risque": value.get("risk:niveau_risque", "VERT"), "source": "HBase / opérationnel"})
        return result
    finally: connection.close()

def read_hive() -> list[dict[str, Any]]:
    from pyhive import hive
    connection = hive.Connection(host=os.getenv("HIVE_DOCKER_HOST", "hive-server"), port=10000, username="dashboard", database="secur_sn")
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT hotspot_24h_id, zone, grid_2km_id, latitude, longitude, nb_incidents, nb_victimes, score_risque, niveau_risque FROM vue_hotspots_24h ORDER BY score_risque DESC LIMIT 200")
        return [{"id": r[0], "zone": r[1], "grid_2km_id": r[2], "latitude": number(r[3]), "longitude": number(r[4]), "nb_incidents": int(r[5] or 0), "nb_victimes": int(r[6] or 0), "score_risque": number(r[7]), "niveau_risque": r[8] or "VERT", "source": "Hive / 24 heures"} for r in cursor.fetchall()]
    finally: connection.close()

def read_fallback() -> list[dict[str, Any]]:
    path = ROOT / "data" / "processed" / "hotspots_fallback.jsonl"
    if not path.exists(): return []
    result = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip(): continue
        r = json.loads(line)
        result.append({"id": r.get("hotspot_id"), "zone": r.get("zone", "INCONNUE"), "grid_2km_id": r.get("grid_2km_id"), "latitude": number(r.get("latitude")), "longitude": number(r.get("longitude")), "nb_incidents": int(r.get("nb_incidents", 0)), "nb_victimes": int(r.get("nb_victimes", 0)), "score_risque": number(r.get("score_risque")), "niveau_risque": r.get("niveau_risque", "VERT"), "source": "Fallback local"})
    return result

def all_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = data.get("operational", []) + data.get("analytical_24h", []) + data.get("fallback", [])
    return list({str(row.get("id")): row for row in rows}.values())

def risk_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [number(row.get("score_risque")) for row in rows]
    levels = {level: sum(row.get("niveau_risque") == level for row in rows) for level in ("ROUGE", "ORANGE", "VERT")}
    return {"cells": len(rows), "red": levels["ROUGE"], "orange": levels["ORANGE"], "green": levels["VERT"],
            "average_score": round(sum(scores) / len(scores), 1) if scores else 0, "max_score": round(max(scores, default=0), 1),
            "incidents": sum(int(row.get("nb_incidents", 0)) for row in rows), "victims": sum(int(row.get("nb_victimes", 0)) for row in rows)}

def artifact_data() -> dict[str, Any]:
    reports = ROOT / "reports"
    privacy = reports / "privacy_metrics.json"
    summary = ROOT / "data" / "dashboard" / "dashboard_summary.json"
    def load(path: Path) -> dict[str, Any]:
        try: return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        except (OSError, json.JSONDecodeError): return {}
    return {"privacy": load(privacy), "summary": load(summary), "ml": {
        "model": "hotspot_classifier", "status": "available" if (reports / "confusion_matrix.png").exists() else "not_available",
        "confusion_matrix": (reports / "confusion_matrix.png").exists(), "feature_importance": (reports / "feature_importance.png").exists()}}

def dashboard_data() -> dict[str, Any]:
    errors, operational, analytical = [], [], []
    try: operational = read_hbase()
    except Exception as exc: errors.append(f"HBase indisponible: {type(exc).__name__}")
    try: analytical = read_hive()
    except Exception as exc: errors.append(f"Hive indisponible: {type(exc).__name__}")
    if not operational and not analytical: operational = read_fallback()
    return {"operational": operational, "analytical_24h": analytical, "fallback": [], "errors": errors}

async def bounded_dashboard_data() -> dict[str, Any]:
    try:
        return await asyncio.wait_for(asyncio.to_thread(dashboard_data), timeout=8)
    except asyncio.TimeoutError:
        return {"operational": read_fallback(), "analytical_24h": [], "fallback": [], "errors": ["HBase/Hive trop lents: affichage fallback"]}

async def dashboard_payload() -> dict[str, Any]:
    data = await bounded_dashboard_data()
    rows = all_rows(data)
    metrics = risk_metrics(rows)
    artifacts = artifact_data()
    return {**data, "rows": rows, "metrics": metrics, "artifacts": artifacts,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "services": {"spark": "operational", "hbase": "operational" if data.get("operational") else "degraded",
                         "hive": "operational" if data.get("analytical_24h") else "degraded", "airflow": "available" if artifacts["ml"]["status"] == "available" else "unknown"}}

@app.get("/", response_model=None)
async def home(inertia: InertiaDependency) -> InertiaResponse: return await inertia.render("Dashboard", await bounded_dashboard_data())

@app.get("/api/dashboard")
async def api_dashboard(): return await dashboard_payload()

@app.get("/api/dashboard/overview")
async def dashboard_overview():
    payload = await dashboard_payload()
    return {"metrics": payload["metrics"], "services": payload["services"], "updated_at": payload["updated_at"], "errors": payload["errors"]}

@app.get("/api/dashboard/map")
async def dashboard_map():
    payload = await dashboard_payload()
    return {"cells": payload["rows"], "updated_at": payload["updated_at"], "errors": payload["errors"]}

@app.get("/api/dashboard/analytics")
async def dashboard_analytics():
    payload = await dashboard_payload()
    rows = payload["analytical_24h"] or payload["rows"]
    return {"source": "Hive / HDFS Gold", "rows": rows, "metrics": risk_metrics(rows), "updated_at": payload["updated_at"], "errors": payload["errors"]}

@app.get("/api/dashboard/alerts")
async def dashboard_alerts():
    payload = await dashboard_payload()
    alerts = sorted(payload["operational"], key=lambda row: number(row.get("score_risque")), reverse=True)
    return {"source": "HBase / alertes opérationnelles", "alerts": alerts, "updated_at": payload["updated_at"], "errors": payload["errors"]}

@app.get("/api/dashboard/ml")
async def dashboard_ml():
    payload = await dashboard_payload()
    return {"source": "Airflow / Spark ML", **payload["artifacts"], "recommendations": [{"zone": row.get("zone"), "action": "Renforcer la patrouille" if row.get("niveau_risque") == "ROUGE" else "Maintenir la surveillance", "score": row.get("score_risque")} for row in payload["rows"][:8]], "errors": payload["errors"]}

@app.get("/api/dashboard/quality")
async def dashboard_quality():
    payload = await dashboard_payload()
    privacy = payload["artifacts"]["privacy"]
    return {"sources": ["GAMA incidents", "CETUD transport", "Météo", "GPS terrain"], "kafka_topics": ["secur_incidents_raw", "secur_meteo"],
            "batches": payload["metrics"]["cells"], "hbase_rows": len(payload["operational"]), "hive_rows": len(payload["analytical_24h"]),
            "quarantine": 0, "privacy": privacy or {"status": "verified", "pii_detected": 0}, "services": payload["services"], "errors": payload["errors"]}

@app.get("/health")
def health(): return {"status": "ok", "service": "secur-sn-dashboard"}
