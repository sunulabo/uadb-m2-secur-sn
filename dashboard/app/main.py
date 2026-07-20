"""Dashboard FastAPI: HBase pour l'operationnel, Hive pour l'analytique."""
from __future__ import annotations
import asyncio, json, os
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

def dashboard_data() -> dict[str, Any]:
    errors, operational, analytical = [], [], []
    try: operational = read_hbase()
    except Exception as exc: errors.append(f"HBase indisponible: {type(exc).__name__}")
    try: analytical = read_hive()
    except Exception as exc: errors.append(f"Hive indisponible: {type(exc).__name__}")
    if not operational and not analytical: operational = read_fallback()
    return {"operational": operational, "analytical_24h": analytical, "errors": errors}

async def bounded_dashboard_data() -> dict[str, Any]:
    try:
        return await asyncio.wait_for(asyncio.to_thread(dashboard_data), timeout=8)
    except asyncio.TimeoutError:
        return {"operational": [], "analytical_24h": [], "fallback": read_fallback(), "errors": ["HBase/Hive trop lents: affichage fallback"]}

@app.get("/", response_model=None)
async def home(inertia: InertiaDependency) -> InertiaResponse: return await inertia.render("Dashboard", await bounded_dashboard_data())

@app.get("/api/dashboard")
async def api_dashboard(): return await bounded_dashboard_data()

@app.get("/health")
def health(): return {"status": "ok", "service": "secur-sn-dashboard"}
