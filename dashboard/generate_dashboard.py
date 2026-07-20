#!/usr/bin/env python3
"""Generation du dashboard PNG et des recommandations."""

from __future__ import annotations

import csv
import html
import json
import struct
import sys
import zlib
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import aggregate_alerts, parse_timestamp, project_path, read_jsonl, write_csv, write_privacy_metrics


LEVEL_COLORS = {
    "VERT": (46, 160, 67),
    "ORANGE": (245, 158, 11),
    "ROUGE": (220, 38, 38),
}


def ensure_data() -> Tuple[List[Dict], List[Dict]]:
    alerts = read_jsonl(project_path("data", "processed", "alerts_fallback.jsonl"))
    if not alerts:
        from producers.generate_batch_history import main as generate_history

        generate_history()
        alerts = read_jsonl(project_path("data", "processed", "alerts_fallback.jsonl"))
    hotspots = read_jsonl(project_path("data", "processed", "hotspots_fallback.jsonl"))
    if not hotspots:
        hotspots = aggregate_alerts(alerts)
    return alerts, hotspots


def write_recommendations(hotspots: List[Dict]) -> None:
    rows = [
        {
            "zone": row["zone"],
            "niveau_risque": row["niveau_risque"],
            "score_risque": row["score_risque"],
            "heure_critique": row["heure_critique"],
            "recommandation": row["recommandation"],
            "justification": f"{row['nb_incidents']} incidents, {row['nb_victimes']} victimes, score {row['score_risque']}",
        }
        for row in hotspots
    ]
    write_csv(project_path("reports", "recommandations_patrouilles.csv"), rows)


def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_json_file(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def build_dashboard_summary(alerts: List[Dict], hotspots: List[Dict], privacy_metrics: Dict, ml_metrics: Dict) -> Dict:
    scores = [safe_float(row.get("score_risque")) for row in hotspots]
    red_hotspots = [row for row in hotspots if row.get("niveau_risque") == "ROUGE"]
    weather_alerts = [row for row in alerts if safe_float(row.get("score_meteo"), 1.0) > 1.0]
    top = hotspots[0] if hotspots else {}
    return {
        "nb_alertes": len(alerts),
        "nb_hotspots": len(hotspots),
        "nb_zones_rouges": len(red_hotspots),
        "score_moyen_hotspots": round(sum(scores) / max(1, len(scores)), 2),
        "meteo_dangereuse": len(weather_alerts),
        "top_zone": top.get("zone"),
        "top_score": top.get("score_risque"),
        "top_niveau": top.get("niveau_risque"),
        "privacy_status": privacy_metrics.get("status", "INCONNU"),
        "pii_bloquees": privacy_metrics.get("blocked_pii_fields", 0),
        "processed_pii_leaks": privacy_metrics.get("processed_pii_leaks", 0),
        "ml_mode": ml_metrics.get("mode", "non_execute"),
        "ml_f1": ml_metrics.get("f1"),
        "ml_rmse": ml_metrics.get("rmse"),
    }


def risk_badge(level: str) -> str:
    color = {
        "VERT": "#16a34a",
        "ORANGE": "#d97706",
        "ROUGE": "#dc2626",
    }.get(level, "#475569")
    return f'<span class="badge" style="background:{color}">{html.escape(level or "N/A")}</span>'


def render_html_dashboard(
    alerts: List[Dict],
    hotspots: List[Dict],
    summary: Dict,
    privacy_metrics: Dict,
    ml_metrics: Dict,
    output: Path,
) -> None:
    """Genere un dashboard HTML autonome pour la demo."""
    vehicle_counts = Counter(row.get("type_vehicule", "INCONNU") for row in alerts)
    level_counts = Counter(row.get("niveau_risque", "INCONNU") for row in hotspots)
    top_hotspots = hotspots[:8]
    max_score = max([safe_float(row.get("score_risque")) for row in top_hotspots] or [1.0])
    cards = [
        ("Incidents analyses", summary["nb_alertes"], "Flux fallback Spark"),
        ("Zones rouges", summary["nb_zones_rouges"], "Hotspots critiques"),
        ("Score moyen", summary["score_moyen_hotspots"], "Risque agrege"),
        ("PII bloquees", summary["pii_bloquees"], f"Privacy {summary['privacy_status']}"),
        ("Meteo dangereuse", summary["meteo_dangereuse"], "Alertes pluie/visibilite"),
        ("Mode ML", summary["ml_mode"], "F1: " + str(summary.get("ml_f1", "N/A"))),
    ]
    cards_html = "\n".join(
        f"""
        <section class="metric">
          <span>{html.escape(str(label))}</span>
          <strong>{html.escape(str(value))}</strong>
          <small>{html.escape(str(detail))}</small>
        </section>
        """
        for label, value, detail in cards
    )
    bars_html = "\n".join(
        f"""
        <div class="bar-row">
          <span>{html.escape(str(row.get("zone", "ZONE")))}</span>
          <div class="bar-track"><div class="bar-fill {html.escape(str(row.get("niveau_risque", "VERT")).lower())}" style="width:{min(100, safe_float(row.get("score_risque")) / max_score * 100):.1f}%"></div></div>
          <b>{safe_float(row.get("score_risque")):.1f}</b>
        </div>
        """
        for row in top_hotspots
    )
    table_rows = "\n".join(
        f"""
        <tr>
          <td>{html.escape(str(row.get("zone", "")))}</td>
          <td>{risk_badge(str(row.get("niveau_risque", "")))}</td>
          <td>{safe_float(row.get("score_risque")):.2f}</td>
          <td>{html.escape(str(row.get("heure_critique", ""))).zfill(2)}h</td>
          <td>{html.escape(str(row.get("recommandation", "")))}</td>
        </tr>
        """
        for row in top_hotspots
    )
    vehicle_html = "\n".join(
        f"<li><span>{html.escape(str(vehicle))}</span><strong>{count}</strong></li>"
        for vehicle, count in vehicle_counts.most_common(6)
    )
    privacy_fields = privacy_metrics.get("raw_pii_fields_detected", {})
    privacy_html = "\n".join(
        f"<li><span>{html.escape(str(field))}</span><strong>{count}</strong></li>"
        for field, count in privacy_fields.items()
    )
    level_html = "\n".join(
        f"<li><span>{html.escape(str(level))}</span><strong>{count}</strong></li>"
        for level, count in level_counts.items()
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="60">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Secur-SN - Dashboard Temps Reel</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #0f172a;
      --muted: #64748b;
      --panel: #ffffff;
      --line: #dbe4ee;
      --bg: #eef4f7;
      --blue: #2563eb;
      --green: #16a34a;
      --orange: #d97706;
      --red: #dc2626;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
    }}
    header {{
      padding: 28px 36px 18px;
      background: #0f172a;
      color: white;
      border-bottom: 5px solid #22c55e;
    }}
    header h1 {{ margin: 0 0 8px; font-size: clamp(28px, 4vw, 48px); letter-spacing: 0; }}
    header p {{ margin: 0; color: #cbd5e1; }}
    main {{ padding: 24px 36px 40px; max-width: 1440px; margin: 0 auto; }}
    .grid {{ display: grid; gap: 18px; }}
    .metrics {{ grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); margin-bottom: 18px; }}
    .metric, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
    }}
    .metric {{ padding: 18px; min-height: 122px; display: flex; flex-direction: column; justify-content: space-between; }}
    .metric span, .metric small {{ color: var(--muted); }}
    .metric strong {{ font-size: 28px; line-height: 1.1; overflow-wrap: anywhere; }}
    .layout {{ grid-template-columns: minmax(0, 1.25fr) minmax(320px, 0.75fr); align-items: start; }}
    .panel {{ padding: 20px; }}
    .panel h2 {{ margin: 0 0 16px; font-size: 20px; }}
    .bar-row {{ display: grid; grid-template-columns: 130px minmax(100px, 1fr) 52px; gap: 12px; align-items: center; margin: 12px 0; }}
    .bar-track {{ height: 18px; background: #e2e8f0; border-radius: 999px; overflow: hidden; }}
    .bar-fill {{ height: 100%; background: var(--green); }}
    .bar-fill.orange {{ background: var(--orange); }}
    .bar-fill.rouge {{ background: var(--red); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 12px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ color: var(--muted); font-size: 13px; text-transform: uppercase; }}
    .badge {{ color: white; border-radius: 999px; padding: 5px 9px; font-size: 12px; font-weight: 700; display: inline-block; min-width: 68px; text-align: center; }}
    ul {{ list-style: none; padding: 0; margin: 0; display: grid; gap: 9px; }}
    li {{ display: flex; justify-content: space-between; gap: 12px; padding: 10px 12px; background: #f8fafc; border-radius: 8px; }}
    .side {{ display: grid; gap: 18px; }}
    footer {{ color: var(--muted); padding-top: 18px; font-size: 13px; }}
    @media (max-width: 900px) {{
      header, main {{ padding-left: 18px; padding-right: 18px; }}
      .layout {{ grid-template-columns: 1fr; }}
      .bar-row {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>Secur-SN - Dashboard Temps Reel</h1>
    <p>Zones a risque, alertes anonymisees, meteo et recommandations de patrouille. Rafraichissement fichier toutes les 60 secondes.</p>
  </header>
  <main>
    <section class="grid metrics">{cards_html}</section>
    <section class="grid layout">
      <div class="grid">
        <article class="panel">
          <h2>Scores de risque par zone</h2>
          {bars_html or "<p>Aucun hotspot disponible.</p>"}
        </article>
        <article class="panel">
          <h2>Recommandations prioritaires</h2>
          <table>
            <thead><tr><th>Zone</th><th>Niveau</th><th>Score</th><th>Heure</th><th>Action</th></tr></thead>
            <tbody>{table_rows}</tbody>
          </table>
        </article>
      </div>
      <aside class="side">
        <article class="panel">
          <h2>Vehicules impliques</h2>
          <ul>{vehicle_html}</ul>
        </article>
        <article class="panel">
          <h2>Privacy by Design</h2>
          <ul>
            <li><span>Statut sorties</span><strong>{html.escape(str(summary["privacy_status"]))}</strong></li>
            <li><span>Fuites traitees</span><strong>{html.escape(str(summary["processed_pii_leaks"]))}</strong></li>
            {privacy_html}
          </ul>
        </article>
        <article class="panel">
          <h2>Niveaux de risque</h2>
          <ul>{level_html}</ul>
        </article>
      </aside>
    </section>
    <footer>Artefacts: dashboard_hotspots.png, privacy_metrics.json, rapport_crise_zone_rouge.md, rapport_crise_zone_rouge.png.</footer>
  </main>
</body>
</html>
""",
        encoding="utf-8",
    )


def select_crisis_hotspot(hotspots: List[Dict]) -> Dict:
    red_hotspots = [row for row in hotspots if row.get("niveau_risque") == "ROUGE"]
    candidates = red_hotspots or hotspots
    if not candidates:
        return {}
    return max(candidates, key=lambda row: safe_float(row.get("score_risque")))


def crisis_rows(alerts: List[Dict], zone: str) -> List[Dict]:
    return [row for row in alerts if row.get("zone") == zone]


def hourly_distribution(rows: List[Dict]) -> Counter:
    distribution = Counter()
    for row in rows:
        try:
            hour = parse_timestamp(row["timestamp"]).hour
        except Exception:
            hour = int(row.get("heure", 0) or 0)
        distribution[f"{hour:02d}h"] += 1
    return distribution


def render_crisis_with_matplotlib(zone_alerts: List[Dict], hotspot: Dict, output: Path) -> bool:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return False

    hours = hourly_distribution(zone_alerts)
    vehicles = Counter(row.get("type_vehicule", "INCONNU") for row in zone_alerts)
    labels = sorted(hours.keys())
    counts = [hours[label] for label in labels]
    vehicle_labels = [label for label, _ in vehicles.most_common(5)]
    vehicle_counts = [count for _, count in vehicles.most_common(5)]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle(f"Rapport de crise Secur-SN - {hotspot.get('zone', 'ZONE')}", fontsize=15, fontweight="bold")
    axes[0].bar(labels, counts, color="#dc2626")
    axes[0].set_title("Incidents par heure")
    axes[0].set_xlabel("Heure")
    axes[0].set_ylabel("Incidents")
    axes[0].tick_params(axis="x", rotation=45)
    axes[1].bar(vehicle_labels, vehicle_counts, color="#2563eb")
    axes[1].set_title("Vehicules les plus impliques")
    axes[1].tick_params(axis="x", rotation=35)
    fig.text(
        0.04,
        0.02,
        f"Niveau {hotspot.get('niveau_risque')} - score {hotspot.get('score_risque')} - recommandation: {hotspot.get('recommandation', '')}",
        fontsize=10,
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.93))
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return True


def render_crisis_with_pillow(zone_alerts: List[Dict], hotspot: Dict, output: Path) -> bool:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return False

    image = Image.new("RGB", (1280, 720), (248, 250, 252))
    draw = ImageDraw.Draw(image)
    try:
        title_font = ImageFont.truetype("Arial.ttf", 32)
        heading_font = ImageFont.truetype("Arial.ttf", 22)
        text_font = ImageFont.truetype("Arial.ttf", 16)
    except Exception:
        title_font = heading_font = text_font = ImageFont.load_default()

    draw.text((42, 34), f"Rapport de crise Secur-SN - {hotspot.get('zone', 'ZONE')}", fill=(15, 23, 42), font=title_font)
    draw.text((44, 80), f"Niveau {hotspot.get('niveau_risque')} | score {hotspot.get('score_risque')}", fill=(71, 85, 105), font=heading_font)
    draw.rounded_rectangle((42, 130, 620, 610), radius=8, fill=(255, 255, 255), outline=(226, 232, 240), width=2)
    draw.rounded_rectangle((660, 130, 1238, 610), radius=8, fill=(255, 255, 255), outline=(226, 232, 240), width=2)
    draw.text((70, 158), "Incidents par heure", fill=(15, 23, 42), font=heading_font)
    hours = hourly_distribution(zone_alerts)
    labels = sorted(hours.keys())
    max_count = max(hours.values() or [1])
    for index, label in enumerate(labels[:12]):
        count = hours[label]
        y = 210 + index * 30
        draw.text((78, y), label, fill=(51, 65, 85), font=text_font)
        draw.rectangle((150, y + 5, 150 + int(count / max_count * 370), y + 22), fill=(220, 38, 38))
        draw.text((530, y), str(count), fill=(15, 23, 42), font=text_font)

    draw.text((690, 158), "Facteurs operationnels", fill=(15, 23, 42), font=heading_font)
    vehicles = Counter(row.get("type_vehicule", "INCONNU") for row in zone_alerts)
    for index, (vehicle, count) in enumerate(vehicles.most_common(6)):
        y = 215 + index * 38
        draw.text((700, y), vehicle, fill=(51, 65, 85), font=text_font)
        draw.text((1120, y), str(count), fill=(15, 23, 42), font=text_font)
    recommendation = str(hotspot.get("recommandation", "Maintenir la surveillance."))
    draw.text((700, 500), "Action recommandee", fill=(15, 23, 42), font=heading_font)
    draw.text((700, 535), recommendation[:78], fill=(71, 85, 105), font=text_font)
    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output)
    return True


def render_crisis_fallback_png(hotspot: Dict, output: Path) -> None:
    render_fallback_png([hotspot] if hotspot else [], output)


def write_crisis_report(
    alerts: List[Dict],
    hotspots: List[Dict],
    privacy_metrics: Dict,
    output_md: Optional[Path] = None,
    output_png: Optional[Path] = None,
) -> Dict:
    hotspot = select_crisis_hotspot(hotspots)
    report_path = output_md or project_path("reports", "rapport_crise_zone_rouge.md")
    image_path = output_png or project_path("reports", "rapport_crise_zone_rouge.png")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if not hotspot:
        report_path.write_text("# Rapport de crise Secur-SN\n\nAucune zone rouge disponible.\n", encoding="utf-8")
        render_crisis_fallback_png({}, image_path)
        return {"zone": None, "report": str(report_path), "image": str(image_path)}

    rows = crisis_rows(alerts, str(hotspot["zone"]))
    vehicles = Counter(row.get("type_vehicule", "INCONNU") for row in rows)
    dangerous_weather = sum(1 for row in rows if safe_float(row.get("score_meteo"), 1.0) > 1.0)
    if not render_crisis_with_matplotlib(rows, hotspot, image_path) and not render_crisis_with_pillow(rows, hotspot, image_path):
        render_crisis_fallback_png(hotspot, image_path)

    top_vehicle = vehicles.most_common(1)[0][0] if vehicles else "INCONNU"
    content = f"""# Rapport de crise Secur-SN

## Zone prioritaire

- Zone: {hotspot.get("zone")}
- Niveau: {hotspot.get("niveau_risque")}
- Score de risque: {hotspot.get("score_risque")}
- Heure critique: {int(hotspot.get("heure_critique", 0) or 0):02d}h
- Incidents analyses dans la zone: {len(rows)}
- Victimes estimees: {hotspot.get("nb_victimes")}
- Vehicule dominant: {top_vehicle}
- Alertes avec facteur meteo: {dangerous_weather}

## Diagnostic

La zone {hotspot.get("zone")} concentre le signal le plus critique du dernier lot analyse.
Le score combine la densite d'incidents, la gravite, le type de vehicule, la meteo et le facteur horaire.
Les donnees exposees sont anonymisees avec `incident_secure`; les champs PII bruts ne sont pas diffuses.

## Privacy by Design

- Statut privacy: {privacy_metrics.get("status")}
- PII detectees en entree et bloquees: {privacy_metrics.get("blocked_pii_fields", 0)}
- Fuites PII dans les sorties traitees: {privacy_metrics.get("processed_pii_leaks", 0)}

## Recommandation operationnelle

{hotspot.get("recommandation")}

## Artefact visuel

Voir `{image_path.name}`.
"""
    report_path.write_text(content, encoding="utf-8")
    return {"zone": hotspot.get("zone"), "report": str(report_path), "image": str(image_path)}


def render_with_matplotlib(alerts: List[Dict], hotspots: List[Dict], output: Path) -> bool:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return False

    top_hotspots = hotspots[:8]
    zones = [row["zone"] for row in top_hotspots]
    scores = [float(row["score_risque"]) for row in top_hotspots]
    colors = [tuple(component / 255 for component in LEVEL_COLORS[row["niveau_risque"]]) for row in top_hotspots]
    vehicle_counts = Counter(row["type_vehicule"] for row in alerts)
    level_counts = Counter(row["niveau_risque"] for row in hotspots)

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle("Secur-SN - Hot-spots et recommandations", fontsize=16, fontweight="bold")

    axes[0, 0].bar(zones, scores, color=colors)
    axes[0, 0].set_title("Score risque par zone")
    axes[0, 0].set_ylabel("Score")
    axes[0, 0].tick_params(axis="x", rotation=35)

    axes[0, 1].scatter(
        [float(row["longitude"]) for row in hotspots],
        [float(row["latitude"]) for row in hotspots],
        s=[max(60, float(row["score_risque"]) * 8) for row in hotspots],
        c=[tuple(component / 255 for component in LEVEL_COLORS[row["niveau_risque"]]) for row in hotspots],
        alpha=0.75,
    )
    axes[0, 1].set_title("Carte simplifiee lat/lon")
    axes[0, 1].set_xlabel("Longitude")
    axes[0, 1].set_ylabel("Latitude")

    axes[1, 0].bar(list(vehicle_counts.keys()), list(vehicle_counts.values()), color="#2563eb")
    axes[1, 0].set_title("Top types de vehicules")
    axes[1, 0].tick_params(axis="x", rotation=35)

    axes[1, 1].pie(
        [level_counts.get(level, 0) for level in ["VERT", "ORANGE", "ROUGE"]],
        labels=["VERT", "ORANGE", "ROUGE"],
        colors=[tuple(component / 255 for component in LEVEL_COLORS[level]) for level in ["VERT", "ORANGE", "ROUGE"]],
        autopct="%1.0f%%",
    )
    axes[1, 1].set_title("Repartition des niveaux")

    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return True


def render_with_pillow(alerts: List[Dict], hotspots: List[Dict], output: Path) -> bool:
    """Produit un dashboard lisible si Matplotlib est absent."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return False

    width, height = 1280, 820
    image = Image.new("RGB", (width, height), (248, 250, 252))
    draw = ImageDraw.Draw(image)
    try:
        title_font = ImageFont.truetype("Arial.ttf", 34)
        subtitle_font = ImageFont.truetype("Arial.ttf", 20)
        text_font = ImageFont.truetype("Arial.ttf", 16)
        small_font = ImageFont.truetype("Arial.ttf", 13)
    except Exception:
        title_font = subtitle_font = text_font = small_font = ImageFont.load_default()

    def panel(x0: int, y0: int, x1: int, y1: int, title: str) -> None:
        draw.rounded_rectangle((x0, y0, x1, y1), radius=10, fill=(255, 255, 255), outline=(226, 232, 240), width=2)
        draw.text((x0 + 18, y0 + 14), title, fill=(15, 23, 42), font=subtitle_font)

    draw.text((48, 28), "Secur-SN - Hot-spots accidents et recommandations", fill=(15, 23, 42), font=title_font)
    draw.text((50, 70), "Scores anonymises, niveaux de risque et actions de patrouille", fill=(71, 85, 105), font=text_font)

    top = hotspots[:8]
    panel(48, 115, 610, 420, "Score risque par zone")
    max_score = max([float(row["score_risque"]) for row in top] or [1.0])
    base_y = 370
    for index, row in enumerate(top):
        score = float(row["score_risque"])
        x0 = 82 + index * 65
        bar_h = int((score / max_score) * 190)
        color = LEVEL_COLORS.get(row["niveau_risque"], (71, 85, 105))
        draw.rectangle((x0, base_y - bar_h, x0 + 42, base_y), fill=color)
        draw.text((x0 - 4, base_y + 8), row["zone"][:5], fill=(51, 65, 85), font=small_font)
        draw.text((x0 - 2, base_y - bar_h - 18), str(round(score, 1)), fill=(15, 23, 42), font=small_font)

    panel(670, 115, 1230, 420, "Carte simplifiee lat/lon")
    if hotspots:
        lats = [float(row["latitude"]) for row in hotspots]
        lons = [float(row["longitude"]) for row in hotspots]
        min_lat, max_lat = min(lats), max(lats)
        min_lon, max_lon = min(lons), max(lons)
        for row in hotspots:
            lon = float(row["longitude"])
            lat = float(row["latitude"])
            x = 710 + int((lon - min_lon) / max(0.001, max_lon - min_lon) * 460)
            y = 365 - int((lat - min_lat) / max(0.001, max_lat - min_lat) * 205)
            score = float(row["score_risque"])
            radius = max(7, min(28, int(score / max_score * 28)))
            color = LEVEL_COLORS.get(row["niveau_risque"], (71, 85, 105))
            draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=color, outline=(255, 255, 255), width=2)
            draw.text((x + radius + 4, y - 7), row["zone"][:9], fill=(30, 41, 59), font=small_font)

    panel(48, 455, 610, 760, "Vehicules impliques")
    vehicle_counts = Counter(row["type_vehicule"] for row in alerts)
    for index, (vehicle, count) in enumerate(vehicle_counts.most_common(6)):
        y = 515 + index * 36
        draw.text((80, y), vehicle, fill=(30, 41, 59), font=text_font)
        draw.rectangle((245, y + 4, 245 + count * 7, y + 22), fill=(37, 99, 235))
        draw.text((255 + count * 7, y), str(count), fill=(15, 23, 42), font=text_font)

    panel(670, 455, 1230, 760, "Recommandations prioritaires")
    for index, row in enumerate(top[:5]):
        y = 515 + index * 42
        color = LEVEL_COLORS.get(row["niveau_risque"], (71, 85, 105))
        draw.rounded_rectangle((700, y, 770, y + 26), radius=6, fill=color)
        draw.text((708, y + 6), row["niveau_risque"], fill=(255, 255, 255), font=small_font)
        draw.text((785, y), f"{row['zone']} - {row['heure_critique']:02d}h - score {row['score_risque']}", fill=(15, 23, 42), font=text_font)
        draw.text((785, y + 20), row["recommandation"][:58], fill=(71, 85, 105), font=small_font)

    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output)
    return True


def png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)


def render_fallback_png(hotspots: List[Dict], output: Path) -> None:
    width, height = 960, 540
    pixels = bytearray([248, 250, 252] * width * height)

    def set_pixel(x: int, y: int, color: Tuple[int, int, int]) -> None:
        if 0 <= x < width and 0 <= y < height:
            offset = (y * width + x) * 3
            pixels[offset : offset + 3] = bytes(color)

    def rect(x0: int, y0: int, x1: int, y1: int, color: Tuple[int, int, int]) -> None:
        for y in range(max(0, y0), min(height, y1)):
            for x in range(max(0, x0), min(width, x1)):
                set_pixel(x, y, color)

    rect(60, 60, 900, 420, (226, 232, 240))
    rect(62, 62, 898, 418, (255, 255, 255))
    top = hotspots[:8]
    max_score = max([float(row["score_risque"]) for row in top] or [1.0])
    bar_width = 80
    gap = 24
    base_y = 390
    for index, row in enumerate(top):
        score = float(row["score_risque"])
        bar_height = int((score / max_score) * 280)
        x0 = 90 + index * (bar_width + gap)
        color = LEVEL_COLORS.get(row["niveau_risque"], (71, 85, 105))
        rect(x0, base_y - bar_height, x0 + bar_width, base_y, color)

    raw = b"".join(bytes([0]) + pixels[y * width * 3 : (y + 1) * width * 3] for y in range(height))
    png = (
        b"\x89PNG\r\n\x1a\n"
        + png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + png_chunk(b"IDAT", zlib.compress(raw, 9))
        + png_chunk(b"IEND", b"")
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(png)


def main() -> int:
    alerts, hotspots = ensure_data()
    hotspots = sorted(hotspots, key=lambda row: float(row["score_risque"]), reverse=True)
    write_recommendations(hotspots)
    privacy_metrics = write_privacy_metrics()
    ml_metrics = load_json_file(project_path("reports", "ml_metrics.json"))
    summary = build_dashboard_summary(alerts, hotspots, privacy_metrics, ml_metrics)
    project_path("data", "dashboard").mkdir(parents=True, exist_ok=True)
    project_path("data", "dashboard", "dashboard_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    output = project_path("reports", "dashboard_hotspots.png")
    if not render_with_matplotlib(alerts, hotspots, output) and not render_with_pillow(alerts, hotspots, output):
        render_fallback_png(hotspots, output)
    html_output = project_path("reports", "dashboard_live.html")
    render_html_dashboard(alerts, hotspots, summary, privacy_metrics, ml_metrics, html_output)
    crisis = write_crisis_report(alerts, hotspots, privacy_metrics)
    print(f"Dashboard genere: {output}")
    print(f"Dashboard HTML: {html_output}")
    print(f"Metrics privacy: reports/privacy_metrics.json")
    print(f"Rapport de crise: {crisis['report']}")
    print("Recommandations: reports/recommandations_patrouilles.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
