import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.generate_dashboard import (
    build_dashboard_summary,
    render_html_dashboard,
    select_crisis_hotspot,
    write_crisis_report,
)
from spark.spark_utils import compute_privacy_metrics


class ReportingTest(unittest.TestCase):
    def sample_alerts(self):
        return [
            {
                "incident_secure": "a" * 64,
                "zone": "PIKINE",
                "type_incident": "ACCIDENT_GRAVE",
                "type_vehicule": "MOTO_JAKARTA",
                "latitude": 14.7,
                "longitude": -17.4,
                "nb_victimes": 2,
                "heure": 18,
                "timestamp": "2026-01-01T18:00:00Z",
                "score_meteo": 1.5,
                "score_risque": 16.2,
                "niveau_risque": "ORANGE",
            },
            {
                "incident_secure": "b" * 64,
                "zone": "PIKINE",
                "type_incident": "ACCIDENT_MORTEL",
                "type_vehicule": "CAR_RAPIDE",
                "latitude": 14.71,
                "longitude": -17.41,
                "nb_victimes": 3,
                "heure": 19,
                "timestamp": "2026-01-01T19:00:00Z",
                "score_meteo": 1.2,
                "score_risque": 24.0,
                "niveau_risque": "ROUGE",
            },
        ]

    def sample_hotspots(self):
        return [
            {
                "hotspot_id": "PIKINE#2026010119",
                "zone": "PIKINE",
                "latitude": 14.7,
                "longitude": -17.4,
                "nb_incidents": 2,
                "nb_victimes": 5,
                "heure_critique": 19,
                "score_risque": 42.0,
                "niveau_risque": "ROUGE",
                "timestamp": "2026-01-01T19:00:00Z",
                "recommandation": "Renforcer les patrouilles a PIKINE autour de 19h.",
            },
            {
                "hotspot_id": "THIES#2026010118",
                "zone": "THIES",
                "latitude": 14.79,
                "longitude": -16.93,
                "nb_incidents": 1,
                "nb_victimes": 1,
                "heure_critique": 18,
                "score_risque": 18.0,
                "niveau_risque": "ORANGE",
                "timestamp": "2026-01-01T18:00:00Z",
                "recommandation": "Positionner une equipe mobile a THIES.",
            },
        ]

    def test_privacy_metrics_counts_blocked_pii_without_processed_leak(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw.jsonl"
            processed = Path(tmp) / "processed.jsonl"
            raw.write_text(json.dumps({"incident_id": "INC-1", "nom_victime": "X", "zone": "PIKINE"}) + "\n", encoding="utf-8")
            processed.write_text(json.dumps({"incident_secure": "a" * 64, "zone": "PIKINE"}) + "\n", encoding="utf-8")
            metrics = compute_privacy_metrics(raw_paths=[raw], processed_paths=[processed])
            self.assertEqual(metrics["status"], "OK")
            self.assertEqual(metrics["raw_records_with_pii"], 1)
            self.assertEqual(metrics["processed_pii_leaks"], 0)
            self.assertGreaterEqual(metrics["blocked_pii_fields"], 2)

    def test_dashboard_html_contains_core_cards(self):
        alerts = self.sample_alerts()
        hotspots = self.sample_hotspots()
        privacy = {"status": "OK", "blocked_pii_fields": 4, "processed_pii_leaks": 0, "raw_pii_fields_detected": {"incident_id": 2}}
        summary = build_dashboard_summary(alerts, hotspots, privacy, {"mode": "fallback_rule_model", "f1": 0.9})
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "dashboard_live.html"
            render_html_dashboard(alerts, hotspots, summary, privacy, {"mode": "fallback_rule_model"}, output)
            html = output.read_text(encoding="utf-8")
            self.assertIn("Dashboard Temps Reel", html)
            self.assertIn("PII bloquees", html)
            self.assertIn("Recommandations prioritaires", html)
            self.assertIn("PIKINE", html)

    def test_select_crisis_hotspot_prefers_highest_red_zone(self):
        hotspot = select_crisis_hotspot(self.sample_hotspots())
        self.assertEqual(hotspot["zone"], "PIKINE")
        self.assertEqual(hotspot["niveau_risque"], "ROUGE")

    def test_crisis_report_files_are_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "rapport_crise_zone_rouge.md"
            image = Path(tmp) / "rapport_crise_zone_rouge.png"
            crisis = write_crisis_report(
                self.sample_alerts(),
                self.sample_hotspots(),
                {"status": "OK", "blocked_pii_fields": 4, "processed_pii_leaks": 0},
                output_md=report,
                output_png=image,
            )
            self.assertEqual(crisis["zone"], "PIKINE")
            self.assertTrue(report.exists())
            self.assertTrue(image.exists())
            self.assertIn("Rapport de crise Secur-SN", report.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
