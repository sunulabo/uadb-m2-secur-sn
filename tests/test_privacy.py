import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import FORBIDDEN_PII_FIELDS, anonymize_incident, build_alert, compute_privacy_metrics, sample_incident


class PrivacyTest(unittest.TestCase):
    def test_privacy_no_pii_after_anonymization(self):
        incident = sample_incident(index=3)
        safe = anonymize_incident(incident, salt="test")
        self.assertIn("incident_secure", safe)
        self.assertEqual(len(safe["incident_secure"]), 64)
        for field in FORBIDDEN_PII_FIELDS:
            self.assertNotIn(field, safe)

    def test_alert_no_pii_after_scoring(self):
        incident = sample_incident(index=4)
        alert = build_alert(incident, {"pluie_mm": 22, "route_mouillee": True})
        self.assertIn("niveau_risque", alert)
        for field in FORBIDDEN_PII_FIELDS:
            self.assertNotIn(field, alert)

    def test_processed_outputs_have_no_pii_keys(self):
        metrics = compute_privacy_metrics()
        self.assertEqual(metrics["processed_pii_leaks"], 0)
        self.assertEqual(metrics["status"], "OK")


if __name__ == "__main__":
    unittest.main()
