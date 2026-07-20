import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import compute_risk_score, risk_level, sample_incident


class ScoringTest(unittest.TestCase):
    def test_score_risque_outputs_valid_level(self):
        incident = sample_incident(index=5)
        score = compute_risk_score(incident, {"pluie_mm": 0, "route_mouillee": False})
        self.assertIn(risk_level(score), {"VERT", "ORANGE", "ROUGE"})

    def test_mortel_has_higher_score_than_grave(self):
        incident = sample_incident(index=6)
        incident["type_incident"] = "ACCIDENT_GRAVE"
        grave = compute_risk_score(incident, {"pluie_mm": 0, "route_mouillee": False})
        incident["type_incident"] = "ACCIDENT_MORTEL"
        mortel = compute_risk_score(incident, {"pluie_mm": 0, "route_mouillee": False})
        self.assertGreater(mortel, grave)

    def test_rain_increases_risk(self):
        incident = sample_incident(index=7)
        dry = compute_risk_score(incident, {"pluie_mm": 0, "route_mouillee": False})
        rain = compute_risk_score(incident, {"pluie_mm": 25, "route_mouillee": True})
        self.assertGreater(rain, dry)


if __name__ == "__main__":
    unittest.main()
