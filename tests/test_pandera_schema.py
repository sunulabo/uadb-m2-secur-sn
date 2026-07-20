import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import build_pandera_schema, sample_incident, validate_incident


class PanderaSchemaTest(unittest.TestCase):
    def test_schema_incidents_valid(self):
        incident = sample_incident(index=1)
        normalized = validate_incident(incident)
        self.assertEqual(normalized["zone"], incident["zone"])
        schema = build_pandera_schema()
        self.assertTrue(schema is None or hasattr(schema, "validate"))

    def test_schema_rejects_bad_hour(self):
        incident = sample_incident(index=2)
        incident["heure"] = 25
        with self.assertRaises(ValueError):
            validate_incident(incident)


if __name__ == "__main__":
    unittest.main()
