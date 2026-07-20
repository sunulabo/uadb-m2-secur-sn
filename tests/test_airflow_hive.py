import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hive.airflow_client import hive_connection_settings


class AirflowHiveContractTest(unittest.TestCase):
    def test_airflow_hive_client_uses_the_docker_hive_server(self):
        settings = hive_connection_settings({})
        self.assertEqual(settings["host"], "hive-server")
        self.assertEqual(settings["port"], "10000")

    def test_dag_reads_hive_24h_view_without_a_fallback_file(self):
        source = (Path(__file__).resolve().parents[1] / "airflow" / "dags" / "secur_sn_dag.py").read_text(encoding="utf-8")
        self.assertIn("analytics_risk_summary", source)
        self.assertNotIn("hotspots_fallback.jsonl", source)


if __name__ == "__main__":
    unittest.main()
