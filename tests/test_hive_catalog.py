import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hive.catalog import catalog_statements, gold_location, hive_settings


class HiveCatalogContractTest(unittest.TestCase):
    def test_catalog_targets_hdfs_gold(self):
        settings = hive_settings({"HDFS_NAMENODE_URI": "hdfs://namenode:8020", "HDFS_GOLD_ROOT": "/gold-demo"})
        self.assertEqual(gold_location(settings, "alerts"), "hdfs://namenode:8020/gold-demo/alerts")

    def test_parquet_tables_define_spark_partitions(self):
        statements = "\n".join(catalog_statements(hive_settings({})))
        self.assertIn("PARTITIONED BY (batch_id INT, event_date STRING)", statements)
        self.assertIn("PARTITIONED BY (batch_id INT, snapshot_date STRING)", statements)
        self.assertIn("vue_tendances_vehicule", statements)

    def test_catalog_schema_has_no_raw_pii(self):
        statements = "\n".join(catalog_statements(hive_settings({}))).lower()
        for field in ("incident_id", "nom_victime", "tel_temoin"):
            self.assertNotIn(field, statements)

    def test_catalog_schema_uses_hdfs_parquet_not_minio(self):
        statements = "\n".join(catalog_statements(hive_settings({})))
        self.assertIn("hdfs://namenode:8020/secur-sn/gold/alerts", statements)
        self.assertIn("STORED AS PARQUET", statements)
        self.assertNotIn("s3a://", statements)

    def test_hotspot_view_deduplicates_replayed_batches(self):
        statements = "\n".join(catalog_statements(hive_settings({})))
        self.assertIn("ROW_NUMBER() OVER", statements)
        self.assertIn("PARTITION BY hotspot_id", statements)


if __name__ == "__main__":
    unittest.main()
