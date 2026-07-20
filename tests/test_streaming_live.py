import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.streaming_secur_sn import assert_pii_free_columns, hdfs_gold_path, hdfs_options, streaming_settings


class LiveStreamingConfigTest(unittest.TestCase):
    def test_live_defaults_replay_backlog_with_a_resource_bounded_trigger(self):
        settings = streaming_settings({})
        self.assertEqual(settings["starting_offsets"], "earliest")
        self.assertEqual(settings["trigger_interval"], "60 seconds")
        self.assertEqual(settings["hotspot_window"], "5 minutes")
        self.assertEqual(settings["max_offsets_per_trigger"], "120")
        self.assertEqual(settings["hdfs_namenode_uri"], "hdfs://namenode:8020")
        self.assertEqual(settings["hdfs_gold_root"], "/secur-sn/gold")
        self.assertEqual(settings["bootstrap"], "localhost:9092")

    def test_hdfs_uses_the_cluster_namenode(self):
        settings = streaming_settings({"HDFS_NAMENODE_URI": "hdfs://namenode:8020"})
        options = hdfs_options(settings)
        self.assertEqual(options["spark.hadoop.fs.defaultFS"], "hdfs://namenode:8020")

    def test_gold_paths_are_partitioned_by_micro_batch(self):
        settings = streaming_settings({"HDFS_GOLD_ROOT": "/secur-sn/gold"})
        self.assertEqual(
            hdfs_gold_path(settings, "alerts/batch_id=7"),
            "hdfs://namenode:8020/secur-sn/gold/alerts/batch_id=7",
        )
        self.assertEqual(
            hdfs_gold_path(settings, "/hotspots_live/"),
            "hdfs://namenode:8020/secur-sn/gold/hotspots_live",
        )

    def test_pii_columns_block_live_output(self):
        assert_pii_free_columns(["incident_secure", "zone", "score_risque"], "test")
        with self.assertRaisesRegex(ValueError, "nom_victime"):
            assert_pii_free_columns(["zone", "nom_victime"], "test")

    def test_live_source_keeps_vehicle_data_without_intermediate_kafka_topics(self):
        source = (Path(__file__).resolve().parents[1] / "spark" / "streaming_secur_sn.py").read_text(encoding="utf-8")
        self.assertIn('"type_vehicule",', source)
        self.assertIn('.drop("incident_id", "nom_victime", "tel_temoin")', source)
        self.assertNotIn("secur_alerts_enriched", source)
        self.assertNotIn('option("topic", "secur_alerts")', source)

    def test_stream_stream_join_uses_an_append_compatible_output_mode(self):
        source = (Path(__file__).resolve().parents[1] / "spark" / "streaming_secur_sn.py").read_text(encoding="utf-8")
        self.assertIn('"inner",', source)
        self.assertIn('.outputMode("append")', source)


if __name__ == "__main__":
    unittest.main()
