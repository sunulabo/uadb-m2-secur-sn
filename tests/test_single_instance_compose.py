import unittest
from pathlib import Path


class SingleInstanceComposeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compose = (Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text(encoding="utf-8")

    def test_cluster_services_are_absent(self):
        for service_name in ("kafka-2", "kafka-3", "spark-master", "spark-worker", "spark-worker-2"):
            self.assertNotIn(f"  {service_name}:", self.compose)

    def test_kafka_and_spark_use_single_instance_configuration(self):
        self.assertIn("KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093", self.compose)
        self.assertIn("KAFKA_DEFAULT_REPLICATION_FACTOR: 1", self.compose)
        self.assertIn("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: ${KAFKA_INTERNAL_TOPIC_PARTITIONS:-1}", self.compose)
        self.assertIn("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS: ${KAFKA_INTERNAL_TOPIC_PARTITIONS:-1}", self.compose)
        self.assertIn("KAFKA_TOPIC_PARTITIONS: \"3\"", self.compose)
        self.assertIn("KAFKA_TOPIC_REPLICATION_FACTOR: \"1\"", self.compose)
        self.assertIn("bash -c '>/dev/tcp/localhost/19092'", self.compose)
        self.assertIn("bash -c 'host=$$(hostname -i); >/dev/tcp/$$host/16000 && >/dev/tcp/127.0.0.1/9090'", self.compose)
        self.assertIn("      - ${SPARK_MASTER:-local[1]}", self.compose)

    def test_new_runtime_volumes_do_not_reuse_cluster_metadata(self):
        self.assertIn("kafka_single_data:", self.compose)
        self.assertIn("spark_single_checkpoints:", self.compose)
        self.assertIn("nifi_work_cache:", self.compose)
        self.assertIn("nifi-work-init:", self.compose)
        self.assertNotIn("kafka1_data:", self.compose)
        self.assertNotIn("spark_checkpoints:", self.compose)


if __name__ == "__main__":
    unittest.main()
