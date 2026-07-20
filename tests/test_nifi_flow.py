import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nifi.setup_nifi_minio_kafka_flow import NifiClient


class NifiFlowLifecycleTest(unittest.TestCase):
    def test_processors_are_confirmed_stopped_before_reconfiguration(self):
        class FakeClient:
            def __init__(self):
                self.stopped = False
                self.requests = []

            def get_group_flow(self, _group_id):
                state = "STOPPED" if self.stopped else "RUNNING"
                return {"processors": [{"component": {"id": "processor-1", "state": state}}]}

            def set_processor_state(self, entity, state):
                self.requests.append((entity["component"]["id"], state))
                self.stopped = True

            def get_group_status(self, _group_id):
                return {"activeThreadCount": 0}

        client = FakeClient()
        NifiClient.stop_group_processors(client, "group-1")
        self.assertEqual(client.requests, [("processor-1", "STOPPED")])


if __name__ == "__main__":
    unittest.main()
