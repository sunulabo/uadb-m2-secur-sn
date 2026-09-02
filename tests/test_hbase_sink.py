import unittest
from pathlib import Path
import sys
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import spark.hbase_sink as hbase_sink
from spark.hbase_sink import (
    TABLES,
    assert_safe_payload,
    ensure_namespace,
    ensure_tables,
    hbase_namespace_url,
    hbase_settings,
    hotspot_cells,
    incident_cells,
)


class HBaseSinkContractTest(unittest.TestCase):
    def test_realtime_incidents_have_a_24_hour_ttl(self):
        for family in TABLES["secur:incidents_temps_reel"].values():
            self.assertEqual(family["time_to_live"], 86400)

    def test_incident_cells_are_pii_free(self):
        cells = incident_cells(
            {
                "incident_secure": "hashed",
                "zone": "PIKINE",
                "grid_2km_id": "wm2km_-968_837",
                "type_incident": "ACCIDENT_GRAVE",
                "type_vehicule": "BUS",
                "nb_victimes": 2,
                "score_risque": 8.5,
            }
        )
        self.assertIn(b"meta:type_vehicule", cells)
        self.assertIn(b"meta:grid_2km_id", cells)
        self.assertNotIn(b"meta:nom_victime", cells)

    def test_hotspot_cells_keep_the_batch_identity(self):
        cells = hotspot_cells(
            {
                "hotspot_id": "PIKINE#202607150600",
                "zone": "PIKINE",
                "grid_2km_id": "wm2km_-968_837",
                "nb_incidents": 4,
                "nb_victimes": 3,
                "batch_id": 12,
                "score_risque": 24.5,
                "niveau_risque": "ROUGE",
            }
        )
        self.assertEqual(cells[b"stats:batch_id"], b"12")
        self.assertEqual(cells[b"meta:grid_2km_id"], b"wm2km_-968_837")

    def test_pii_alias_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "telephone_temoin"):
            assert_safe_payload({"zone": "THIES", "telephone_temoin": "+221770000000"})

    def test_sink_uses_docker_defaults(self):
        settings = hbase_settings({})
        self.assertEqual(settings["hbase_host"], "hbase")
        self.assertEqual(settings["hbase_port"], "9090")
        self.assertEqual(settings["hbase_rest_url"], "http://hbase:8080")

    def test_namespace_is_created_with_hbase_rest_before_tables(self):
        calls = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        def opener(request, timeout):
            calls.append((request.get_method(), request.full_url, timeout))
            if request.get_method() == "GET":
                raise HTTPError(request.full_url, 500, "missing", {}, None)
            return Response()

        settings = hbase_settings({})
        ensure_namespace(settings, opener=opener)

        self.assertEqual(calls[0][0], "GET")
        self.assertEqual(calls[1][0], "POST")
        self.assertEqual(calls[1][1], hbase_namespace_url(settings))

    def test_ensure_tables_skips_the_flaky_rest_namespace_check_when_nothing_is_missing(self):
        class FakeConnection:
            def tables(self):
                return [name.encode() for name in TABLES]

            def close(self):
                pass

        def failing_ensure_namespace(*_args, **_kwargs):
            raise AssertionError("ensure_namespace ne doit pas etre appele si les tables existent deja")

        original_connect = hbase_sink._connect
        original_ensure_namespace = hbase_sink.ensure_namespace
        hbase_sink._connect = lambda settings: FakeConnection()
        hbase_sink.ensure_namespace = failing_ensure_namespace
        try:
            ensure_tables(hbase_settings({}))
        finally:
            hbase_sink._connect = original_connect
            hbase_sink.ensure_namespace = original_ensure_namespace


if __name__ == "__main__":
    unittest.main()
