import json
import unittest
from pathlib import Path


class ReferenceDataTest(unittest.TestCase):
    def test_domain_data_is_consistent(self):
        data = json.loads((Path(__file__).parents[1] / "reference" / "domain.json").read_text(encoding="utf-8"))
        self.assertEqual(data["domain"], "greenhouse-irrigation")
        self.assertGreater(data["daily_water_limit_liters"], 0)
        self.assertEqual({z["valve_id"] for z in data["zones"]}, {v["id"] for v in data["valves"]})
        self.assertEqual(len({e["event_id"] for e in data["telemetry"]}), len(data["telemetry"]))


if __name__ == "__main__":
    unittest.main()
