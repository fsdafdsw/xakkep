import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from features import compute_volume_anomaly


class FeatureTests(unittest.TestCase):
    def test_volume_anomaly_detects_elevated_activity(self):
        result = compute_volume_anomaly(
            {
                "volume24h": 1800.0,
                "volume": 6000.0,
                "liquidity": 1200.0,
                "spread": 0.02,
                "one_hour_change": 0.02,
                "one_day_change": 0.03,
                "one_week_change": 0.01,
            }
        )
        self.assertGreater(result["anomaly_score"], 0.0)
        self.assertGreater(result["confirmation"], 0.5)

    def test_volume_anomaly_stays_low_for_quiet_market(self):
        result = compute_volume_anomaly(
            {
                "volume24h": 40.0,
                "volume": 4000.0,
                "liquidity": 1200.0,
                "spread": 0.07,
                "one_hour_change": 0.0,
                "one_day_change": 0.0,
                "one_week_change": 0.0,
            }
        )
        self.assertLess(result["anomaly_score"], 0.1)
        self.assertLess(result["confirmation"], 0.5)


if __name__ == "__main__":
    unittest.main()
