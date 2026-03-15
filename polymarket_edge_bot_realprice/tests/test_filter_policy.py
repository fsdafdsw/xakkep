import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from filter_policy import filter_reason


def _market(question):
    return {
        "question": question,
        "volume24h": 500.0,
        "volume": 500.0,
        "liquidity": 500.0,
        "ref_price": 0.15,
        "best_bid": 0.14,
        "best_ask": 0.16,
        "spread": 0.02,
        "hours_to_close": 24.0,
        "endDate": "2026-06-01T00:00:00Z",
        "category": "Politics",
        "groupItemTitle": question,
        "outcomes": '["Yes","No"]',
    }


class FilterPolicyTests(unittest.TestCase):
    def test_excludes_gta_comparison_market(self):
        reason = filter_reason(_market("Russia-Ukraine Ceasefire before GTA VI?"))
        self.assertEqual(reason, "excluded_pattern")

    def test_excludes_what_happens_before_market(self):
        reason = filter_reason(_market("What will happen before GTA VI?"))
        self.assertEqual(reason, "excluded_pattern")

    def test_allows_normal_geopolitical_market(self):
        reason = filter_reason(_market("Xi Jinping out before 2027?"))
        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()
