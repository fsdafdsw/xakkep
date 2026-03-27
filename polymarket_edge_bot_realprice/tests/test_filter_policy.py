import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import filter_policy
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

    def test_allows_short_crypto_market_in_fast_crypto_mode(self):
        market = _market("Bitcoin Up or Down - March 27, 12:15-12:30 UTC?")
        market["hours_to_close"] = 0.20
        market["category"] = "Crypto"
        market["groupItemTitle"] = "Crypto"
        market["outcomes"] = '["Up","Down"]'
        market["volume24h"] = 2500.0
        market["volume"] = 2500.0
        market["liquidity"] = 2500.0
        with patch.object(filter_policy, "FAST_CRYPTO_MODE", True), patch.object(
            filter_policy, "FAST_CRYPTO_ALLOWED_SYMBOLS", ["bitcoin", "btc"]
        ):
            reason = filter_reason(market)
        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()
