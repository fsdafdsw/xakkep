import unittest

from fast_crypto import build_fast_crypto_candidates


class FastCryptoTests(unittest.TestCase):
    def _item(self, *, one_hour_change=0.015, one_day_change=0.03, fair=0.53):
        market = {
            "id": "crypto-market-1",
            "event_id": "crypto-event-1",
            "event_slug": "btc-up-or-down-15m",
            "slug": "btc-up-or-down-15m",
            "question": "Bitcoin Up or Down - March 27, 12:15-12:30 UTC?",
            "event_title": "Bitcoin Up or Down",
            "category_group": "crypto",
            "market_type": "near_term_binary",
            "hours_to_close": 0.20,
            "outcomes": ["Up", "Down"],
            "outcome_prices": [0.47, 0.53],
            "token_ids": ["token-up", "token-down"],
            "selected_outcome_index": 0,
            "selected_outcome": "Up",
            "selected_token_id": "token-up",
            "spread": 0.02,
            "liquidity": 2500.0,
            "volume24h": 4000.0,
            "one_hour_change": one_hour_change,
            "one_day_change": one_day_change,
            "end_ts": 1775000000,
        }
        metrics = {
            "momentum": 0.72 if one_hour_change >= 0 else 0.28,
            "orderbook": 0.72,
            "volume_confirmation": 0.66,
            "anomaly": 0.20,
        }
        return {
            "market": market,
            "metrics": metrics,
            "fair": fair,
            "entry": 0.47,
        }

    def test_builds_buy_now_candidate_for_positive_up_signal(self):
        result = build_fast_crypto_candidates([self._item()])
        self.assertEqual(result["summary"]["active_short_markets"], 1)
        self.assertEqual(len(result["buy_candidates"]), 1)
        candidate = result["buy_candidates"][0]
        self.assertEqual(candidate["selected_outcome"], "Up")
        self.assertEqual(candidate["repricing_lane_key"], "crypto_micro")
        self.assertEqual(candidate["repricing_verdict"], "buy_now")

    def test_can_choose_down_when_signal_turns_negative(self):
        result = build_fast_crypto_candidates([self._item(one_hour_change=-0.018, one_day_change=-0.04, fair=0.47)])
        self.assertEqual(result["summary"]["active_short_markets"], 1)
        self.assertEqual(len(result["buy_candidates"]), 1)
        candidate = result["buy_candidates"][0]
        self.assertEqual(candidate["selected_outcome"], "Down")
        self.assertEqual(candidate["repricing_verdict"], "buy_now")
