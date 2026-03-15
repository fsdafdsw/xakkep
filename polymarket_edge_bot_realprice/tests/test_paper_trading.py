import tempfile
import unittest

from paper_trading import run_paper_cycle


def _market(price, best_bid=None):
    return {
        "id": "market-1",
        "slug": "market-1",
        "event_slug": "market-1",
        "selected_token_id": "token-1",
        "token_yes": "token-1",
        "best_bid": best_bid if best_bid is not None else price,
        "best_ask": price,
        "ref_price": price,
        "selected_price": price,
    }


def _candidate(entry, stake=50.0):
    return {
        "market_id": "market-1",
        "event_slug": "market-1",
        "market_key": "market-1|token-1",
        "selected_token_id": "token-1",
        "link": "https://polymarket.com/event/market-1?tid=token-1",
        "question": "Test market?",
        "selected_outcome": "Yes",
        "entry": entry,
        "cost_per_share": 0.002,
        "stake_usd": stake,
        "domain_action_family": "conflict",
        "catalyst_type": "military_action",
        "repricing_verdict": "buy_now",
        "repricing_lane_key": "conflict_fast",
        "repricing_lane_label": "Conflict fast lane",
    }


class PaperTradingTests(unittest.TestCase):
    def test_opens_position_on_buy_now_candidate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_paper_cycle([_market(0.10)], [_candidate(0.10)], state_dir=tmpdir, generated_at_utc="2026-03-15 12:00:00 UTC")
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["open_position_count"], 1)
            self.assertLess(summary["cash_usd"], summary["initial_bankroll_usd"])

    def test_closes_position_when_mark_reaches_take_profit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_paper_cycle([_market(0.10)], [_candidate(0.10)], state_dir=tmpdir, generated_at_utc="2026-03-15 12:00:00 UTC")
            result = run_paper_cycle([_market(0.20, best_bid=0.20)], [], state_dir=tmpdir, generated_at_utc="2026-03-15 12:05:00 UTC")
            summary = result["summary"]
            self.assertEqual(len(summary["closed"]), 1)
            self.assertEqual(summary["open_position_count"], 0)
            self.assertGreater(summary["realized_pnl_usd"], 0.0)


if __name__ == "__main__":
    unittest.main()
