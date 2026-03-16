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

    def test_opens_scout_trade_from_watch_high_upside_lane(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.10, stake=0.25)
            scout["repricing_verdict"] = "watch_high_upside"
            scout["repricing_lane_key"] = "diplomacy_talk_call"
            scout["repricing_lane_label"] = "Talk / call lane"
            result = run_paper_cycle(
                [_market(0.10)],
                [],
                best_watchlist=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["opened"][0]["trade_mode"], "scout")

    def test_opens_scout_trade_from_strong_watch_lane(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.10, stake=0.25)
            scout["repricing_verdict"] = "watch"
            scout["repricing_lane_key"] = "diplomacy_talk_call"
            scout["repricing_lane_label"] = "Talk / call lane"
            scout["repricing_watch_score"] = 0.84
            scout["repricing_attention_gap"] = 0.52
            result = run_paper_cycle(
                [_market(0.10)],
                [],
                scout_candidates=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["opened"][0]["trade_mode"], "scout")

    def test_opens_scout_trade_from_strong_radar_buy_now(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.09, stake=0.25)
            scout["repricing_lane_key"] = "generic_repricing"
            scout["repricing_lane_label"] = "Generic repricing"
            scout["repricing_score"] = 0.86
            scout["repricing_watch_score"] = 0.96
            scout["repricing_attention_gap"] = 0.54
            scout["confidence"] = 0.83
            result = run_paper_cycle(
                [_market(0.09)],
                [],
                scout_candidates=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["opened"][0]["trade_mode"], "scout")

    def test_does_not_open_scout_trade_from_weak_radar_buy_now(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.21, stake=0.25)
            scout["repricing_lane_key"] = "generic_repricing"
            scout["repricing_lane_label"] = "Generic repricing"
            scout["repricing_score"] = 0.78
            scout["repricing_watch_score"] = 0.90
            scout["repricing_attention_gap"] = 0.30
            scout["confidence"] = 0.74
            result = run_paper_cycle(
                [_market(0.21)],
                [],
                scout_candidates=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)

    def test_summary_includes_signal_counts_and_preview_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            buy = _candidate(0.10)
            watch = _candidate(0.11, stake=0.25)
            watch["market_id"] = "market-2"
            watch["event_slug"] = "market-2"
            watch["market_key"] = "market-2|token-2"
            watch["selected_token_id"] = "token-2"
            watch["link"] = "https://polymarket.com/event/market-2?tid=token-2"
            watch["question"] = "Watch setup?"
            watch["repricing_verdict"] = "watch_high_upside"
            watch["repricing_lane_key"] = "diplomacy_talk_call"
            watch["repricing_lane_label"] = "Talk / call lane"
            radar = _candidate(0.12, stake=0.25)
            radar["market_id"] = "market-3"
            radar["event_slug"] = "market-3"
            radar["market_key"] = "market-3|token-3"
            radar["selected_token_id"] = "token-3"
            radar["link"] = "https://polymarket.com/event/market-3?tid=token-3"
            radar["question"] = "Radar setup?"
            radar["repricing_lane_key"] = "generic_repricing"
            radar["repricing_lane_label"] = "Generic repricing"
            result = run_paper_cycle(
                [_market(0.10)],
                [buy],
                best_watchlist=[watch],
                radar_candidates=[radar],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(summary["buy_now_count"], 1)
            self.assertEqual(summary["watchlist_count"], 1)
            self.assertEqual(summary["radar_count"], 1)
            self.assertGreaterEqual(len(summary["idea_preview"]), 1)
            self.assertEqual(summary["idea_preview"][0]["question"], "Radar setup?")
            self.assertIn("Signal pool: 1 buy now | 1 watchlist | 1 radar", result["report_text"])
            self.assertIn("Next trade", result["report_text"])


if __name__ == "__main__":
    unittest.main()
