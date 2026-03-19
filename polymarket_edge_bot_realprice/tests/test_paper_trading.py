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
        "thesis_id": "threshold_ladder:test",
        "thesis_type": "threshold_ladder",
        "thesis_cluster_size": 2,
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

    def test_blocks_second_open_in_same_thesis(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = _candidate(0.10)
            second = _candidate(0.11)
            second["market_id"] = "market-2"
            second["event_slug"] = "market-2"
            second["market_key"] = "market-2|token-2"
            second["selected_token_id"] = "token-2"
            second["link"] = "https://polymarket.com/event/market-2?tid=token-2"
            second["question"] = "Sibling market?"
            second["thesis_id"] = first["thesis_id"]
            result = run_paper_cycle(
                [_market(0.10)],
                [first, second],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["open_position_count"], 1)

    def test_blocks_reentry_into_same_thesis_after_close(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = _candidate(0.10)
            run_paper_cycle(
                [_market(0.10)],
                [first],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            sibling = _candidate(0.12)
            sibling["market_id"] = "market-2"
            sibling["event_slug"] = "market-2"
            sibling["market_key"] = "market-2|token-2"
            sibling["selected_token_id"] = "token-2"
            sibling["link"] = "https://polymarket.com/event/market-2?tid=token-2"
            sibling["question"] = "Sibling market?"
            sibling["thesis_id"] = first["thesis_id"]
            result = run_paper_cycle(
                [_market(0.20, best_bid=0.20)],
                [sibling],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:05:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["closed"]), 1)
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["open_position_count"], 0)

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

    def test_opens_scout_trade_from_global_high_upside_lane(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.12, stake=0.25)
            scout["repricing_verdict"] = "watch_high_upside"
            scout["repricing_lane_key"] = "diplomacy_ceasefire"
            scout["repricing_lane_label"] = "Ceasefire lane"
            scout["repricing_watch_score"] = 0.79
            scout["repricing_attention_gap"] = 0.42
            scout["repricing_lane_prior"] = 0.68
            scout["confidence"] = 0.78
            result = run_paper_cycle(
                [_market(0.12)],
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
            watch["thesis_id"] = "watch_thesis:test"
            watch["primary_entity_key"] = "putin"
            radar = _candidate(0.12, stake=0.25)
            radar["market_id"] = "market-3"
            radar["event_slug"] = "market-3"
            radar["market_key"] = "market-3|token-3"
            radar["selected_token_id"] = "token-3"
            radar["link"] = "https://polymarket.com/event/market-3?tid=token-3"
            radar["question"] = "Radar setup?"
            radar["repricing_lane_key"] = "generic_repricing"
            radar["repricing_lane_label"] = "Generic repricing"
            radar["thesis_id"] = "radar_thesis:test"
            radar["primary_entity_key"] = "poland"
            radar["repricing_score"] = 0.86
            radar["repricing_watch_score"] = 0.96
            radar["repricing_attention_gap"] = 0.54
            radar["confidence"] = 0.83
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
            self.assertEqual(len(summary["opened"]), 2)
            self.assertEqual(summary["idea_preview"], [])
            self.assertIn("Executable pool: 1 core | 1 watch scout | 1 radar scout", result["report_text"])
            self.assertIn("Next trade", result["report_text"])
            self.assertIn("Next trade\nnone", result["report_text"])
            self.assertNotIn("Watch only", result["report_text"])

    def test_preview_hides_candidate_blocked_by_theme_cap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = _candidate(0.10)
            first["primary_entity_key"] = "israel"
            run_paper_cycle(
                [_market(0.10)],
                [first],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )

            blocked = _candidate(0.12, stake=0.25)
            blocked["market_id"] = "market-2"
            blocked["event_slug"] = "market-2"
            blocked["market_key"] = "market-2|token-2"
            blocked["selected_token_id"] = "token-2"
            blocked["link"] = "https://polymarket.com/event/market-2?tid=token-2"
            blocked["question"] = "Will Israel strike 6 countries in 2026?"
            blocked["repricing_verdict"] = "watch_high_upside"
            blocked["primary_entity_key"] = "israel"

            result = run_paper_cycle(
                [_market(0.10)],
                [],
                best_watchlist=[blocked],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:05:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(summary["watchlist_count"], 0)
            self.assertEqual(summary["idea_preview"], [])
            self.assertIn("Next trade\nnone", result["report_text"])

    def test_blocks_third_conflict_position_by_lane_cap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            china = _candidate(0.10)
            china["question"] = "Will China invade Taiwan by end of 2026?"
            china["primary_entity_key"] = "china"
            china["event_slug"] = "china-invade"
            china["market_key"] = "china|token-1"
            china["link"] = "https://polymarket.com/event/china-invade?tid=token-1"
            china["thesis_id"] = "conflict_thesis:china"

            north_korea = _candidate(0.10)
            north_korea["question"] = "Will North Korea invade South Korea before 2027?"
            north_korea["primary_entity_key"] = "north_korea"
            north_korea["event_slug"] = "nk-invade"
            north_korea["market_key"] = "nk|token-1"
            north_korea["link"] = "https://polymarket.com/event/nk-invade?tid=token-1"
            north_korea["thesis_id"] = "conflict_thesis:nk"

            run_paper_cycle(
                [_market(0.10), _market(0.10)],
                [china, north_korea],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )

            third = _candidate(0.11)
            third["question"] = "Will Russia strike Poland by June 30?"
            third["primary_entity_key"] = "russia"
            third["event_slug"] = "russia-strike-poland"
            third["market_key"] = "russia|token-1"
            third["link"] = "https://polymarket.com/event/russia-strike-poland?tid=token-1"
            third["thesis_id"] = "conflict_thesis:russia"

            result = run_paper_cycle(
                [_market(0.11)],
                [third],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:05:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["buy_now_count"], 0)

    def test_blocks_non_selected_consistency_candidate_in_ladder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            blocked = _candidate(0.10)
            blocked["question"] = "Blocked sibling?"
            blocked["consistency_engine_supported"] = True
            blocked["consistency_selected"] = False
            blocked["consistency_residual"] = 0.05
            blocked["consistency_bias"] = "underpriced_yes"

            result = run_paper_cycle(
                [_market(0.10)],
                [blocked],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["buy_now_count"], 0)

    def test_opens_selected_consistency_candidate_in_ladder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            selected = _candidate(0.10)
            selected["question"] = "Selected sibling?"
            selected["consistency_engine_supported"] = True
            selected["consistency_selected"] = True
            selected["consistency_residual"] = 0.05
            selected["consistency_bias"] = "underpriced_yes"

            result = run_paper_cycle(
                [_market(0.10)],
                [selected],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["buy_now_count"], 1)

    def test_blocks_non_selected_next_buyer_candidate_in_ladder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            blocked = _candidate(0.10)
            blocked["question"] = "Blocked next buyer sibling?"
            blocked["next_buyer_supported"] = True
            blocked["next_buyer_selected"] = False
            blocked["next_buyer_edge"] = 0.12
            blocked["consistency_engine_supported"] = True
            blocked["consistency_selected"] = True
            blocked["consistency_residual"] = 0.05

            result = run_paper_cycle(
                [_market(0.10)],
                [blocked],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["buy_now_count"], 0)

    def test_opens_selected_next_buyer_candidate_in_ladder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            selected = _candidate(0.10)
            selected["question"] = "Selected next buyer sibling?"
            selected["next_buyer_supported"] = True
            selected["next_buyer_selected"] = True
            selected["next_buyer_edge"] = 0.16
            selected["next_buyer_score"] = 0.74
            selected["consistency_engine_supported"] = True
            selected["consistency_selected"] = False
            selected["consistency_residual"] = 0.01

            result = run_paper_cycle(
                [_market(0.10)],
                [selected],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["buy_now_count"], 1)


if __name__ == "__main__":
    unittest.main()
