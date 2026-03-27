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
        "domain_action_family": "release",
        "catalyst_type": "hearing",
        "repricing_verdict": "buy_now",
        "repricing_score": 0.90,
        "repricing_watch_score": 0.90,
        "repricing_attention_gap": 0.54,
        "repricing_lane_key": "release_hearing",
        "repricing_lane_label": "Legal catalyst lane",
        "repricing_lane_prior": 0.78,
        "repricing_fresh_catalyst_score": 0.72,
        "repricing_conflict_setup_score": 0.86,
        "repricing_conflict_urgency_score": 0.92,
        "confidence": 0.82,
        "regime_selected": False,
        "regime_gap_score": 0.0,
        "next_buyer_selected": False,
        "latent_state_selected": False,
        "thesis_id": "threshold_ladder:test",
        "thesis_type": "standalone",
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

    def test_does_not_open_watch_high_upside_when_scout_is_disabled(self):
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
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["watchlist_count"], 0)

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

    def test_does_not_open_watch_lane_when_scout_is_disabled(self):
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
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["watchlist_count"], 0)

    def test_does_not_open_global_high_upside_when_scout_is_disabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.12, stake=0.25)
            scout["repricing_verdict"] = "watch_high_upside"
            scout["repricing_lane_key"] = "diplomacy_ceasefire"
            scout["repricing_lane_label"] = "Ceasefire lane"
            scout["repricing_watch_score"] = 0.93
            scout["repricing_attention_gap"] = 0.42
            scout["repricing_lane_prior"] = 0.68
            scout["confidence"] = 0.78
            scout["latent_state_selected"] = True
            result = run_paper_cycle(
                [_market(0.12)],
                [],
                scout_candidates=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["watchlist_count"], 0)

    def test_does_not_open_generic_radar_buy_when_scout_is_disabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.09, stake=0.25)
            scout["repricing_lane_key"] = "generic_repricing"
            scout["repricing_lane_label"] = "Generic repricing"
            scout["repricing_score"] = 0.86
            scout["repricing_watch_score"] = 0.96
            scout["repricing_attention_gap"] = 0.54
            scout["confidence"] = 0.83
            scout["regime_selected"] = True
            scout["regime_gap_score"] = 0.22
            result = run_paper_cycle(
                [_market(0.09)],
                [],
                scout_candidates=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["radar_count"], 0)

    def test_does_not_open_conflict_fast_scout_from_radar_buy_now(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.07, stake=0.25)
            scout["repricing_verdict"] = "buy_now"
            scout["thesis_type"] = "threshold_ladder"
            result = run_paper_cycle(
                [_market(0.07)],
                [],
                scout_candidates=[scout],
                radar_candidates=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)

    def test_does_not_open_generic_radar_buy_without_regime_confirmation(self):
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
            self.assertEqual(len(summary["opened"]), 0)

    def test_does_not_open_meeting_watch_high_upside_when_scout_is_disabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scout = _candidate(0.07, stake=0.25)
            scout["repricing_verdict"] = "watch_high_upside"
            scout["repricing_lane_key"] = "diplomacy_meeting"
            scout["repricing_lane_label"] = "Meeting lane"
            scout["repricing_watch_score"] = 0.95
            scout["repricing_attention_gap"] = 0.31
            scout["confidence"] = 0.78
            scout["regime_selected"] = True
            result = run_paper_cycle(
                [_market(0.07)],
                [],
                best_watchlist=[scout],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["watchlist_count"], 0)

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
            result = run_paper_cycle(
                [_market(0.10)],
                [buy],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(summary["buy_now_count"], 1)
            self.assertEqual(summary["watchlist_count"], 0)
            self.assertEqual(summary["radar_count"], 0)
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["idea_preview"], [])
            self.assertIn("Executable core pool: 1", result["report_text"])
            self.assertIn("Next executable trade\nnone", result["report_text"])
            self.assertIn("Why no trade\nOpened a core trade this run.", result["report_text"])

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
            blocked["repricing_verdict"] = "buy_now"
            blocked["primary_entity_key"] = "israel"

            result = run_paper_cycle(
                [_market(0.10)],
                [blocked],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:05:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(summary["buy_now_count"], 0)
            self.assertEqual(summary["idea_preview"], [])
            self.assertIn("Next executable trade\nnone", result["report_text"])
            self.assertIn("Theme cap blocked a repeat trade in the same story.", result["report_text"])

    def test_blocks_non_whitelisted_lane_from_core_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            china = _candidate(0.10)
            china["question"] = "Will China invade Taiwan by end of 2026?"
            china["domain_action_family"] = "conflict"
            china["catalyst_type"] = "military_action"
            china["repricing_lane_key"] = "conflict_fast"
            china["repricing_lane_label"] = "Conflict fast lane"

            result = run_paper_cycle(
                [_market(0.10)],
                [china],
                state_dir=tmpdir,
                generated_at_utc="2026-03-15 12:00:00 UTC",
            )
            summary = result["summary"]
            self.assertEqual(len(summary["opened"]), 0)
            self.assertEqual(summary["buy_now_count"], 0)
            self.assertIn("No candidate landed in the active core lanes.", result["report_text"])

    def test_non_selected_consistency_candidate_can_open_in_core_mode(self):
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
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["buy_now_count"], 1)

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

    def test_non_selected_next_buyer_candidate_can_open_in_core_mode(self):
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
            self.assertEqual(len(summary["opened"]), 1)
            self.assertEqual(summary["buy_now_count"], 1)

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
