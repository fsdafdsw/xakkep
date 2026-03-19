import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from portfolio_admission import can_open_portfolio_trade, portfolio_theme_key, register_closed_trade


def _candidate(question="Will Israel strike 2 countries in 2026?"):
    return {
        "question": question,
        "domain_action_family": "conflict",
        "repricing_lane_key": "conflict_fast",
        "primary_entity_key": "israel",
        "event_slug": "how-many-different-countries-will-israel-strike-in-2026",
        "market_key": "mkt",
    }


class PortfolioAdmissionTests(unittest.TestCase):
    def test_theme_key_uses_primary_entity(self):
        candidate = _candidate()
        self.assertEqual(portfolio_theme_key(candidate), "conflict:israel")

    def test_blocks_second_open_in_same_theme(self):
        candidate = _candidate()
        state = {
            "positions": [
                {
                    "question": "Existing conflict",
                    "action_family": "conflict",
                    "lane_key": "conflict_fast",
                    "primary_entity_key": "israel",
                    "event_slug": "other-event",
                }
            ],
            "closed_trade_memory": [],
        }
        result = can_open_portfolio_trade(
            state,
            candidate,
            max_open_per_theme=1,
            max_conflict_open_positions=2,
            lane_recent_trades=6,
            lane_kill_min_trades=3,
            lane_kill_max_mean_pnl_usd=-0.05,
            lane_kill_loss_streak=3,
            theme_recent_trades=4,
            theme_kill_min_trades=2,
            theme_kill_max_mean_pnl_usd=-0.04,
        )
        self.assertFalse(result["allowed"])
        self.assertEqual(result["blocked_reason"], "theme_cap")

    def test_blocks_conflict_lane_after_loss_streak(self):
        candidate = _candidate()
        state = {"positions": [], "closed_trade_memory": []}
        for pnl in (-0.10, -0.08, -0.06):
            register_closed_trade(
                state,
                {
                    "question": "Past loss",
                    "lane_key": "conflict_fast",
                    "action_family": "conflict",
                    "primary_entity_key": "russia",
                    "event_slug": "past",
                    "trade_mode": "scout",
                },
                pnl_usd=pnl,
                closed_ts=1,
            )
        result = can_open_portfolio_trade(
            state,
            candidate,
            max_open_per_theme=1,
            max_conflict_open_positions=2,
            lane_recent_trades=6,
            lane_kill_min_trades=3,
            lane_kill_max_mean_pnl_usd=-0.05,
            lane_kill_loss_streak=3,
            theme_recent_trades=4,
            theme_kill_min_trades=2,
            theme_kill_max_mean_pnl_usd=-0.04,
        )
        self.assertFalse(result["allowed"])
        self.assertIn(result["blocked_reason"], {"lane_expectancy_kill", "lane_loss_streak"})

    def test_allows_trade_when_portfolio_is_clean(self):
        candidate = _candidate(question="Will China invade Taiwan by end of 2026?")
        candidate["primary_entity_key"] = "china"
        state = {"positions": [], "closed_trade_memory": []}
        result = can_open_portfolio_trade(
            state,
            candidate,
            max_open_per_theme=1,
            max_conflict_open_positions=2,
            lane_recent_trades=6,
            lane_kill_min_trades=3,
            lane_kill_max_mean_pnl_usd=-0.05,
            lane_kill_loss_streak=3,
            theme_recent_trades=4,
            theme_kill_min_trades=2,
            theme_kill_max_mean_pnl_usd=-0.04,
        )
        self.assertTrue(result["allowed"])


if __name__ == "__main__":
    unittest.main()
