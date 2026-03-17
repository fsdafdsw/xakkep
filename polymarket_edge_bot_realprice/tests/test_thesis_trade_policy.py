import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from thesis_trade_policy import can_open_thesis_trade, register_closed_thesis


class ThesisTradePolicyTests(unittest.TestCase):
    def test_blocks_when_same_thesis_position_is_open(self):
        state = {
            "positions": [
                {
                    "market_key": "m1",
                    "thesis_id": "threshold_ladder:abc",
                }
            ],
            "recently_closed_theses": {},
        }
        candidate = {
            "market_key": "m2",
            "thesis_id": "threshold_ladder:abc",
        }
        gate = can_open_thesis_trade(
            state,
            candidate,
            now_ts=1_000,
            thesis_cooldown_minutes=240,
        )
        self.assertFalse(gate["allowed"])
        self.assertEqual(gate["blocked_reason"], "thesis_position_open")

    def test_blocks_when_same_thesis_is_in_cooldown(self):
        state = {
            "positions": [],
            "recently_closed_theses": {
                "threshold_ladder:abc": 1_000,
            },
        }
        candidate = {
            "market_key": "m2",
            "thesis_id": "threshold_ladder:abc",
        }
        gate = can_open_thesis_trade(
            state,
            candidate,
            now_ts=1_000 + (60 * 30),
            thesis_cooldown_minutes=240,
        )
        self.assertFalse(gate["allowed"])
        self.assertEqual(gate["blocked_reason"], "thesis_cooldown")

    def test_registers_closed_thesis_timestamp(self):
        state = {"recently_closed_theses": {}}
        thesis_id = register_closed_thesis(
            state,
            {"market_key": "m1", "thesis_id": "deadline_ladder:def"},
            closed_ts=1234,
        )
        self.assertEqual(thesis_id, "deadline_ladder:def")
        self.assertEqual(state["recently_closed_theses"]["deadline_ladder:def"], 1234)


if __name__ == "__main__":
    unittest.main()
