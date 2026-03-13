import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exit_policy import live_exit_plan, should_execute_repricing_trade, simulate_exit


class ExitPolicyTests(unittest.TestCase):
    def test_executes_only_buy_now(self):
        self.assertTrue(should_execute_repricing_trade("buy_now"))
        self.assertFalse(should_execute_repricing_trade("watch"))

    def test_conflict_trade_hits_take_profit(self):
        result = simulate_exit(
            [
                (1_000, 0.10),
                (2_000, 0.20),
            ],
            entry_ts=1_000,
            settle_ts=10_000,
            entry_price=0.10,
            action_family="conflict",
            repricing_verdict="buy_now",
            catalyst_type="military_action",
        )
        self.assertEqual(result["exit_reason"], "take_profit")
        self.assertGreater(result["exit_return_pct"], 0.0)

    def test_release_hearing_plan_uses_fast_profile(self):
        plan = live_exit_plan(
            "release",
            repricing_verdict="buy_now",
            entry_price=0.125,
            catalyst_type="hearing",
        )
        self.assertEqual(plan["policy_name"], "release_hearing_fast")
        self.assertGreater(plan["take_profit_price"], 0.125)


if __name__ == "__main__":
    unittest.main()
