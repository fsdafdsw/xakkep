import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from repricing_context import build_repricing_context


class RepricingContextTests(unittest.TestCase):
    def test_conflict_near_deadline_is_more_urgent_than_distant(self):
        near = build_repricing_context(
            entry_price=0.08,
            repricing_potential=0.82,
            catalyst_strength=0.9,
            action_family="conflict",
            catalyst_type="military_action",
            market_type="dated_binary",
            hours_to_close=24.0,
        )
        far = build_repricing_context(
            entry_price=0.08,
            repricing_potential=0.82,
            catalyst_strength=0.9,
            action_family="conflict",
            catalyst_type="military_action",
            market_type="dated_binary",
            hours_to_close=24.0 * 120.0,
        )
        self.assertGreater(near["deadline_pressure"], far["deadline_pressure"])
        self.assertEqual(near["urgency_phase"], "imminent")

    def test_talk_call_prefers_active_window_over_distant(self):
        active = build_repricing_context(
            entry_price=0.10,
            repricing_potential=0.74,
            catalyst_strength=0.70,
            action_family="diplomacy",
            catalyst_type="call_or_meeting",
            meeting_subtype="talk_call",
            market_type="dated_binary",
            hours_to_close=24.0 * 10.0,
        )
        distant = build_repricing_context(
            entry_price=0.10,
            repricing_potential=0.74,
            catalyst_strength=0.70,
            action_family="diplomacy",
            catalyst_type="call_or_meeting",
            meeting_subtype="talk_call",
            market_type="dated_binary",
            hours_to_close=24.0 * 150.0,
        )
        self.assertGreater(active["deadline_pressure"], distant["deadline_pressure"])
        self.assertEqual(active["urgency_phase"], "active_window")


if __name__ == "__main__":
    unittest.main()
