import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from consistency_engine import annotate_consistency_engine


def _candidate(question, *, market_key, thesis_id, thesis_type, dimension_value, entry):
    return {
        "question": question,
        "market_key": market_key,
        "thesis_id": thesis_id,
        "thesis_type": thesis_type,
        "thesis_dimension_value": dimension_value,
        "thesis_dimension_label": str(dimension_value),
        "entry": entry,
    }


class ConsistencyEngineTests(unittest.TestCase):
    def test_descending_threshold_selects_underpriced_lower_threshold(self):
        low = _candidate(
            "Will Israel strike 2 countries in 2026?",
            market_key="m2",
            thesis_id="threshold:test",
            thesis_type="threshold_ladder",
            dimension_value=2,
            entry=0.04,
        )
        mid = _candidate(
            "Will Israel strike 6 countries in 2026?",
            market_key="m6",
            thesis_id="threshold:test",
            thesis_type="threshold_ladder",
            dimension_value=6,
            entry=0.07,
        )
        high = _candidate(
            "Will Israel strike 15 countries in 2026?",
            market_key="m15",
            thesis_id="threshold:test",
            thesis_type="threshold_ladder",
            dimension_value=15,
            entry=0.03,
        )

        routes = annotate_consistency_engine([low, mid, high])

        self.assertEqual(len(routes), 1)
        self.assertTrue(low["consistency_selected"])
        self.assertEqual(routes[0]["selected_market_key"], "m2")
        self.assertGreater(low["consistency_residual"], 0.0)
        self.assertLess(mid["consistency_residual"], 0.0)
        self.assertEqual(low["consistency_bias"], "underpriced_yes")
        self.assertEqual(mid["consistency_bias"], "overpriced_yes")

    def test_ascending_deadline_selects_underpriced_middle_deadline(self):
        jan = _candidate(
            "Released by January 31?",
            market_key="jan31",
            thesis_id="deadline:test",
            thesis_type="deadline_ladder",
            dimension_value=20260131,
            entry=0.40,
        )
        feb = _candidate(
            "Released by February 20?",
            market_key="feb20",
            thesis_id="deadline:test",
            thesis_type="deadline_ladder",
            dimension_value=20260220,
            entry=0.25,
        )
        mar = _candidate(
            "Released by March 31?",
            market_key="mar31",
            thesis_id="deadline:test",
            thesis_type="deadline_ladder",
            dimension_value=20260331,
            entry=0.55,
        )

        routes = annotate_consistency_engine([jan, feb, mar])

        self.assertEqual(len(routes), 1)
        self.assertTrue(feb["consistency_selected"])
        self.assertEqual(routes[0]["selected_market_key"], "feb20")
        self.assertGreater(feb["consistency_residual"], 0.0)
        self.assertLess(jan["consistency_residual"], 0.0)

    def test_no_selection_when_curve_is_already_consistent(self):
        a = _candidate(
            "Will Israel strike 2 countries in 2026?",
            market_key="m2",
            thesis_id="threshold:fair",
            thesis_type="threshold_ladder",
            dimension_value=2,
            entry=0.20,
        )
        b = _candidate(
            "Will Israel strike 6 countries in 2026?",
            market_key="m6",
            thesis_id="threshold:fair",
            thesis_type="threshold_ladder",
            dimension_value=6,
            entry=0.10,
        )
        c = _candidate(
            "Will Israel strike 15 countries in 2026?",
            market_key="m15",
            thesis_id="threshold:fair",
            thesis_type="threshold_ladder",
            dimension_value=15,
            entry=0.05,
        )

        routes = annotate_consistency_engine([a, b, c])

        self.assertEqual(len(routes), 1)
        self.assertIsNone(routes[0]["selected_market_key"])
        self.assertFalse(a["consistency_selected"])
        self.assertEqual(a["consistency_bias"], "fair")
        self.assertEqual(b["consistency_bias"], "fair")
        self.assertEqual(c["consistency_bias"], "fair")


if __name__ == "__main__":
    unittest.main()
