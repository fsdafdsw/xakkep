import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from surface_router import annotate_surface_routes


def _candidate(question, *, market_key, thesis_id, thesis_type, dimension_value, entry, repricing_score=0.8, watch_score=0.8):
    return {
        "question": question,
        "market_key": market_key,
        "thesis_id": thesis_id,
        "thesis_type": thesis_type,
        "thesis_dimension_value": dimension_value,
        "thesis_dimension_label": str(dimension_value),
        "entry": entry,
        "repricing_score": repricing_score,
        "repricing_watch_score": watch_score,
        "repricing_lane_prior": 0.7,
        "confidence": 0.8,
        "repricing_verdict": "watch_high_upside",
    }


class SurfaceRouterTests(unittest.TestCase):
    def test_selects_underpriced_middle_threshold(self):
        low = _candidate(
            "Will Israel strike 2 countries in 2026?",
            market_key="m2",
            thesis_id="threshold_ladder:test",
            thesis_type="threshold_ladder",
            dimension_value=2,
            entry=0.20,
        )
        mid = _candidate(
            "Will Israel strike 6 countries in 2026?",
            market_key="m6",
            thesis_id="threshold_ladder:test",
            thesis_type="threshold_ladder",
            dimension_value=6,
            entry=0.05,
        )
        high = _candidate(
            "Will Israel strike 15 countries in 2026?",
            market_key="m15",
            thesis_id="threshold_ladder:test",
            thesis_type="threshold_ladder",
            dimension_value=15,
            entry=0.04,
        )

        routes = annotate_surface_routes([low, mid, high])

        self.assertEqual(len(routes), 1)
        self.assertTrue(mid["thesis_surface_selected"])
        self.assertEqual(routes[0]["selected_market_key"], "m6")
        self.assertGreater(mid["thesis_surface_residual"], 0.0)
        self.assertEqual(mid["thesis_surface_rank"], 1)

    def test_selects_underpriced_middle_deadline(self):
        early = _candidate(
            "Released by January 31?",
            market_key="jan31",
            thesis_id="deadline_ladder:test",
            thesis_type="deadline_ladder",
            dimension_value=20260131,
            entry=0.18,
        )
        middle = _candidate(
            "Released by February 20?",
            market_key="feb20",
            thesis_id="deadline_ladder:test",
            thesis_type="deadline_ladder",
            dimension_value=20260220,
            entry=0.20,
        )
        late = _candidate(
            "Released by March 31?",
            market_key="mar31",
            thesis_id="deadline_ladder:test",
            thesis_type="deadline_ladder",
            dimension_value=20260331,
            entry=0.50,
        )

        routes = annotate_surface_routes([late, middle, early])

        self.assertEqual(len(routes), 1)
        self.assertTrue(middle["thesis_surface_selected"])
        self.assertEqual(routes[0]["selected_market_key"], "feb20")
        self.assertGreater(middle["thesis_surface_residual"], 0.0)


if __name__ == "__main__":
    unittest.main()
