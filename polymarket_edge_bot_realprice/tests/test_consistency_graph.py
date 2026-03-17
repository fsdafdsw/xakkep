import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from consistency_graph import annotate_consistency_graphs


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


class ConsistencyGraphTests(unittest.TestCase):
    def test_descending_threshold_violation_marks_wrong_shape(self):
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

        graphs = annotate_consistency_graphs([low, mid, high])

        self.assertEqual(len(graphs), 1)
        self.assertEqual(graphs[0]["direction"], "descending")
        self.assertGreater(graphs[0]["total_violation_score"], 0.0)
        self.assertGreater(mid["consistency_total_violation_score"], 0.0)
        self.assertEqual(mid["consistency_violation_rank"], 1)
        self.assertEqual(graphs[0]["worst_market_key"], "m6")

    def test_ascending_deadline_violation_marks_early_contract(self):
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

        graphs = annotate_consistency_graphs([jan, feb, mar])

        self.assertEqual(len(graphs), 1)
        self.assertEqual(graphs[0]["direction"], "ascending")
        self.assertGreater(graphs[0]["total_violation_score"], 0.0)
        self.assertGreater(jan["consistency_total_violation_score"], 0.0)
        self.assertEqual(jan["consistency_violation_rank"], 1)
        self.assertEqual(graphs[0]["worst_market_key"], "jan31")

    def test_supported_false_for_standalone(self):
        standalone = _candidate(
            "Will Xi leave office before 2027?",
            market_key="xi",
            thesis_id="standalone:test",
            thesis_type="standalone",
            dimension_value=None,
            entry=0.12,
        )

        graphs = annotate_consistency_graphs([standalone])

        self.assertEqual(graphs, [])
        self.assertFalse(standalone["consistency_supported"])
        self.assertEqual(standalone["consistency_total_violation_score"], 0.0)


if __name__ == "__main__":
    unittest.main()
