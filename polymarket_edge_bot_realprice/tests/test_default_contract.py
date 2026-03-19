import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from default_contract import annotate_default_contracts


def _candidate(question, *, market_key, thesis_id, thesis_type, dimension_value, entry=0.10):
    return {
        "question": question,
        "market_key": market_key,
        "thesis_id": thesis_id,
        "thesis_type": thesis_type,
        "thesis_dimension_value": dimension_value,
        "thesis_dimension_label": str(dimension_value),
        "entry": entry,
        "repricing_lane_prior": 0.65,
    }


class DefaultContractTests(unittest.TestCase):
    def test_selects_middle_threshold_contract(self):
        low = _candidate(
            "Will Israel strike 2 countries in 2026?",
            market_key="m2",
            thesis_id="threshold_ladder:test",
            thesis_type="threshold_ladder",
            dimension_value=2,
        )
        middle = _candidate(
            "Will Israel strike 6 countries in 2026?",
            market_key="m6",
            thesis_id="threshold_ladder:test",
            thesis_type="threshold_ladder",
            dimension_value=6,
        )
        high = _candidate(
            "Will Israel strike 15 countries in 2026?",
            market_key="m15",
            thesis_id="threshold_ladder:test",
            thesis_type="threshold_ladder",
            dimension_value=15,
        )

        routes = annotate_default_contracts([low, middle, high])

        self.assertEqual(len(routes), 1)
        self.assertTrue(middle["default_contract_selected"])
        self.assertEqual(middle["default_contract_rank"], 1)
        self.assertGreater(middle["threshold_centrality_score"], low["threshold_centrality_score"])
        self.assertEqual(routes[0]["selected_market_key"], "m6")

    def test_selects_middle_deadline_contract(self):
        early = _candidate(
            "Will the Supreme Court rule by January 31?",
            market_key="jan31",
            thesis_id="deadline_ladder:test",
            thesis_type="deadline_ladder",
            dimension_value=20260131,
        )
        middle = _candidate(
            "Will the Supreme Court rule by February 20?",
            market_key="feb20",
            thesis_id="deadline_ladder:test",
            thesis_type="deadline_ladder",
            dimension_value=20260220,
        )
        late = _candidate(
            "Will the Supreme Court rule by March 31?",
            market_key="mar31",
            thesis_id="deadline_ladder:test",
            thesis_type="deadline_ladder",
            dimension_value=20260331,
        )

        routes = annotate_default_contracts([late, middle, early])

        self.assertEqual(len(routes), 1)
        self.assertTrue(middle["default_contract_selected"])
        self.assertEqual(middle["default_contract_rank"], 1)
        self.assertGreater(middle["deadline_centrality_score"], early["deadline_centrality_score"])
        self.assertEqual(routes[0]["selected_market_key"], "feb20")

    def test_penalizes_needlessly_complex_question(self):
        simple = _candidate(
            "Will Trump meet Putin by June 30?",
            market_key="simple",
            thesis_id="deadline_ladder:complexity",
            thesis_type="deadline_ladder",
            dimension_value=20260630,
        )
        complex_row = _candidate(
            "Will Trump, after talks with European leaders, meet with Vladimir Putin by June 30, 2026?",
            market_key="complex",
            thesis_id="deadline_ladder:complexity",
            thesis_type="deadline_ladder",
            dimension_value=20260630,
        )

        annotate_default_contracts([simple, complex_row])

        self.assertGreater(simple["headline_fit_score"], 0.0)
        self.assertGreater(complex_row["question_complexity_penalty"], simple["question_complexity_penalty"])


if __name__ == "__main__":
    unittest.main()
