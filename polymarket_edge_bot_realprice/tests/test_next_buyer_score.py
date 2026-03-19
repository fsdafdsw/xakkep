import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from next_buyer_score import annotate_next_buyer_scores


def _candidate(question, *, market_key, thesis_id, entry):
    return {
        "question": question,
        "market_key": market_key,
        "thesis_id": thesis_id,
        "thesis_type": "threshold_ladder",
        "entry": entry,
        "default_contract_supported": True,
        "attention_flow_supported": True,
        "consistency_engine_supported": True,
        "default_contract_score": 0.0,
        "attention_capture_score": 0.0,
        "consistency_residual": 0.0,
        "regime_gap_score": 0.0,
        "regime_transition_quality": 0.0,
        "repricing_already_priced_penalty": 0.0,
        "repricing_trend_chase_penalty": 0.0,
        "repricing_lane_prior": 0.65,
        "default_contract_selected": False,
        "attention_flow_selected": False,
        "consistency_selected": False,
        "regime_selected": False,
    }


class NextBuyerScoreTests(unittest.TestCase):
    def test_selects_candidate_with_best_combined_distribution_and_structure(self):
        cheap = _candidate("Cheap leader", market_key="cheap", thesis_id="thesis:test", entry=0.08)
        cheap["default_contract_score"] = 0.82
        cheap["attention_capture_score"] = 0.78
        cheap["consistency_residual"] = 0.08
        cheap["regime_gap_score"] = 0.16
        cheap["regime_transition_quality"] = 0.74
        cheap["default_contract_selected"] = True
        cheap["attention_flow_selected"] = True
        cheap["consistency_selected"] = True

        chased = _candidate("Chased contract", market_key="chased", thesis_id="thesis:test", entry=0.24)
        chased["default_contract_score"] = 0.75
        chased["attention_capture_score"] = 0.73
        chased["consistency_residual"] = 0.02
        chased["regime_gap_score"] = 0.04
        chased["regime_transition_quality"] = 0.48
        chased["repricing_already_priced_penalty"] = 0.42
        chased["repricing_trend_chase_penalty"] = 0.30

        routes = annotate_next_buyer_scores([cheap, chased])

        self.assertEqual(len(routes), 1)
        self.assertTrue(cheap["next_buyer_selected"])
        self.assertEqual(routes[0]["selected_market_key"], "cheap")
        self.assertGreater(cheap["next_buyer_edge"], chased["next_buyer_edge"])

    def test_leaves_selection_empty_when_all_candidates_are_overheated(self):
        hot = _candidate("Hot contract", market_key="hot", thesis_id="thesis:hot", entry=0.28)
        hot["default_contract_score"] = 0.52
        hot["attention_capture_score"] = 0.50
        hot["repricing_already_priced_penalty"] = 0.50
        hot["repricing_trend_chase_penalty"] = 0.36

        hotter = _candidate("Hotter contract", market_key="hotter", thesis_id="thesis:hot", entry=0.30)
        hotter["default_contract_score"] = 0.55
        hotter["attention_capture_score"] = 0.53
        hotter["repricing_already_priced_penalty"] = 0.56
        hotter["repricing_trend_chase_penalty"] = 0.42

        routes = annotate_next_buyer_scores([hot, hotter])

        self.assertEqual(len(routes), 1)
        self.assertIsNone(routes[0]["selected_market_key"])
        self.assertFalse(hot["next_buyer_selected"])
        self.assertFalse(hotter["next_buyer_selected"])


if __name__ == "__main__":
    unittest.main()
