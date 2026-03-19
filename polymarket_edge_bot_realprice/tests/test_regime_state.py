import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from regime_state import annotate_regime_state


def _candidate(question, *, market_key, thesis_id, entry):
    return {
        "question": question,
        "market_key": market_key,
        "thesis_id": thesis_id,
        "thesis_type": "threshold_ladder",
        "entry": entry,
        "confidence": 0.80,
        "repricing_fresh_catalyst_score": 0.0,
        "repricing_underreaction_score": 0.0,
        "repricing_attention_gap": 0.0,
        "repricing_optionality_score": 0.0,
        "repricing_conflict_urgency_score": 0.0,
        "repricing_release_legitimacy_score": 0.0,
        "repricing_already_priced_penalty": 0.0,
        "repricing_trend_chase_penalty": 0.0,
        "repricing_recent_runup": 0.0,
        "consistency_residual": 0.0,
        "consistency_selected": False,
        "repricing_score": 0.7,
    }


class RegimeStateTests(unittest.TestCase):
    def test_selects_market_when_actual_regime_is_ahead_of_price_implied_regime(self):
        early = _candidate("Cheap contract", market_key="cheap", thesis_id="thesis:test", entry=0.08)
        early["repricing_fresh_catalyst_score"] = 0.88
        early["repricing_underreaction_score"] = 0.76
        early["repricing_attention_gap"] = 0.74
        early["repricing_optionality_score"] = 0.68
        early["repricing_conflict_urgency_score"] = 0.72
        early["consistency_residual"] = 0.09
        early["consistency_selected"] = True

        late = _candidate("Already moved contract", market_key="late", thesis_id="thesis:test", entry=0.27)
        late["repricing_fresh_catalyst_score"] = 0.70
        late["repricing_underreaction_score"] = 0.50
        late["repricing_attention_gap"] = 0.30
        late["repricing_already_priced_penalty"] = 0.42
        late["repricing_trend_chase_penalty"] = 0.38
        late["repricing_recent_runup"] = 0.48

        routes = annotate_regime_state([late, early])

        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0]["actual_regime"], "active")
        self.assertEqual(routes[0]["selected_market_key"], "cheap")
        self.assertTrue(early["regime_selected"])
        self.assertIn(early["regime_trade_window"], {"early", "active"})
        self.assertGreater(early["regime_gap_score"], 0.0)

    def test_marks_late_window_when_price_already_reflects_regime(self):
        chased = _candidate("Chased market", market_key="chased", thesis_id="thesis:late", entry=0.31)
        chased["repricing_fresh_catalyst_score"] = 0.76
        chased["repricing_underreaction_score"] = 0.62
        chased["repricing_attention_gap"] = 0.22
        chased["repricing_already_priced_penalty"] = 0.48
        chased["repricing_trend_chase_penalty"] = 0.44
        chased["repricing_recent_runup"] = 0.52

        routes = annotate_regime_state([chased])

        self.assertEqual(len(routes), 1)
        self.assertEqual(chased["regime_trade_window"], "late")
        self.assertIsNone(routes[0]["selected_market_key"])


if __name__ == "__main__":
    unittest.main()
