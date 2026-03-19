import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from latent_state import annotate_latent_states


def _candidate(question, *, market_key, action_family, entry):
    return {
        "question": question,
        "market_key": market_key,
        "domain_action_family": action_family,
        "entry": entry,
        "confidence": 0.78,
        "repricing_fresh_catalyst_score": 0.0,
        "repricing_underreaction_score": 0.0,
        "repricing_attention_gap": 0.0,
        "repricing_already_priced_penalty": 0.0,
        "repricing_trend_chase_penalty": 0.0,
        "repricing_recent_runup": 0.0,
        "next_buyer_edge": 0.0,
        "next_buyer_score": 0.0,
        "next_buyer_selected": False,
        "consistency_residual": 0.0,
        "consistency_selected": False,
        "regime_actual_score": 0.0,
        "regime_implied_score": 0.0,
        "catalyst_type": None,
        "meeting_subtype": None,
        "repricing_score": 0.7,
    }


class LatentStateTests(unittest.TestCase):
    def test_selects_underpriced_conflict_candidate_with_positive_state_gap(self):
        cheap = _candidate(
            "Will China invade Taiwan by end of 2026?",
            market_key="cheap",
            action_family="conflict",
            entry=0.08,
        )
        cheap["repricing_fresh_catalyst_score"] = 0.84
        cheap["repricing_underreaction_score"] = 0.74
        cheap["repricing_attention_gap"] = 0.72
        cheap["next_buyer_edge"] = 0.18
        cheap["next_buyer_score"] = 0.76
        cheap["next_buyer_selected"] = True
        cheap["consistency_residual"] = 0.08
        cheap["consistency_selected"] = True
        cheap["regime_actual_score"] = 0.80

        chased = _candidate(
            "Will Russia strike Poland by June 30?",
            market_key="chased",
            action_family="conflict",
            entry=0.24,
        )
        chased["repricing_fresh_catalyst_score"] = 0.66
        chased["repricing_underreaction_score"] = 0.42
        chased["repricing_attention_gap"] = 0.32
        chased["repricing_already_priced_penalty"] = 0.44
        chased["repricing_trend_chase_penalty"] = 0.34
        chased["repricing_recent_runup"] = 0.46
        chased["next_buyer_edge"] = 0.04
        chased["next_buyer_score"] = 0.51

        routes = annotate_latent_states([cheap, chased])

        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0]["latent_state_name"], "regional_escalation_state")
        self.assertEqual(routes[0]["selected_market_key"], "cheap")
        self.assertTrue(cheap["latent_state_selected"])
        self.assertGreater(cheap["latent_state_gap_score"], chased["latent_state_gap_score"])

    def test_groups_legal_release_markets_under_legal_activation_state(self):
        early = _candidate(
            "Will the Supreme Court rule by January 31?",
            market_key="jan31",
            action_family="release",
            entry=0.12,
        )
        early["catalyst_type"] = "hearing"
        early["repricing_fresh_catalyst_score"] = 0.72
        early["next_buyer_edge"] = 0.12
        early["next_buyer_score"] = 0.68

        late = _candidate(
            "Will the Supreme Court rule by March 31?",
            market_key="mar31",
            action_family="release",
            entry=0.26,
        )
        late["catalyst_type"] = "appeal"
        late["repricing_already_priced_penalty"] = 0.38
        late["repricing_recent_runup"] = 0.40

        routes = annotate_latent_states([early, late])

        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0]["latent_state_name"], "legal_activation_state")
        self.assertTrue(early["latent_state_supported"])
        self.assertEqual(late["latent_state_name"], "legal_activation_state")

    def test_leaves_unmapped_candidates_unsupported(self):
        generic = _candidate(
            "Will Bitcoin hit $200k by year end?",
            market_key="btc",
            action_family="generic_repricing",
            entry=0.19,
        )

        routes = annotate_latent_states([generic])

        self.assertEqual(routes, [])
        self.assertFalse(generic["latent_state_supported"])
        self.assertIsNone(generic["latent_state_name"])
        self.assertFalse(generic["latent_state_selected"])


if __name__ == "__main__":
    unittest.main()
