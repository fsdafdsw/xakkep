import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from repricing_selector import score_repricing_signal


def _geo_model(action_family, catalyst_type, **extra_components):
    components = {
        "action_family": action_family,
        "catalyst_type": catalyst_type,
        "catalyst_strength": 0.9,
        "catalyst_hardness": "hard",
        "catalyst_reversibility": "low",
        "catalyst_has_official_source": True,
        "repricing_potential": 0.9,
        "liquidity": 500.0,
        "volume24h": 500.0,
        "hours_to_close": 24.0,
    }
    components.update(extra_components)
    return {
        "domain_name": "geopolitical_repricing",
        "domain_confidence": 0.92,
        "external_components": {
            "domain": {"components": components},
            "relation_residual": {"support_confidence": 0.7, "residual": 0.02},
        },
    }


class RepricingSelectorTests(unittest.TestCase):
    def test_non_repricing_domain_is_ignored(self):
        result = score_repricing_signal(
            entry_price=0.10,
            confidence=0.9,
            net_edge=0.02,
            net_edge_lcb=0.01,
            spread=0.01,
            model={"domain_name": "other"},
            question="Will something happen?",
        )
        self.assertEqual(result["verdict"], "ignore")

    def test_conflict_case_can_be_buy_now(self):
        result = score_repricing_signal(
            entry_price=0.06,
            confidence=0.93,
            net_edge=0.02,
            net_edge_lcb=0.01,
            spread=0.01,
            one_hour_change=0.0,
            one_day_change=0.0,
            one_week_change=0.0,
            hours_to_close=12.0,
            model=_geo_model("conflict", "military_action", hard_state=True),
            category_group="geopolitics",
            question="Will Israel strike Gaza by Friday?",
        )
        self.assertEqual(result["lane_key"], "conflict_fast")
        self.assertEqual(result["verdict"], "buy_now")

    def test_talk_call_stays_watch_family(self):
        result = score_repricing_signal(
            entry_price=0.08,
            confidence=0.86,
            net_edge=0.005,
            net_edge_lcb=-0.003,
            spread=0.01,
            one_hour_change=0.0,
            one_day_change=0.0,
            one_week_change=0.0,
            hours_to_close=72.0,
            model=_geo_model(
                "diplomacy",
                "call_or_meeting",
                catalyst_hardness="soft",
                catalyst_reversibility="high",
                catalyst_has_official_source=False,
                repricing_potential=0.88,
            ),
            category_group="geopolitics",
            question="Will Trump talk to Vladimir Putin in February?",
        )
        self.assertEqual(result["meeting_subtype"], "talk_call")
        self.assertIn(result["verdict"], {"watch", "watch_high_upside", "watch_late"})
        self.assertNotEqual(result["verdict"], "buy_now")


if __name__ == "__main__":
    unittest.main()
