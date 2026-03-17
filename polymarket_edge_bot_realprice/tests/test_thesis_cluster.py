import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from thesis_cluster import annotate_thesis_clusters


def _candidate(question, *, event_slug, market_key, action_family, entry=0.1):
    return {
        "question": question,
        "event_slug": event_slug,
        "market_key": market_key,
        "event_key": event_slug,
        "domain_action_family": action_family,
        "entry": entry,
    }


class ThesisClusterTests(unittest.TestCase):
    def test_groups_conflict_threshold_ladder(self):
        a = _candidate(
            "Will Israel strike 2 countries in 2026?",
            event_slug="how-many-different-countries-will-israel-strike-in-2026",
            market_key="m2",
            action_family="conflict",
        )
        b = _candidate(
            "Will Israel strike 7 countries in 2026?",
            event_slug="how-many-different-countries-will-israel-strike-in-2026",
            market_key="m7",
            action_family="conflict",
        )
        c = _candidate(
            "Will Israel strike 15 or more countries in 2026?",
            event_slug="how-many-different-countries-will-israel-strike-in-2026",
            market_key="m15",
            action_family="conflict",
        )

        clusters = annotate_thesis_clusters([a, b, c])

        self.assertEqual(len(clusters), 1)
        self.assertEqual(a["thesis_id"], b["thesis_id"])
        self.assertEqual(b["thesis_id"], c["thesis_id"])
        self.assertEqual(a["thesis_type"], "threshold_ladder")
        self.assertEqual(a["thesis_cluster_size"], 3)
        self.assertEqual(a["thesis_member_order"], 1)
        self.assertEqual(b["thesis_member_order"], 2)
        self.assertEqual(c["thesis_member_order"], 3)
        self.assertEqual(a["thesis_dimension_value"], 2)
        self.assertEqual(b["thesis_dimension_value"], 7)
        self.assertEqual(c["thesis_dimension_value"], 15)

    def test_groups_release_deadline_ladder(self):
        jan = _candidate(
            "Will the Supreme Court rule on Trump's tariffs by January 31?",
            event_slug="supreme-court-rule-on-trump-tariffs",
            market_key="jan31",
            action_family="release",
        )
        feb = _candidate(
            "Will the Supreme Court rule on Trump's tariffs by February 20?",
            event_slug="supreme-court-rule-on-trump-tariffs",
            market_key="feb20",
            action_family="release",
        )
        mar = _candidate(
            "Will the Supreme Court rule on Trump's tariffs by March 31?",
            event_slug="supreme-court-rule-on-trump-tariffs",
            market_key="mar31",
            action_family="release",
        )

        annotate_thesis_clusters([mar, feb, jan])

        self.assertEqual(jan["thesis_id"], feb["thesis_id"])
        self.assertEqual(feb["thesis_id"], mar["thesis_id"])
        self.assertEqual(jan["thesis_type"], "deadline_ladder")
        self.assertEqual(jan["thesis_member_order"], 1)
        self.assertEqual(feb["thesis_member_order"], 2)
        self.assertEqual(mar["thesis_member_order"], 3)
        self.assertLess(jan["thesis_dimension_value"], feb["thesis_dimension_value"])
        self.assertLess(feb["thesis_dimension_value"], mar["thesis_dimension_value"])

    def test_keeps_unrelated_markets_in_different_theses(self):
        threshold = _candidate(
            "Will Israel strike 2 countries in 2026?",
            event_slug="how-many-different-countries-will-israel-strike-in-2026",
            market_key="m2",
            action_family="conflict",
        )
        dated = _candidate(
            "Will Israel strike Gaza by Friday?",
            event_slug="how-many-different-countries-will-israel-strike-in-2026",
            market_key="gaza-friday",
            action_family="conflict",
        )

        annotate_thesis_clusters([threshold, dated])

        self.assertNotEqual(threshold["thesis_id"], dated["thesis_id"])
        self.assertEqual(threshold["thesis_type"], "threshold_ladder")
        self.assertEqual(dated["thesis_type"], "standalone")
        self.assertEqual(dated["thesis_cluster_size"], 1)


if __name__ == "__main__":
    unittest.main()
