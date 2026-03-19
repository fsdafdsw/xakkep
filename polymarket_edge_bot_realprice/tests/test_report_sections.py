import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from report_sections import _build_best_watchlist, _build_paper_scout_candidates


def _candidate(question, *, link, verdict="watch_high_upside", watch_score=0.8, repricing_score=0.8, lane_prior=0.6):
    return {
        "question": question,
        "link": link,
        "repricing_verdict": verdict,
        "repricing_watch_score": watch_score,
        "repricing_score": repricing_score,
        "repricing_lane_prior": lane_prior,
        "confidence": 0.75,
        "net_edge": 0.01,
        "thesis_surface_selected": True,
        "thesis_surface_score": 0.5,
        "consistency_selected": False,
        "consistency_residual": 0.0,
        "next_buyer_selected": False,
        "next_buyer_edge": None,
        "next_buyer_score": None,
        "latent_state_selected": False,
        "latent_state_gap_score": None,
    }


class ReportSectionsTests(unittest.TestCase):
    def test_best_watchlist_prefers_consistency_selected(self):
        consistency = _candidate(
            "Consistency-selected setup",
            link="https://example.com/a",
            watch_score=0.70,
            repricing_score=0.70,
        )
        consistency["consistency_selected"] = True
        consistency["consistency_residual"] = 0.09

        stronger_plain = _candidate(
            "Plain stronger watch score",
            link="https://example.com/b",
            watch_score=0.95,
            repricing_score=0.95,
        )

        rows = _build_best_watchlist([stronger_plain, consistency])

        self.assertEqual(rows[0]["question"], "Consistency-selected setup")

    def test_paper_scout_candidates_prefers_consistency_selected(self):
        consistency = _candidate(
            "Consistency-selected buy",
            link="https://example.com/c",
            verdict="buy_now",
            watch_score=0.72,
            repricing_score=0.80,
        )
        consistency["consistency_selected"] = True
        consistency["consistency_residual"] = 0.07

        stronger_plain = _candidate(
            "Plain stronger buy",
            link="https://example.com/d",
            verdict="buy_now",
            watch_score=0.90,
            repricing_score=0.92,
        )

        rows = _build_paper_scout_candidates([stronger_plain, consistency])

        self.assertEqual(rows[0]["question"], "Consistency-selected buy")

    def test_best_watchlist_prefers_next_buyer_selected_when_consistency_is_equal(self):
        next_buyer = _candidate(
            "Next-buyer setup",
            link="https://example.com/e",
            watch_score=0.72,
            repricing_score=0.72,
        )
        next_buyer["next_buyer_selected"] = True
        next_buyer["next_buyer_edge"] = 0.18
        next_buyer["next_buyer_score"] = 0.74

        stronger_plain = _candidate(
            "Plain stronger watch",
            link="https://example.com/f",
            watch_score=0.90,
            repricing_score=0.90,
        )

        rows = _build_best_watchlist([stronger_plain, next_buyer])

        self.assertEqual(rows[0]["question"], "Next-buyer setup")

    def test_best_watchlist_prefers_latent_state_selected_when_other_structure_is_equal(self):
        latent = _candidate(
            "Latent-state winner",
            link="https://example.com/g",
            watch_score=0.71,
            repricing_score=0.71,
        )
        latent["latent_state_selected"] = True
        latent["latent_state_gap_score"] = 0.22

        plain = _candidate(
            "Plain stronger watch",
            link="https://example.com/h",
            watch_score=0.92,
            repricing_score=0.92,
        )

        rows = _build_best_watchlist([plain, latent])

        self.assertEqual(rows[0]["question"], "Latent-state winner")


if __name__ == "__main__":
    unittest.main()
