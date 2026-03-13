import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from meeting_subtype import infer_meeting_subtype, meeting_subtype_label


class MeetingSubtypeTests(unittest.TestCase):
    def test_detects_talk_call(self):
        subtype = infer_meeting_subtype(
            "Will Trump talk to Vladimir Putin in February?",
            catalyst_type="call_or_meeting",
        )
        self.assertEqual(subtype, "talk_call")

    def test_detects_meeting(self):
        subtype = infer_meeting_subtype(
            "Will Trump meet with Mohammed bin Salman in February 2026?",
            catalyst_type="call_or_meeting",
        )
        self.assertEqual(subtype, "meeting")

    def test_detects_resume_talks(self):
        subtype = infer_meeting_subtype(
            "US x Iran nuclear talks resume before August?",
            catalyst_type="call_or_meeting",
        )
        self.assertEqual(subtype, "resume_talks")

    def test_returns_human_label(self):
        self.assertEqual(meeting_subtype_label("talk_call"), "Talk / call setup")


if __name__ == "__main__":
    unittest.main()
