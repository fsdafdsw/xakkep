import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalyst_parser import parse_catalyst


class CatalystParserTests(unittest.TestCase):
    def test_parses_appeal_release_case(self):
        result = parse_catalyst("Jimmy Lai released by June 30 after appeal hearing?")
        self.assertEqual(result["catalyst_type"], "appeal")
        self.assertEqual(result["catalyst_family"], "release")
        self.assertTrue(result["has_deadline"])

    def test_parses_hostage_release_case(self):
        result = parse_catalyst("Will Hamas release 10 hostages by March 1?")
        self.assertEqual(result["catalyst_type"], "hostage_release")
        self.assertEqual(result["catalyst_family"], "release")
        self.assertEqual(result["hardness"], "hard")

    def test_parses_diplomacy_call_case(self):
        result = parse_catalyst("Will Trump talk to Vladimir Putin in February?")
        self.assertEqual(result["catalyst_type"], "call_or_meeting")
        self.assertEqual(result["catalyst_family"], "diplomacy")


if __name__ == "__main__":
    unittest.main()
