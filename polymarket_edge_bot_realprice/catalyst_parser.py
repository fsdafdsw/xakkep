import re
from functools import lru_cache


_HEARING_KEYWORDS = (
    "hearing",
    "court hearing",
    "appeals court",
    "supreme court",
    "oral arguments",
)
_APPEAL_KEYWORDS = (
    "appeal",
    "appeals",
    "appealed",
)
_COURT_RULING_KEYWORDS = (
    "court ruling",
    "ruling",
    "verdict",
    "judgment",
    "sentence",
    "sentenced",
    "tribunal",
)
_HOSTAGE_RELEASE_KEYWORDS = (
    "hostage",
    "hostages",
    "prisoner swap",
    "exchange",
    "swap",
)
_RELEASE_KEYWORDS = (
    "release",
    "released",
    "freed",
    "pardon",
    "pardoned",
    "bail",
    "parole",
    "amnesty",
    "clemency",
    "extradite",
    "extradited",
    "extradition",
    "detention",
    "custody",
    "imprisoned",
    "jailed",
)
_SUMMIT_KEYWORDS = (
    "summit",
    "visit",
    "state visit",
)
_MEETING_KEYWORDS = (
    "meet",
    "meets",
    "meeting",
    "talk",
    "talks",
    "call",
    "calls",
)
_CEASEFIRE_KEYWORDS = (
    "ceasefire",
    "truce",
    "peace deal",
    "peace agreement",
    "peace accord",
    "humanitarian pause",
    "pause in fighting",
    "halt fighting",
    "stop fighting",
    "end hostilities",
    "hostilities end",
    "armistice",
    "de-escalation",
    "de-escalate",
    "brokered",
    "mediated",
    "accord",
)
_NEGOTIATION_KEYWORDS = (
    "negotiation",
    "negotiations",
    "deal",
    "agreement",
)
_SANCTIONS_KEYWORDS = (
    "sanction",
    "sanctions",
    "tariff",
    "tariffs",
)
_REGIME_SHIFT_KEYWORDS = (
    "resign",
    "resignation",
    "step down",
    "ousted",
    "removed",
    "removed from office",
    "impeached",
    "impeachment",
    "succession",
    "out by",
)
_MILITARY_ACTION_KEYWORDS = (
    "strike",
    "strikes",
    "attack",
    "attacks",
    "missile",
    "missiles",
    "raid",
    "airstrike",
    "retaliation",
    "retaliate",
    "offensive",
    "invade",
    "invasion",
    "bombing",
)
_OFFICIAL_SOURCE_KEYWORDS = (
    "court",
    "judge",
    "ministry",
    "foreign ministry",
    "state department",
    "white house",
    "department of justice",
    "secretary of state",
    "supreme court",
    "appeals court",
    "tribunal",
    "united nations",
    "u.n.",
)
_DEADLINE_RE = re.compile(
    r"\b(by|before|on|until|through)\b|\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|q[1-4])\b",
    re.IGNORECASE,
)

_CATALYST_RULES = (
    {
        "type": "court_ruling",
        "family": "release",
        "keywords": _COURT_RULING_KEYWORDS,
        "strength": 0.92,
        "hardness": "hard",
        "reversibility": "low",
    },
    {
        "type": "appeal",
        "family": "release",
        "keywords": _APPEAL_KEYWORDS,
        "strength": 0.84,
        "hardness": "hard",
        "reversibility": "medium",
    },
    {
        "type": "hearing",
        "family": "release",
        "keywords": _HEARING_KEYWORDS,
        "strength": 0.82,
        "hardness": "hard",
        "reversibility": "medium",
    },
    {
        "type": "hostage_release",
        "family": "release",
        "keywords": _HOSTAGE_RELEASE_KEYWORDS,
        "strength": 0.88,
        "hardness": "hard",
        "reversibility": "medium",
    },
    {
        "type": "release",
        "family": "release",
        "keywords": _RELEASE_KEYWORDS,
        "strength": 0.78,
        "hardness": "hard",
        "reversibility": "medium",
    },
    {
        "type": "summit",
        "family": "diplomacy",
        "keywords": _SUMMIT_KEYWORDS,
        "strength": 0.78,
        "hardness": "soft",
        "reversibility": "high",
    },
    {
        "type": "call_or_meeting",
        "family": "diplomacy",
        "keywords": _MEETING_KEYWORDS,
        "strength": 0.68,
        "hardness": "soft",
        "reversibility": "high",
    },
    {
        "type": "ceasefire",
        "family": "diplomacy",
        "keywords": _CEASEFIRE_KEYWORDS,
        "strength": 0.80,
        "hardness": "soft",
        "reversibility": "medium",
    },
    {
        "type": "negotiation",
        "family": "diplomacy",
        "keywords": _NEGOTIATION_KEYWORDS,
        "strength": 0.70,
        "hardness": "soft",
        "reversibility": "high",
    },
    {
        "type": "sanctions",
        "family": "diplomacy",
        "keywords": _SANCTIONS_KEYWORDS,
        "strength": 0.83,
        "hardness": "hard",
        "reversibility": "medium",
    },
    {
        "type": "regime_shift",
        "family": "regime_shift",
        "keywords": _REGIME_SHIFT_KEYWORDS,
        "strength": 0.85,
        "hardness": "hard",
        "reversibility": "low",
    },
    {
        "type": "military_action",
        "family": "conflict",
        "keywords": _MILITARY_ACTION_KEYWORDS,
        "strength": 0.87,
        "hardness": "hard",
        "reversibility": "low",
    },
)


def normalize_text(*parts):
    text = " ".join(str(part or "") for part in parts)
    return " ".join(text.lower().split())


@lru_cache(maxsize=512)
def _keyword_pattern(keyword):
    escaped = r"\s+".join(re.escape(part) for part in keyword.split())
    return re.compile(rf"(?<!\w){escaped}(?!\w)", re.IGNORECASE)


def _match_keywords(text, keywords):
    matches = []
    for keyword in keywords:
        if _keyword_pattern(keyword).search(text):
            matches.append(keyword)
    return matches


def parse_catalyst(*parts):
    text = normalize_text(*parts)
    has_deadline = bool(_DEADLINE_RE.search(text))
    official_source_keywords = _match_keywords(text, _OFFICIAL_SOURCE_KEYWORDS)
    source_strength = 0.88 if official_source_keywords else (0.58 if has_deadline else 0.42)

    matched_rule = None
    matched_keywords = []
    for rule in _CATALYST_RULES:
        hits = _match_keywords(text, rule["keywords"])
        if hits:
            matched_rule = rule
            matched_keywords = hits
            break

    if matched_rule is None:
        return {
            "text": text,
            "catalyst_type": "generic",
            "catalyst_family": "generic",
            "catalyst_keywords": [],
            "catalyst_strength": 0.45 if has_deadline else 0.35,
            "hardness": "soft",
            "reversibility": "high",
            "has_deadline": has_deadline,
            "has_official_source": bool(official_source_keywords),
            "official_source_keywords": official_source_keywords,
            "source_strength": source_strength,
        }

    catalyst_strength = matched_rule["strength"]
    if has_deadline:
        catalyst_strength += 0.04
    if official_source_keywords:
        catalyst_strength += 0.05

    return {
        "text": text,
        "catalyst_type": matched_rule["type"],
        "catalyst_family": matched_rule["family"],
        "catalyst_keywords": matched_keywords,
        "catalyst_strength": min(0.99, catalyst_strength),
        "hardness": matched_rule["hardness"],
        "reversibility": matched_rule["reversibility"],
        "has_deadline": has_deadline,
        "has_official_source": bool(official_source_keywords),
        "official_source_keywords": official_source_keywords,
        "source_strength": source_strength,
    }
