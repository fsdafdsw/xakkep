import re
from functools import lru_cache


_GEO_KEYWORDS = (
    "china",
    "hong kong",
    "taiwan",
    "russia",
    "ukraine",
    "iran",
    "israel",
    "gaza",
    "hamas",
    "hezbollah",
    "nato",
    "beijing",
    "tehran",
    "moscow",
    "kyiv",
    "taipei",
    "north korea",
    "khamenei",
    "ali khamenei",
    "jimmy lai",
    "xi jinping",
    "putin",
    "zelensky",
    "netanyahu",
)

_RELEASE_ACTION_KEYWORDS = (
    "released",
    "release",
    "freed",
    "pardon",
    "pardoned",
    "hostage",
    "prisoner",
    "detained",
)

_DIPLOMACY_ACTION_KEYWORDS = (
    "ceasefire",
    "truce",
    "talks",
    "deal",
    "summit",
    "visit",
    "meeting",
    "sanction",
    "sanctions",
    "tariff",
    "tariffs",
    "negotiation",
    "negotiations",
)

_REGIME_ACTION_KEYWORDS = (
    "resign",
    "resignation",
    "step down",
    "ousted",
    "removed",
    "removed from office",
    "out by",
    "supreme leader",
)

_CONFLICT_ACTION_KEYWORDS = (
    "strike",
    "strikes",
    "attack",
    "attacks",
    "missile",
    "missiles",
    "invasion",
    "invade",
    "bomb",
    "bombing",
    "offensive",
)

_INSTITUTION_KEYWORDS = (
    "court",
    "hearing",
    "appeal",
    "judge",
    "ministry",
    "foreign ministry",
    "state department",
    "white house",
    "department of justice",
    "secretary of state",
    "united nations",
    "u.n.",
    "eu",
    "european union",
)

_HARD_STATE_KEYWORDS = ("china", "hong kong", "russia", "iran", "north korea")


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


def _has_deadline(text):
    if not text:
        return False
    if any(token in text for token in (" by ", " before ", " on ", " until ", " through ")):
        return True
    return bool(re.search(r"\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|q[1-4])\b", text))


def build_geopolitical_context(*parts):
    text = normalize_text(*parts)
    geo_matches = _match_keywords(text, _GEO_KEYWORDS)
    release_matches = _match_keywords(text, _RELEASE_ACTION_KEYWORDS)
    diplomacy_matches = _match_keywords(text, _DIPLOMACY_ACTION_KEYWORDS)
    regime_matches = _match_keywords(text, _REGIME_ACTION_KEYWORDS)
    conflict_matches = _match_keywords(text, _CONFLICT_ACTION_KEYWORDS)
    institution_matches = _match_keywords(text, _INSTITUTION_KEYWORDS)
    has_deadline = _has_deadline(text)
    hard_state = any(keyword in geo_matches for keyword in _HARD_STATE_KEYWORDS)

    action_family = "generic_geo"
    action_matches = []
    if release_matches:
        action_family = "release"
        action_matches = release_matches
    elif diplomacy_matches:
        action_family = "diplomacy"
        action_matches = diplomacy_matches
    elif regime_matches:
        action_family = "regime_shift"
        action_matches = regime_matches
    elif conflict_matches:
        action_family = "conflict"
        action_matches = conflict_matches

    match_score = 0.0
    match_score += min(1.8, len(geo_matches) * 0.75)
    if action_family != "generic_geo":
        match_score += 1.0
    if institution_matches:
        match_score += min(0.6, len(institution_matches) * 0.2)
    if has_deadline:
        match_score += 0.45
    if hard_state:
        match_score += 0.25

    is_geopolitical = False
    if geo_matches and action_family != "generic_geo":
        is_geopolitical = True
    elif hard_state and institution_matches and has_deadline:
        is_geopolitical = True
    elif hard_state and len(geo_matches) >= 2 and has_deadline:
        is_geopolitical = True

    return {
        "text": text,
        "is_geopolitical": is_geopolitical,
        "match_score": match_score,
        "action_family": action_family,
        "geo_keywords": geo_matches,
        "action_keywords": action_matches,
        "institution_keywords": institution_matches,
        "has_deadline": has_deadline,
        "hard_state": hard_state,
    }


def is_geopolitical_text(*parts, min_match_score=1.5):
    context = build_geopolitical_context(*parts)
    return context["is_geopolitical"] and context["match_score"] >= min_match_score
