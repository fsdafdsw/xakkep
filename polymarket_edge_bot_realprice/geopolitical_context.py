import re
from functools import lru_cache

from catalyst_parser import parse_catalyst


_GEO_KEYWORDS = (
    "china",
    "hong kong",
    "taiwan",
    "u.s.",
    "united states",
    "us-china",
    "usa",
    "russia",
    "ukraine",
    "iran",
    "israel",
    "gaza",
    "hamas",
    "hezbollah",
    "venezuela",
    "saudi arabia",
    "syria",
    "lebanon",
    "somalia",
    "palestine",
    "palestinian",
    "european union",
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
    "vladimir putin",
    "zelensky",
    "netanyahu",
    "trump",
    "erdogan",
    "maduro",
    "mohammed bin salman",
    "maria corina machado",
    "emmanuel macron",
    "friedrich merz",
    "mette frederiksen",
    "ahmed al-sharaa",
    "luiz inacio lula da silva",
    "lula da silva",
    "julian assange",
    "osman kavala",
    "nawalny",
    "navalny",
)

_RELEASE_ACTION_KEYWORDS = (
    "released",
    "release",
    "freed",
    "pardon",
    "pardoned",
    "hostage",
    "hostages",
    "prisoner",
    "prisoners",
    "detained",
    "bail",
    "appeal",
    "appeals",
    "hearing",
    "trial",
    "verdict",
    "sentence",
    "sentenced",
    "jailed",
    "imprisoned",
    "detention",
    "custody",
    "extradition",
    "exchange",
    "swap",
)

_DIPLOMACY_ACTION_KEYWORDS = (
    "ceasefire",
    "truce",
    "talk",
    "talks",
    "call",
    "calls",
    "deal",
    "summit",
    "visit",
    "meet",
    "meets",
    "meeting",
    "sanction",
    "sanctions",
    "tariff",
    "tariffs",
    "negotiation",
    "negotiations",
    "agreement",
    "accord",
    "brokered",
    "mediated",
    "peace",
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
    "succession",
    "impeached",
    "impeachment",
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
    "retaliation",
    "retaliate",
    "airstrike",
    "raid",
)

_INSTITUTION_KEYWORDS = (
    "court",
    "hearing",
    "appeal",
    "judge",
    "appeals court",
    "supreme court",
    "prosecutor",
    "tribunal",
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
_WEAK_GEO_KEYWORDS = ("u.s.", "united states", "usa")
_BUSINESS_EXCLUSION_KEYWORDS = (
    "earnings",
    "quarterly earnings",
    "eps",
    "revenue",
    "guidance",
    "dividend",
    "dividends",
    "stock split",
    "buyback",
    "share repurchase",
)
_QUOTE_EVENT_KEYWORDS = (
    "podcast",
    "episode",
    "address",
    "speech",
    "interview",
    "debate",
    "town hall",
    "appearance",
    "wef",
    "joe rogan",
)
_QUOTE_ACTION_KEYWORDS = (
    " say ",
    " said ",
    " be said ",
    " mention ",
    " mentions ",
    " mentioned ",
    " 5+ times",
    " 10+ times",
    " 25+ times",
)
_LEGAL_RELEASE_CONTEXT_KEYWORDS = (
    "political prisoner",
    "dissident",
    "detained",
    "detention",
    "custody",
    "custodial",
    "jailed",
    "imprisoned",
    "prison",
    "prisoner",
    "hostage",
    "bail",
    "parole",
    "amnesty",
    "clemency",
    "extradite",
    "extradited",
    "appeal",
    "appeals court",
    "supreme court",
    "court",
    "hearing",
    "tribunal",
    "prosecutor",
    "extradition",
    "national security law",
)
_KNOWN_RELEASE_FIGURES = (
    "jimmy lai",
    "julian assange",
    "osman kavala",
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


def _has_deadline(text):
    if not text:
        return False
    if any(token in text for token in (" by ", " before ", " on ", " until ", " through ")):
        return True
    return bool(re.search(r"\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|q[1-4])\b", text))


def _is_quote_or_speech_market(text):
    if not text:
        return False
    if any(token in text for token in _QUOTE_ACTION_KEYWORDS):
        return True
    if "will " in text and " say " in text:
        return True
    if " during " in text and any(token in text for token in _QUOTE_EVENT_KEYWORDS):
        return True
    if any(token in text for token in _QUOTE_EVENT_KEYWORDS) and (" be said " in text or " say " in text or " mention " in text):
        return True
    return False


def build_geopolitical_context(*parts):
    question_text = normalize_text(parts[0] if parts else "")
    text = normalize_text(*parts)
    catalyst = parse_catalyst(
        parts[0] if parts else "",
        parts[1] if len(parts) > 1 else "",
        parts[4] if len(parts) > 4 else "",
    )
    geo_matches = _match_keywords(text, _GEO_KEYWORDS)
    question_geo_matches = _match_keywords(question_text, _GEO_KEYWORDS)
    release_matches = _match_keywords(text, _RELEASE_ACTION_KEYWORDS)
    release_question_matches = _match_keywords(question_text, _RELEASE_ACTION_KEYWORDS)
    diplomacy_matches = _match_keywords(text, _DIPLOMACY_ACTION_KEYWORDS)
    diplomacy_question_matches = _match_keywords(question_text, _DIPLOMACY_ACTION_KEYWORDS)
    regime_matches = _match_keywords(text, _REGIME_ACTION_KEYWORDS)
    regime_question_matches = _match_keywords(question_text, _REGIME_ACTION_KEYWORDS)
    conflict_matches = _match_keywords(text, _CONFLICT_ACTION_KEYWORDS)
    conflict_question_matches = _match_keywords(question_text, _CONFLICT_ACTION_KEYWORDS)
    institution_matches = _match_keywords(text, _INSTITUTION_KEYWORDS)
    release_context_matches = _match_keywords(text, _LEGAL_RELEASE_CONTEXT_KEYWORDS)
    business_matches = _match_keywords(text, _BUSINESS_EXCLUSION_KEYWORDS)
    has_deadline = _has_deadline(text)
    quote_market = _is_quote_or_speech_market(question_text) or _is_quote_or_speech_market(text)
    hard_state = any(keyword in geo_matches for keyword in _HARD_STATE_KEYWORDS)
    strong_geo_matches = [keyword for keyword in geo_matches if keyword not in _WEAK_GEO_KEYWORDS]
    release_figure_matches = [name for name in _KNOWN_RELEASE_FIGURES if _keyword_pattern(name).search(text)]

    action_family = "generic_geo"
    action_matches = []
    catalyst_family = catalyst.get("catalyst_family")
    if catalyst_family == "release" and (release_question_matches or release_context_matches or release_figure_matches):
        action_family = "release"
        action_matches = release_question_matches or release_matches or list(catalyst.get("catalyst_keywords") or [])
    elif catalyst_family == "diplomacy" and diplomacy_question_matches:
        action_family = "diplomacy"
        action_matches = diplomacy_question_matches or diplomacy_matches or list(catalyst.get("catalyst_keywords") or [])
    elif catalyst_family == "regime_shift" and regime_question_matches:
        action_family = "regime_shift"
        action_matches = regime_question_matches or regime_matches or list(catalyst.get("catalyst_keywords") or [])
    elif catalyst_family == "conflict" and conflict_question_matches:
        action_family = "conflict"
        action_matches = conflict_question_matches or conflict_matches or list(catalyst.get("catalyst_keywords") or [])
    elif release_question_matches:
        action_family = "release"
        action_matches = release_question_matches
    elif diplomacy_question_matches:
        action_family = "diplomacy"
        action_matches = diplomacy_question_matches
    elif regime_question_matches:
        action_family = "regime_shift"
        action_matches = regime_question_matches
    elif conflict_question_matches:
        action_family = "conflict"
        action_matches = conflict_question_matches

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
    if release_context_matches:
        match_score += min(0.7, len(release_context_matches) * 0.15)
    if release_figure_matches:
        match_score += min(0.6, len(release_figure_matches) * 0.3)
    if business_matches:
        match_score -= min(0.9, len(business_matches) * 0.3)
    if quote_market:
        match_score -= 1.2
    match_score += max(0.0, (float(catalyst.get("catalyst_strength") or 0.0) - 0.50) * 0.8)

    is_geopolitical = False
    if geo_matches and action_family == "conflict":
        is_geopolitical = True
    elif (
        action_family == "diplomacy"
        and (
            len(set(geo_matches)) >= 2
            or hard_state
            or institution_matches
        )
    ):
        is_geopolitical = True
    elif (
        action_family == "release"
        and (
            strong_geo_matches
            or hard_state
            or release_figure_matches
            or (release_context_matches and (institution_matches or question_geo_matches))
        )
    ):
        is_geopolitical = True
    elif strong_geo_matches and action_family == "regime_shift":
        is_geopolitical = True
    elif hard_state and institution_matches and has_deadline:
        is_geopolitical = True
    elif hard_state and len(geo_matches) >= 2 and has_deadline:
        is_geopolitical = True

    if business_matches and not hard_state and not institution_matches and not strong_geo_matches:
        is_geopolitical = False
    if quote_market:
        is_geopolitical = False

    return {
        "text": text,
        "is_geopolitical": is_geopolitical,
        "match_score": match_score,
        "action_family": action_family,
        "geo_keywords": geo_matches,
        "question_geo_keywords": question_geo_matches,
        "action_keywords": action_matches,
        "institution_keywords": institution_matches,
        "business_keywords": business_matches,
        "quote_market": quote_market,
        "release_context_keywords": release_context_matches,
        "release_figure_keywords": release_figure_matches,
        "catalyst_type": catalyst.get("catalyst_type"),
        "catalyst_family": catalyst.get("catalyst_family"),
        "catalyst_strength": catalyst.get("catalyst_strength"),
        "catalyst_hardness": catalyst.get("hardness"),
        "catalyst_reversibility": catalyst.get("reversibility"),
        "catalyst_keywords": catalyst.get("catalyst_keywords") or [],
        "catalyst_has_official_source": catalyst.get("has_official_source"),
        "catalyst_source_strength": catalyst.get("source_strength"),
        "catalyst_official_source_keywords": catalyst.get("official_source_keywords") or [],
        "has_deadline": has_deadline,
        "hard_state": hard_state,
    }


def is_geopolitical_text(*parts, min_match_score=1.5):
    context = build_geopolitical_context(*parts)
    return context["is_geopolitical"] and context["match_score"] >= min_match_score
