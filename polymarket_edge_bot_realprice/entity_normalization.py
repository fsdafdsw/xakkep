import re
import unicodedata

from market_profile import normalize_text


_GENERIC_ENTITY_WORDS = {
    "the",
    "a",
    "an",
    "candidate",
    "party",
    "team",
    "fc",
    "cf",
    "sc",
    "afc",
    "ac",
    "club",
    "inc",
    "corp",
    "co",
    "llc",
    "ltd",
    "sa",
    "plc",
}
_GENERIC_OUTCOMES = {
    "yes",
    "no",
    "true",
    "false",
    "other",
    "draw",
}
_ENTITY_SPLIT_MARKERS = (" vs ", " v ", " @ ", " at ")


def _ascii_fold(value):
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return normalized.encode("ascii", "ignore").decode("ascii")


def normalize_entity_name(value):
    text = _ascii_fold(normalize_text(value))
    text = re.sub(r"[^\w\s]", " ", text)
    text = " ".join(text.split())
    return text.strip()


def entity_key(value):
    name = normalize_entity_name(value)
    if not name:
        return None

    original_tokens = name.split()
    tokens = [token for token in original_tokens if token and token not in _GENERIC_ENTITY_WORDS]
    if not tokens or all(len(token) <= 1 for token in tokens):
        tokens = original_tokens
    if not tokens:
        return None

    return "_".join(tokens[:6])


def split_event_entities(text):
    normalized = normalize_entity_name(text)
    if not normalized:
        return []

    for marker in _ENTITY_SPLIT_MARKERS:
        if marker in normalized:
            left, right = normalized.split(marker, 1)
            entities = [left.strip(), right.strip()]
            return [entity for entity in entities if entity]
    return []


def outcome_entity_key(outcome):
    normalized = normalize_entity_name(outcome)
    if not normalized or normalized in _GENERIC_OUTCOMES:
        return None
    if re.fullmatch(r"[\d\.\-:%\s]+", normalized):
        return None
    return entity_key(normalized)


def extract_market_entities(market):
    question = market.get("question")
    event_title = market.get("event_title")
    outcomes = market.get("outcomes") or []

    event_entities = split_event_entities(event_title)
    outcome_entities = []
    for outcome in outcomes:
        key = outcome_entity_key(outcome)
        if key:
            outcome_entities.append(
                {
                    "name": normalize_entity_name(outcome),
                    "key": key,
                }
            )

    unique_keys = []
    seen = set()
    for name in event_entities:
        key = entity_key(name)
        if key and key not in seen:
            seen.add(key)
            unique_keys.append(key)
    for outcome in outcome_entities:
        if outcome["key"] not in seen:
            seen.add(outcome["key"])
            unique_keys.append(outcome["key"])

    return {
        "event_entities": [normalize_entity_name(name) for name in event_entities if name],
        "event_entity_keys": [entity_key(name) for name in event_entities if entity_key(name)],
        "outcome_entities": outcome_entities,
        "entity_keys": unique_keys,
        "question_normalized": normalize_entity_name(question),
    }
