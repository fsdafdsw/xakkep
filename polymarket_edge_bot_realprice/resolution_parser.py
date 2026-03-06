import re
from datetime import datetime

from entity_normalization import entity_key, normalize_entity_name
from market_profile import normalize_text


_ISO_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_NUMBER_THRESHOLD_RE = re.compile(
    r"\b(?P<comparator>above|below|over|under|at least|at most|greater than|less than|reach|hit|close at)\s+\$?(?P<value>\d+(?:[\.,]\d+)?)\s*(?P<unit>[kmbt%a-z]+)?",
    re.IGNORECASE,
)
_WILL_SUBJECT_RE = re.compile(
    r"^will (?P<subject>.+?) (?P<predicate>win|be|have|reach|hit|return|leave|resign|lose|take|close|finish|outpoll|secure)\b",
    re.IGNORECASE,
)
_BY_RE = re.compile(r"\bby (?P<date>20\d{2}-\d{2}-\d{2}|20\d{2})\b", re.IGNORECASE)
_BEFORE_RE = re.compile(r"\bbefore (?P<date>20\d{2}-\d{2}-\d{2}|20\d{2})\b", re.IGNORECASE)
_AFTER_RE = re.compile(r"\bafter (?P<date>20\d{2}-\d{2}-\d{2}|20\d{2})\b", re.IGNORECASE)
_ON_RE = re.compile(r"\bon (?P<date>20\d{2}-\d{2}-\d{2})\b", re.IGNORECASE)
_WHO_WILL_RE = re.compile(r"^(who|which).+\b(win|winner|largest|most seats|nomination)\b", re.IGNORECASE)
_BETWEEN_RE = re.compile(
    r"\bbetween\s+\$?(?P<low>\d+(?:[\.,]\d+)?)\s*(?P<unit>[kmbt%a-z]+)?\s+and\s+\$?(?P<high>\d+(?:[\.,]\d+)?)",
    re.IGNORECASE,
)


def _safe_date(value):
    if not value:
        return None
    try:
        if len(value) == 4:
            return f"{value}-12-31"
        datetime.fromisoformat(value)
        return value
    except ValueError:
        return None


def _parse_threshold(text):
    match = _BETWEEN_RE.search(text)
    if match:
        low = float(match.group("low").replace(",", ""))
        high = float(match.group("high").replace(",", ""))
        unit = (match.group("unit") or "").lower()
        return {
            "kind": "range",
            "comparator": "between",
            "low": min(low, high),
            "high": max(low, high),
            "unit": unit,
            "key": f"between:{min(low, high)}:{max(low, high)}:{unit}",
        }

    match = _NUMBER_THRESHOLD_RE.search(text)
    if not match:
        return None

    comparator = normalize_text(match.group("comparator"))
    value = float(match.group("value").replace(",", ""))
    unit = (match.group("unit") or "").lower()
    if comparator in {"reach", "hit", "close at"}:
        direction = "at_or_above"
    elif comparator in {"above", "over", "at least", "greater than"}:
        direction = "above"
    else:
        direction = "below"

    return {
        "kind": "threshold",
        "comparator": comparator,
        "direction": direction,
        "value": value,
        "unit": unit,
        "key": f"{direction}:{value}:{unit}",
    }


def parse_resolution_semantics(market):
    question = normalize_text(market.get("question"))
    question_raw = str(market.get("question") or "")
    selected_outcome = str(market.get("selected_outcome") or "")

    family = "generic"
    subject = None
    comparator = None
    target_date = None
    threshold = _parse_threshold(question)
    confidence = 0.35

    subject_match = _WILL_SUBJECT_RE.match(question)
    if subject_match:
        subject = normalize_entity_name(subject_match.group("subject"))
        family = "will_subject"
        confidence += 0.12

    if _WHO_WILL_RE.match(question):
        family = "winner"
        confidence += 0.10

    for matcher, relation_name in ((_BY_RE, "by"), (_BEFORE_RE, "before"), (_AFTER_RE, "after"), (_ON_RE, "on")):
        match = matcher.search(question)
        if match:
            comparator = relation_name
            target_date = _safe_date(match.group("date"))
            confidence += 0.18 if target_date else 0.08
            break

    if target_date is None:
        iso_match = _ISO_DATE_RE.search(question)
        if iso_match:
            target_date = _safe_date(iso_match.group(1))
            comparator = comparator or "on"
            confidence += 0.12

    target_year = None
    year_match = _YEAR_RE.search(question)
    if year_match:
        target_year = int(year_match.group(1))
        confidence += 0.05

    if threshold:
        family = "threshold"
        confidence += 0.20

    if "yes" in selected_outcome.lower() or "no" in selected_outcome.lower():
        confidence += 0.04

    subject_key = entity_key(subject)
    relation_key = None
    if threshold and subject_key:
        relation_key = f"{subject_key}:{threshold['key']}"
    elif target_date and subject_key:
        relation_key = f"{subject_key}:{comparator}:{target_date}"
    elif subject_key and family == "winner":
        relation_key = f"{subject_key}:winner"

    return {
        "family": family,
        "subject": subject,
        "subject_entity_key": subject_key,
        "comparator": comparator,
        "target_date": target_date,
        "target_year": target_year,
        "threshold": threshold,
        "selected_outcome_text": selected_outcome,
        "question_raw": question_raw,
        "relation_key": relation_key,
        "confidence": max(0.0, min(confidence, 0.98)),
    }
