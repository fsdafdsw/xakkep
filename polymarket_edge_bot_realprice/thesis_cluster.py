import hashlib
import re

from market_profile import normalize_text


_WORD_NUMBERS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
}

_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

_THRESHOLD_RE = re.compile(
    r"\b(?P<count>\d+|"
    + "|".join(sorted(_WORD_NUMBERS.keys(), key=len, reverse=True))
    + r")(?P<plus>\+)?(?:\s+or\s+more)?\s+(?P<unit>countries?|hostages?)\b",
    re.IGNORECASE,
)
_NATURAL_DATE_RE = re.compile(
    r"\b(?P<comparator>by|before|on|in)\s+"
    r"(?:(?P<end>end\s+of)\s+)?"
    r"(?:(?P<month>"
    + "|".join(_MONTHS.keys())
    + r")"
    r"(?:\s+(?P<day>\d{1,2}))?"
    r"(?:,?\s+(?P<year>20\d{2}))?"
    r"|(?P<year_only>20\d{2}))\b",
    re.IGNORECASE,
)
_PUNCT_RE = re.compile(r"[^\w\s<>]")


def _safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _singular_unit(unit):
    unit = str(unit or "").lower()
    if unit == "countries":
        return "country"
    if unit == "hostages":
        return "hostage"
    return unit.rstrip("s")


def _number_from_token(token):
    if token is None:
        return None
    token = str(token).strip().lower()
    if token.isdigit():
        return int(token)
    return _WORD_NUMBERS.get(token)


def _normalize_stem(text):
    normalized = normalize_text(text)
    normalized = _PUNCT_RE.sub(" ", normalized)
    return " ".join(normalized.split()).strip()


def _stable_id(prefix, key):
    digest = hashlib.sha1(str(key).encode("utf-8")).hexdigest()[:10]
    return f"{prefix}:{digest}"


def _question_year(text):
    match = re.search(r"\b(20\d{2})\b", str(text or ""))
    if not match:
        return None
    return _safe_int(match.group(1))


def _extract_threshold_descriptor(candidate):
    question = str(candidate.get("question") or "")
    action_family = str(candidate.get("domain_action_family") or "")
    if action_family != "conflict":
        return None

    match = _THRESHOLD_RE.search(question)
    if not match:
        return None

    value = _number_from_token(match.group("count"))
    unit = _singular_unit(match.group("unit"))
    if value is None or unit != "country":
        return None

    stem = _normalize_stem(_THRESHOLD_RE.sub(" <threshold> ", question, count=1))
    return {
        "thesis_type": "threshold_ladder",
        "dimension_type": "count_threshold",
        "dimension_label": f"{value} {unit}",
        "dimension_value": value,
        "stem": stem,
    }


def _extract_deadline_descriptor(candidate):
    question = str(candidate.get("question") or "")
    action_family = str(candidate.get("domain_action_family") or "")
    if action_family not in {"release", "diplomacy", "conflict", "regime_shift"}:
        return None

    match = _NATURAL_DATE_RE.search(question)
    if not match:
        return None

    comparator = str(match.group("comparator") or "").lower()
    year = _safe_int(match.group("year")) or _safe_int(match.group("year_only")) or _question_year(question) or 2100
    month_name = (match.group("month") or "").lower()
    month = _MONTHS.get(month_name) if month_name else 12
    if match.group("year_only"):
        day = 31
        month = 12
    elif match.group("end"):
        day = 31
    else:
        day = _safe_int(match.group("day")) or 28

    sort_value = (year * 10000) + (month * 100) + day
    label = _normalize_stem(match.group(0))
    stem = _normalize_stem(_NATURAL_DATE_RE.sub(" <deadline> ", question, count=1))
    return {
        "thesis_type": "deadline_ladder",
        "dimension_type": "deadline",
        "dimension_label": label,
        "dimension_value": sort_value,
        "comparator": comparator,
        "stem": stem,
    }


def _standalone_descriptor(candidate):
    question = str(candidate.get("question") or "")
    return {
        "thesis_type": "standalone",
        "dimension_type": None,
        "dimension_label": None,
        "dimension_value": None,
        "stem": _normalize_stem(question),
    }


def _describe_candidate(candidate):
    descriptor = _extract_threshold_descriptor(candidate)
    if descriptor:
        return descriptor

    descriptor = _extract_deadline_descriptor(candidate)
    if descriptor:
        return descriptor

    return _standalone_descriptor(candidate)


def _event_anchor(candidate):
    return (
        candidate.get("event_slug")
        or candidate.get("event_key")
        or candidate.get("primary_entity_key")
        or candidate.get("event_title")
        or candidate.get("question")
        or "unknown"
    )


def _cluster_key(candidate, descriptor):
    action_family = str(candidate.get("domain_action_family") or "unknown")
    anchor = _normalize_stem(_event_anchor(candidate))
    parts = [
        anchor,
        action_family,
        descriptor["thesis_type"],
        descriptor.get("dimension_type") or "none",
        descriptor["stem"],
    ]
    return "|".join(parts)


def annotate_thesis_clusters(*candidate_groups):
    all_candidates = []
    for group in candidate_groups:
        for candidate in group or []:
            all_candidates.append(candidate)

    grouped = {}
    descriptors = {}
    for candidate in all_candidates:
        descriptor = _describe_candidate(candidate)
        key = _cluster_key(candidate, descriptor)
        grouped.setdefault(key, []).append(candidate)
        descriptors[key] = descriptor

    clusters = []
    for key, members in grouped.items():
        descriptor = descriptors[key]
        member_descriptors = {id(candidate): _describe_candidate(candidate) for candidate in members}
        if descriptor.get("dimension_value") is not None:
            sorted_members = sorted(
                members,
                key=lambda row: (
                    member_descriptors[id(row)].get("dimension_value"),
                    str(row.get("question") or ""),
                ),
            )
        else:
            sorted_members = sorted(members, key=lambda row: str(row.get("question") or ""))

        thesis_id = _stable_id(descriptor["thesis_type"], key)
        cluster_size = len(sorted_members)

        for idx, candidate in enumerate(sorted_members, start=1):
            member_descriptor = member_descriptors[id(candidate)]
            candidate["thesis_id"] = thesis_id
            candidate["thesis_type"] = descriptor["thesis_type"]
            candidate["thesis_stem"] = descriptor["stem"]
            candidate["thesis_cluster_size"] = cluster_size
            candidate["thesis_member_order"] = idx
            candidate["thesis_dimension_type"] = member_descriptor.get("dimension_type")
            candidate["thesis_dimension_label"] = member_descriptor.get("dimension_label")
            candidate["thesis_dimension_value"] = member_descriptor.get("dimension_value")

        clusters.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": descriptor["thesis_type"],
                "thesis_stem": descriptor["stem"],
                "thesis_cluster_size": cluster_size,
                "event_slug": sorted_members[0].get("event_slug"),
                "domain_action_family": sorted_members[0].get("domain_action_family"),
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "dimension_label": row.get("thesis_dimension_label"),
                        "dimension_value": row.get("thesis_dimension_value"),
                        "member_order": row.get("thesis_member_order"),
                    }
                    for row in sorted_members
                ],
            }
        )

    clusters.sort(
        key=lambda cluster: (
            -cluster["thesis_cluster_size"],
            cluster["thesis_type"],
            str(cluster.get("event_slug") or ""),
            cluster["thesis_id"],
        )
    )
    return clusters
