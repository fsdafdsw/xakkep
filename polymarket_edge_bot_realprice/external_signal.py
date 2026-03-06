import math
import re


_DATE_RE = re.compile(
    r"\b("
    r"20\d{2}|"
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|"
    r"q[1-4]|"
    r"\d{1,2}:\d{2}"
    r")\b"
)
_CONDITION_RE = re.compile(r"\b(before|after|by|on|at|during|until|through|prior to)\b")
_WINNER_MARKET_KEYWORDS = (
    "winner",
    "win the",
    "most seats",
    "champion",
    "mvp",
    "nomination",
    "election",
    "ballon d'or",
    "top grossing",
    "largest company",
    "stanley cup",
    "world cup",
    "academy award",
    "oscars",
    "eurovision",
)
_NOISY_CATEGORY_KEYWORDS = (
    "sports",
    "entertainment",
    "pop culture",
    "crypto",
)
_NOISY_MARKET_KEYWORDS = (
    "mvp",
    "champion",
    "award",
    "oscars",
    "academy award",
    "eurovision",
    "ballon d'or",
)


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _normalize_text(*parts):
    text = " ".join(str(part or "") for part in parts)
    return " ".join(text.lower().split())


def _contains_any(text, patterns):
    return any(pattern in text for pattern in patterns)


def _as_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _specificity_score(question_text):
    tokens = question_text.split()
    score = 0.40

    if len(tokens) >= 6:
        score += 0.08
    if len(tokens) >= 10:
        score += 0.06
    if _DATE_RE.search(question_text):
        score += 0.22
    if _CONDITION_RE.search(question_text):
        score += 0.14
    if question_text.endswith("?"):
        score += 0.05

    return _clamp(score)


def _resolution_quality(market):
    score = 0.42
    if market.get("resolution_source"):
        score += 0.25
    if market.get("event_description") or market.get("market_description"):
        score += 0.15
    if market.get("event_category"):
        score += 0.08
    if market.get("selected_outcome"):
        score += 0.05
    return _clamp(score)


def _competition_risk(market, question_text):
    event_market_count = max(1, _as_int(market.get("event_market_count"), default=1))
    outcome_count = max(1, _as_int(market.get("outcome_count"), default=2))

    risk = 0.0
    if event_market_count > 1:
        risk += min(0.45, (event_market_count - 1) * 0.045)
    if outcome_count > 2:
        risk += min(0.20, (outcome_count - 2) * 0.07)
    if _contains_any(question_text, _WINNER_MARKET_KEYWORDS):
        risk = max(risk, 0.32 + min(0.32, max(0, event_market_count - 2) * 0.03))

    return _clamp(risk)


def _horizon_risk(hours_to_close):
    if hours_to_close is None:
        return 0.25

    days = max(0.0, float(hours_to_close) / 24.0)
    if days <= 3:
        return 0.05
    if days <= 14:
        return 0.05 + ((days - 3) / 11.0) * 0.20
    if days <= 60:
        return 0.25 + ((days - 14) / 46.0) * 0.35
    if days <= 180:
        return 0.60 + ((days - 60) / 120.0) * 0.25
    return 0.92


def _favorite_longshot_prior(market, question_text):
    price = market.get("ref_price")
    if price is None:
        return 0.5

    event_market_count = max(1, _as_int(market.get("event_market_count"), default=1))
    score = 0.5 + (math.tanh((float(price) - 0.33) * 4.2) * 0.18)

    if price < 0.20:
        score -= min(0.12, (0.20 - price) * 0.60)
    if event_market_count >= 4 and price < 0.25:
        score -= 0.06
    if _contains_any(question_text, _WINNER_MARKET_KEYWORDS) and price < 0.25:
        score -= 0.05

    return _clamp(score)


def _category_risk(market, question_text):
    category_text = _normalize_text(market.get("event_category"))
    risk = 0.0

    if _contains_any(category_text, _NOISY_CATEGORY_KEYWORDS):
        risk = max(risk, 0.18)
    if _contains_any(question_text, _NOISY_MARKET_KEYWORDS):
        risk = max(risk, 0.24)

    return _clamp(risk)


def compute_external_signal(market):
    question_text = _normalize_text(
        market.get("question"),
        market.get("event_title"),
    )
    specificity = _specificity_score(question_text)
    resolution_quality = _resolution_quality(market)
    competition_risk = _competition_risk(market, question_text)
    horizon_risk = _horizon_risk(market.get("hours_to_close"))
    price_prior = _favorite_longshot_prior(market, question_text)
    category_risk = _category_risk(market, question_text)

    signal = 0.5
    signal += (specificity - 0.5) * 0.12
    signal += (resolution_quality - 0.5) * 0.10
    signal += (price_prior - 0.5) * 0.60
    signal -= competition_risk * 0.12
    signal -= horizon_risk * 0.10
    signal -= category_risk * 0.08

    confidence = 0.54
    confidence += (specificity - 0.5) * 0.25
    confidence += (resolution_quality - 0.5) * 0.25
    confidence -= competition_risk * 0.18
    confidence -= horizon_risk * 0.12
    confidence -= category_risk * 0.10

    return {
        "signal": _clamp(signal),
        "confidence": _clamp(confidence),
        "components": {
            "specificity": specificity,
            "resolution_quality": resolution_quality,
            "competition_risk": competition_risk,
            "horizon_risk": horizon_risk,
            "price_prior": price_prior,
            "category_risk": category_risk,
        },
    }
