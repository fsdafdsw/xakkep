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
_RANGE_MARKET_KEYWORDS = (
    "between",
    "range",
    "at least",
    "at most",
    "or higher",
    "or lower",
)
_PRICE_TARGET_KEYWORDS = (
    "price",
    "market cap",
    "above",
    "below",
    "over",
    "under",
    "hit",
    "reach",
    "close at",
)
_POLITICS_KEYWORDS = ("politic", "election", "president", "nomination", "seat", "prime minister")
_SPORTS_KEYWORDS = ("nba", "nfl", "nhl", "mlb", "f1", "cup", "final", "mvp", "champion")
_ENTERTAINMENT_KEYWORDS = ("movie", "film", "oscars", "academy award", "eurovision", "box office")
_CRYPTO_KEYWORDS = ("bitcoin", "btc", "eth", "ethereum", "sol", "crypto", "token")
_BUSINESS_KEYWORDS = ("market cap", "stock", "company", "earnings", "apple", "tesla", "nvidia")

_PROFILE_CONFIG = {
    "winner_multi": {
        "adjustment_multiplier": 0.65,
        "signal_bias": -0.04,
        "confidence_bias": -0.06,
        "factor_weights": {
            "momentum": 0.22,
            "orderbook": 0.14,
            "news": 0.14,
            "anomaly": 0.26,
        },
    },
    "range_multi": {
        "adjustment_multiplier": 0.70,
        "signal_bias": -0.03,
        "confidence_bias": -0.05,
        "factor_weights": {
            "momentum": 0.18,
            "orderbook": 0.12,
            "news": 0.12,
            "anomaly": 0.24,
        },
    },
    "dated_binary": {
        "adjustment_multiplier": 1.08,
        "signal_bias": 0.02,
        "confidence_bias": 0.05,
        "factor_weights": {
            "momentum": 0.35,
            "orderbook": 0.18,
            "news": 0.15,
            "anomaly": 0.18,
        },
    },
    "near_term_binary": {
        "adjustment_multiplier": 1.05,
        "signal_bias": 0.01,
        "confidence_bias": 0.03,
        "factor_weights": {
            "momentum": 0.50,
            "orderbook": 0.28,
            "news": 0.10,
            "anomaly": 0.18,
        },
    },
    "price_target": {
        "adjustment_multiplier": 0.82,
        "signal_bias": -0.01,
        "confidence_bias": -0.02,
        "factor_weights": {
            "momentum": 0.24,
            "orderbook": 0.16,
            "news": 0.06,
            "anomaly": 0.22,
        },
    },
    "generic_binary": {
        "adjustment_multiplier": 0.92,
        "signal_bias": 0.0,
        "confidence_bias": 0.0,
        "factor_weights": {
            "momentum": 0.45,
            "orderbook": 0.25,
            "news": 0.10,
            "anomaly": 0.20,
        },
    },
}


def normalize_text(*parts):
    text = " ".join(str(part or "") for part in parts)
    return " ".join(text.lower().split())


def contains_any(text, patterns):
    return any(pattern in text for pattern in patterns)


def as_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def classify_category_group(market):
    text = normalize_text(
        market.get("event_category"),
        market.get("question"),
        market.get("event_title"),
    )

    if contains_any(text, _POLITICS_KEYWORDS):
        return "politics"
    if contains_any(text, _SPORTS_KEYWORDS):
        return "sports"
    if contains_any(text, _ENTERTAINMENT_KEYWORDS):
        return "entertainment"
    if contains_any(text, _CRYPTO_KEYWORDS):
        return "crypto"
    if contains_any(text, _BUSINESS_KEYWORDS):
        return "business"
    return "other"


def classify_market_profile(market):
    question_text = normalize_text(
        market.get("question"),
        market.get("event_title"),
    )
    event_market_count = max(1, as_int(market.get("event_market_count"), default=1))
    outcome_count = max(1, as_int(market.get("outcome_count"), default=2))
    hours_to_close = market.get("hours_to_close")
    has_date = bool(_DATE_RE.search(question_text))
    has_condition = bool(_CONDITION_RE.search(question_text))

    if outcome_count > 2:
        market_type = "range_multi"
    elif contains_any(question_text, _PRICE_TARGET_KEYWORDS):
        market_type = "price_target"
    elif event_market_count >= 5 or contains_any(question_text, _WINNER_MARKET_KEYWORDS):
        market_type = "winner_multi"
    elif has_date or has_condition or contains_any(question_text, _RANGE_MARKET_KEYWORDS):
        market_type = "dated_binary"
    elif hours_to_close is not None and float(hours_to_close) <= 72.0:
        market_type = "near_term_binary"
    else:
        market_type = "generic_binary"

    profile = _PROFILE_CONFIG[market_type]
    return {
        "market_type": market_type,
        "category_group": classify_category_group(market),
        "adjustment_multiplier": profile["adjustment_multiplier"],
        "signal_bias": profile["signal_bias"],
        "confidence_bias": profile["confidence_bias"],
        "factor_weights": dict(profile["factor_weights"]),
        "structure_flags": {
            "has_date": has_date,
            "has_condition": has_condition,
            "event_market_count": event_market_count,
            "outcome_count": outcome_count,
        },
    }


def enrich_market_profile(market):
    if market.get("market_profile"):
        return market["market_profile"]

    profile = classify_market_profile(market)
    market["market_profile"] = profile
    market["market_type"] = profile["market_type"]
    market["category_group"] = profile["category_group"]
    return profile
