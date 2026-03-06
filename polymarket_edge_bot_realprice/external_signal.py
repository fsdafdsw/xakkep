import math
import re

from config import (
    DOMAIN_CONFIDENCE_WEIGHT,
    DOMAIN_SIGNAL_WEIGHT,
    RELATION_CONFIDENCE_WEIGHT,
    RELATION_SIGNAL_WEIGHT,
)
from domain_predictor import compute_domain_predictor
from market_profile import as_int, contains_any, enrich_market_profile, normalize_text


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
    parsed = market.get("resolution_metadata") or {}
    relation_metrics = market.get("relation_metrics") or {}
    if market.get("resolution_source"):
        score += 0.25
    if market.get("event_description") or market.get("market_description"):
        score += 0.15
    if market.get("event_category"):
        score += 0.08
    if market.get("selected_outcome"):
        score += 0.05
    if parsed.get("confidence"):
        score += min(0.12, float(parsed["confidence"]) * 0.12)
    if parsed.get("subject_entity_key"):
        score += 0.04
    if relation_metrics.get("relation_degree", 0) > 0:
        score += min(0.08, relation_metrics.get("relation_degree", 0) * 0.01)
    return _clamp(score)


def _competition_risk(market, question_text):
    event_market_count = max(1, as_int(market.get("event_market_count"), default=1))
    outcome_count = max(1, as_int(market.get("outcome_count"), default=2))
    relation_metrics = market.get("relation_metrics") or {}

    risk = 0.0
    if event_market_count > 1:
        risk += min(0.45, (event_market_count - 1) * 0.045)
    if outcome_count > 2:
        risk += min(0.20, (outcome_count - 2) * 0.07)
    if contains_any(question_text, _WINNER_MARKET_KEYWORDS):
        risk = max(risk, 0.32 + min(0.32, max(0, event_market_count - 2) * 0.03))
    if relation_metrics.get("exclusive_degree", 0) > 0:
        risk += min(0.10, relation_metrics.get("exclusive_degree", 0) * 0.01)

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

    event_market_count = max(1, as_int(market.get("event_market_count"), default=1))
    score = 0.5 + (math.tanh((float(price) - 0.33) * 4.2) * 0.18)

    if price < 0.20:
        score -= min(0.12, (0.20 - price) * 0.60)
    if event_market_count >= 4 and price < 0.25:
        score -= 0.06
    if contains_any(question_text, _WINNER_MARKET_KEYWORDS) and price < 0.25:
        score -= 0.05

    return _clamp(score)


def _category_risk(market, question_text):
    category_text = normalize_text(market.get("event_category"))
    risk = 0.0

    if contains_any(category_text, _NOISY_CATEGORY_KEYWORDS):
        risk = max(risk, 0.18)
    if contains_any(question_text, _NOISY_MARKET_KEYWORDS):
        risk = max(risk, 0.24)

    return _clamp(risk)


def compute_external_signal(market):
    profile = enrich_market_profile(market)
    question_text = normalize_text(
        market.get("question"),
        market.get("event_title"),
    )
    specificity = _specificity_score(question_text)
    resolution_quality = _resolution_quality(market)
    competition_risk = _competition_risk(market, question_text)
    horizon_risk = _horizon_risk(market.get("hours_to_close"))
    price_prior = _favorite_longshot_prior(market, question_text)
    category_risk = _category_risk(market, question_text)
    domain = compute_domain_predictor(market)
    relation_metrics = market.get("relation_metrics") or {}
    relation_residual = market.get("relation_residual") or {}
    parsed = market.get("resolution_metadata") or {}
    residual = float(relation_residual.get("residual") or 0.0)
    support_confidence = float(relation_residual.get("support_confidence") or 0.0)
    inconsistency_score = float(relation_residual.get("inconsistency_score") or 0.0)
    constraint_violation = float(relation_residual.get("constraint_violation") or 0.0)

    signal = 0.5
    signal += (specificity - 0.5) * 0.12
    signal += (resolution_quality - 0.5) * 0.10
    signal += (price_prior - 0.5) * 0.60
    signal -= competition_risk * 0.12
    signal -= horizon_risk * 0.10
    signal -= category_risk * 0.08
    signal += profile["signal_bias"]
    signal += (domain["signal"] - 0.5) * DOMAIN_SIGNAL_WEIGHT
    signal += max(-0.020, min(0.020, residual)) * RELATION_SIGNAL_WEIGHT * (0.60 + (support_confidence * 0.40))

    confidence = 0.54
    confidence += (specificity - 0.5) * 0.25
    confidence += (resolution_quality - 0.5) * 0.25
    confidence -= competition_risk * 0.18
    confidence -= horizon_risk * 0.12
    confidence -= category_risk * 0.10
    confidence += profile["confidence_bias"]
    confidence += (domain["confidence"] - 0.5) * DOMAIN_CONFIDENCE_WEIGHT
    confidence += min(0.05, relation_metrics.get("relation_confidence", 0.0) * 0.05)
    confidence += min(0.04, float(parsed.get("confidence") or 0.0) * 0.04)
    confidence += (support_confidence - 0.5) * RELATION_CONFIDENCE_WEIGHT
    confidence -= inconsistency_score * 0.10
    confidence -= min(0.06, constraint_violation * 1.2)

    return {
        "signal": _clamp(signal),
        "confidence": _clamp(confidence),
        "market_type": profile["market_type"],
        "category_group": profile["category_group"],
        "adjustment_multiplier": profile["adjustment_multiplier"],
        "factor_weights": profile["factor_weights"],
        "domain_name": domain["name"],
        "domain_signal": domain["signal"],
        "domain_confidence": domain["confidence"],
        "components": {
            "specificity": specificity,
            "resolution_quality": resolution_quality,
            "competition_risk": competition_risk,
            "horizon_risk": horizon_risk,
            "price_prior": price_prior,
            "category_risk": category_risk,
            "signal_bias": profile["signal_bias"],
            "confidence_bias": profile["confidence_bias"],
            "structure_flags": profile["structure_flags"],
            "resolution_metadata": parsed,
            "relation_metrics": relation_metrics,
            "relation_residual": relation_residual,
            "domain": {
                "name": domain["name"],
                "signal": domain["signal"],
                "confidence": domain["confidence"],
                "components": domain["components"],
            },
        },
    }
