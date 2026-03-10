import re

from geopolitical_context import build_geopolitical_context
from market_profile import contains_any, normalize_text
from odds_feed import get_market_odds_prior


_WIN_ON_DATE_RE = re.compile(
    r"^will (?P<subject>.+?) win on (?P<date>20\d{2}-\d{2}-\d{2})\??$",
    re.IGNORECASE,
)
_UP_DOWN_RE = re.compile(
    r"\b(bitcoin|ethereum|btc|eth).*(up or down)|(up or down).*(bitcoin|ethereum|btc|eth)\b",
    re.IGNORECASE,
)
_SOCCER_LEAGUE_PREFIXES = (
    "epl",
    "fl1",
    "bun",
    "bl2",
    "laliga",
    "ucl",
    "uefa",
    "serie",
    "ligue",
)
_DECISIVE_SPORT_KEYWORDS = (
    "nba",
    "nfl",
    "nhl",
    "mlb",
    "f1",
    "mvp",
)
_TEAM_SPLIT_PATTERNS = (" vs ", " v ", " @ ", " at ")
_HARD_STATE_KEYWORDS = ("china", "hong kong", "russia", "iran", "north korea")


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _slug_prefix(market):
    slug = str(market.get("event_slug") or market.get("slug") or "").lower().strip()
    if not slug:
        return ""
    return slug.split("-", 1)[0]


def _parse_subject_team(question_text):
    match = _WIN_ON_DATE_RE.match(question_text)
    if not match:
        return None
    return normalize_text(match.group("subject"))


def _parse_event_teams(event_text):
    text = normalize_text(event_text)
    for marker in _TEAM_SPLIT_PATTERNS:
        if marker in text:
            left, right = text.split(marker, 1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return left, right
    return None, None


def _subject_side(subject_text, event_text):
    left, right = _parse_event_teams(event_text)
    if not subject_text or not left or not right:
        return "unknown"
    if subject_text in left or left in subject_text:
        return "home"
    if subject_text in right or right in subject_text:
        return "away"
    return "unknown"


def _sports_match_predictor(market, question_text):
    subject = _parse_subject_team(question_text)
    category_text = normalize_text(
        market.get("event_category"),
        market.get("event_title"),
        market.get("category_group"),
    )
    if not subject:
        return None
    if market.get("market_type") not in {"dated_binary", "near_term_binary", "winner_multi"}:
        return None
    sports_context = (
        "sports" in category_text
        or contains_any(category_text, _DECISIVE_SPORT_KEYWORDS)
        or _slug_prefix(market) in _SOCCER_LEAGUE_PREFIXES
        or any(marker in normalize_text(market.get("event_title")) for marker in _TEAM_SPLIT_PATTERNS)
    )
    if not sports_context:
        return None

    price = market.get("ref_price")
    if price is None:
        return None

    hours_to_close = market.get("hours_to_close")
    days_to_close = max(0.0, float(hours_to_close or 0.0) / 24.0)
    spread = market.get("spread") or 0.0
    one_hour_change = market.get("one_hour_change") or 0.0
    one_day_change = market.get("one_day_change") or 0.0
    liquidity = float(market.get("liquidity") or 0.0)
    volume24h = float(market.get("volume24h") or 0.0)
    slug_prefix = _slug_prefix(market)
    event_text = normalize_text(market.get("event_title"), market.get("event_description"))

    is_soccer_style = slug_prefix in _SOCCER_LEAGUE_PREFIXES or contains_any(
        category_text,
        ("soccer", "football", "ligue", "bundesliga", "premier league"),
    )
    decisive_sport = contains_any(category_text, _DECISIVE_SPORT_KEYWORDS) and not is_soccer_style
    subject_side = _subject_side(subject, event_text)

    price_bias = 0.0
    if 0.52 <= price <= 0.72:
        price_bias += 0.14
    elif 0.42 <= price < 0.52:
        price_bias += 0.04
    elif price < 0.32:
        price_bias -= 0.16
    elif price < 0.42:
        price_bias -= 0.08
    elif price > 0.82:
        price_bias -= 0.06

    line_confirmation = 0.0
    line_confirmation += _clamp((0.035 - spread) / 0.035, low=-0.5, high=1.0) * 0.10
    line_confirmation += _clamp((volume24h / 1200.0), 0.0, 1.0) * 0.06
    line_confirmation += _clamp((liquidity / 2500.0), 0.0, 1.0) * 0.05
    line_confirmation += _clamp(((one_hour_change * 6.0) + (one_day_change * 2.0)), -1.0, 1.0) * 0.05

    timing_bias = 0.0
    if days_to_close <= 3:
        timing_bias += 0.08
    elif days_to_close <= 7:
        timing_bias += 0.04
    elif days_to_close > 14:
        timing_bias -= 0.05

    structural_penalty = 0.0
    if is_soccer_style:
        if 0.35 <= price <= 0.58:
            structural_penalty += 0.09
        if subject_side == "away":
            structural_penalty += 0.04
        elif subject_side == "home":
            structural_penalty -= 0.02
    elif decisive_sport:
        if 0.45 <= price <= 0.68:
            price_bias += 0.05
        if subject_side == "home":
            price_bias += 0.02
        elif subject_side == "away":
            structural_penalty += 0.01

    signal = 0.50 + price_bias + line_confirmation + timing_bias - structural_penalty

    confidence = 0.56
    confidence += _clamp((0.030 - spread) / 0.030, low=-0.4, high=1.0) * 0.12
    confidence += _clamp((volume24h / 1500.0), 0.0, 1.0) * 0.08
    confidence += _clamp((liquidity / 3000.0), 0.0, 1.0) * 0.06
    confidence += 0.06 if subject_side != "unknown" else -0.02
    confidence += 0.05 if days_to_close <= 5 else 0.0
    confidence -= 0.08 if is_soccer_style and 0.35 <= price <= 0.58 else 0.0

    return {
        "name": "sports_match_outcome",
        "signal": _clamp(signal),
        "confidence": _clamp(confidence),
        "components": {
            "subject_team": subject,
            "subject_side": subject_side,
            "is_soccer_style": is_soccer_style,
            "decisive_sport": decisive_sport,
            "price_bias": price_bias,
            "line_confirmation": line_confirmation,
            "timing_bias": timing_bias,
            "structural_penalty": structural_penalty,
            "days_to_close": days_to_close,
        },
    }


def _sports_odds_feed_predictor(market, question_text):
    subject = _parse_subject_team(question_text)
    if not subject:
        return None
    if market.get("market_type") not in {"dated_binary", "near_term_binary", "winner_multi"}:
        return None
    return get_market_odds_prior(market)


def _intraday_noise_predictor(market, question_text):
    if not _UP_DOWN_RE.search(question_text):
        return None

    hours_to_close = float(market.get("hours_to_close") or 0.0)
    spread = float(market.get("spread") or 0.0)
    price = float(market.get("ref_price") or 0.5)

    signal = 0.32
    confidence = 0.76
    if hours_to_close <= 24:
        signal -= 0.05
        confidence += 0.05
    if spread > 0.03:
        signal -= 0.04
    if price < 0.10 or price > 0.90:
        signal -= 0.03

    return {
        "name": "intraday_noise_penalty",
        "signal": _clamp(signal),
        "confidence": _clamp(confidence),
        "components": {
            "hours_to_close": hours_to_close,
            "spread": spread,
            "price_extreme": price < 0.10 or price > 0.90,
        },
    }


def _dated_binary_prior(market, question_text):
    if market.get("market_type") != "dated_binary":
        return None

    specificity_bonus = 0.0
    if " before " in question_text or " by " in question_text:
        specificity_bonus += 0.05
    if " on " in question_text:
        specificity_bonus += 0.04
    if market.get("resolution_source"):
        specificity_bonus += 0.04

    price = float(market.get("ref_price") or 0.5)
    price_shape = 0.0
    if 0.40 <= price <= 0.70:
        price_shape += 0.05
    elif price < 0.20:
        price_shape -= 0.08

    signal = 0.50 + specificity_bonus + price_shape
    confidence = 0.54 + specificity_bonus

    return {
        "name": "dated_binary_prior",
        "signal": _clamp(signal),
        "confidence": _clamp(confidence),
        "components": {
            "specificity_bonus": specificity_bonus,
            "price_shape": price_shape,
        },
    }


def _geopolitical_repricing_predictor(market, question_text):
    market_type = str(market.get("market_type") or "")
    outcome_count = int(market.get("outcome_count") or 2)
    if market_type not in {"dated_binary", "near_term_binary", "winner_multi"}:
        return None
    if market_type == "winner_multi" and outcome_count > 2:
        return None

    context_text = normalize_text(
        question_text,
        market.get("event_title"),
        market.get("event_description"),
        market.get("event_category"),
        market.get("resolution_source"),
        market.get("category_group"),
    )
    geo_context = build_geopolitical_context(
        question_text,
        market.get("event_title"),
        market.get("event_description"),
        market.get("event_category"),
        market.get("resolution_source"),
        market.get("category_group"),
    )
    if not geo_context["is_geopolitical"]:
        return None

    price = float(market.get("ref_price") or 0.5)
    hours_to_close = float(market.get("hours_to_close") or 0.0)
    days_to_close = max(0.0, hours_to_close / 24.0)
    spread = float(market.get("spread") or 0.0)
    liquidity = float(market.get("liquidity") or 0.0)
    volume24h = float(market.get("volume24h") or 0.0)
    has_date = any(token in context_text for token in (" by ", " before ", " on ", " june ", " july ", " august ", " september ", " october ", " november ", " december ", " january ", " february ", " march ", " april ", " may ", " 2026", " 2027", " 2028"))
    has_source = bool(market.get("resolution_source"))
    hard_state = geo_context["hard_state"]
    binary_event_grid = market_type == "winner_multi"
    catalyst_type = str(geo_context.get("catalyst_type") or "generic")
    catalyst_strength = float(geo_context.get("catalyst_strength") or 0.0)
    catalyst_hardness = str(geo_context.get("catalyst_hardness") or "soft")
    catalyst_reversibility = str(geo_context.get("catalyst_reversibility") or "high")
    catalyst_has_official_source = bool(geo_context.get("catalyst_has_official_source"))

    action_family = geo_context["action_family"]
    action_bonus = 0.0
    if action_family == "release":
        action_bonus += 0.05
    elif action_family == "diplomacy":
        action_bonus += 0.04
    elif action_family == "regime_shift":
        action_bonus += 0.03
    elif action_family == "conflict":
        action_bonus += 0.04

    deadline_bonus = 0.05 if has_date else 0.0
    source_bonus = 0.04 if has_source else 0.0

    repricing_potential = 0.46
    if 0.03 <= price <= 0.18:
        repricing_potential += 0.18
    elif 0.18 < price <= 0.38:
        repricing_potential += 0.10
    elif price < 0.03:
        repricing_potential += 0.06
    elif price > 0.65:
        repricing_potential -= 0.08

    if 7.0 <= days_to_close <= 150.0:
        repricing_potential += 0.12
    elif 2.0 <= days_to_close < 7.0:
        repricing_potential += 0.05
    elif days_to_close > 180.0:
        repricing_potential -= 0.06

    repricing_potential += action_bonus * 1.4
    repricing_potential += deadline_bonus
    repricing_potential += source_bonus
    repricing_potential -= 0.03 if spread > 0.05 else 0.0
    repricing_potential += max(0.0, catalyst_strength - 0.60) * 0.22
    if catalyst_hardness == "hard":
        repricing_potential += 0.03
    if catalyst_reversibility == "low":
        repricing_potential += 0.02
    if binary_event_grid and action_family in {"diplomacy", "release"}:
        repricing_potential += 0.05
    elif binary_event_grid:
        repricing_potential += 0.02

    if hard_state and action_family in {"release", "regime_shift"}:
        final_probability_penalty = 0.03
    elif hard_state and action_family == "conflict":
        final_probability_penalty = 0.01
    else:
        final_probability_penalty = 0.0
    price_shape = 0.0
    if 0.04 <= price <= 0.16:
        price_shape += 0.05
    elif 0.16 < price <= 0.32:
        price_shape += 0.03
    elif price > 0.55:
        price_shape -= 0.05

    signal = 0.50 + action_bonus + deadline_bonus + source_bonus + price_shape - final_probability_penalty

    confidence = 0.56
    confidence += 0.07 if has_date else 0.0
    confidence += 0.06 if has_source else 0.0
    confidence += min(0.06, volume24h / 4000.0)
    confidence += min(0.05, liquidity / 6000.0)
    confidence -= 0.05 if spread > 0.05 else 0.0
    confidence += 0.03 if 5.0 <= days_to_close <= 120.0 else 0.0
    confidence += 0.03 if binary_event_grid and has_date else 0.0
    confidence += max(0.0, catalyst_strength - 0.65) * 0.18
    confidence += 0.03 if catalyst_has_official_source else 0.0

    return {
        "name": "geopolitical_repricing",
        "signal": _clamp(signal),
        "confidence": _clamp(confidence),
        "components": {
            "action_family": action_family,
            "catalyst_type": catalyst_type,
            "catalyst_strength": catalyst_strength,
            "catalyst_hardness": catalyst_hardness,
            "catalyst_reversibility": catalyst_reversibility,
            "catalyst_has_official_source": catalyst_has_official_source,
            "repricing_potential": _clamp(repricing_potential),
            "days_to_close": days_to_close,
            "has_date_deadline": has_date,
            "has_resolution_source": has_source,
            "hard_state": hard_state,
            "binary_event_grid": binary_event_grid,
            "market_type": market_type,
            "geo_match_score": geo_context["match_score"],
            "geo_keywords": geo_context["geo_keywords"],
            "action_keywords": geo_context["action_keywords"],
            "institution_keywords": geo_context["institution_keywords"],
            "catalyst_keywords": geo_context.get("catalyst_keywords") or [],
            "catalyst_official_source_keywords": geo_context.get("catalyst_official_source_keywords") or [],
            "business_keywords": geo_context.get("business_keywords") or [],
            "action_bonus": action_bonus,
            "deadline_bonus": deadline_bonus,
            "source_bonus": source_bonus,
            "final_probability_penalty": final_probability_penalty,
            "price_shape": price_shape,
            "price": price,
            "spread": spread,
            "liquidity": liquidity,
            "volume24h": volume24h,
        },
    }


def compute_domain_predictor(market):
    question_text = normalize_text(market.get("question"))

    for builder in (
        _sports_odds_feed_predictor,
        _sports_match_predictor,
        _geopolitical_repricing_predictor,
        _intraday_noise_predictor,
        _dated_binary_prior,
    ):
        result = builder(market, question_text)
        if result is not None:
            return result

    return {
        "name": "neutral",
        "signal": 0.50,
        "confidence": 0.50,
        "components": {},
    }
