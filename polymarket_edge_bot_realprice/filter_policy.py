from config import (
    EDGE_THRESHOLD,
    EXCLUDED_QUESTION_PATTERNS,
    MAX_PRICE,
    MAX_SPREAD,
    MIN_CONFIDENCE,
    MIN_GROSS_EDGE,
    MIN_HOURS_TO_CLOSE,
    MIN_LIQUIDITY,
    MIN_PRICE,
    MIN_VOLUME,
    REQUIRE_ORDERBOOK,
    WATCH_THRESHOLD,
)
from market_profile import enrich_market_profile


_TYPE_FILTER_OVERRIDES = {
    # Longshot-heavy multi-outcome markets performed poorly in earlier scans.
    # Tighten price bands there instead of letting cheap tails dominate.
    "winner_multi": {
        "min_price": max(MIN_PRICE, 0.05),
        "max_price": min(MAX_PRICE, 0.90),
    },
    "range_multi": {
        "min_price": max(MIN_PRICE, 0.05),
        "max_price": min(MAX_PRICE, 0.90),
    },
    # Historical diagnostics showed that the blanket pattern exclusion was
    # suppressing only dated binaries; those markets should still be scored
    # and rejected later by confidence/edge if they remain weak.
    "dated_binary": {
        "excluded_patterns": [],
    },
    "price_target": {
        "min_price": max(MIN_PRICE, 0.05),
        "max_price": min(MAX_PRICE, 0.95),
    },
}

_TYPE_SCORING_OVERRIDES = {
    # Historical backtests showed dated binaries clustering near 0.80
    # confidence and ~1.0% gross edge; relax those gates modestly so they can
    # reach watchlist/value ranking without forcing obviously weak markets.
    "dated_binary": {
        "min_confidence": min(MIN_CONFIDENCE, 0.80),
        "min_gross_edge": min(MIN_GROSS_EDGE, 0.010),
        "edge_threshold": min(EDGE_THRESHOLD, 0.015),
        "watch_threshold": min(WATCH_THRESHOLD, 0.009),
    },
    # Near-term binaries still need edge discipline, but confidence can be a
    # bit lower because their resolution path is shorter and cleaner.
    "near_term_binary": {
        "min_confidence": min(MIN_CONFIDENCE, 0.86),
        "min_gross_edge": min(MIN_GROSS_EDGE, 0.015),
        "watch_threshold": min(WATCH_THRESHOLD, 0.010),
    },
}


def filter_policy_for_market(market):
    profile = enrich_market_profile(market)
    policy = {
        "market_type": profile["market_type"],
        "excluded_patterns": list(EXCLUDED_QUESTION_PATTERNS),
        "min_volume": MIN_VOLUME,
        "min_liquidity": MIN_LIQUIDITY,
        "min_price": MIN_PRICE,
        "max_price": MAX_PRICE,
        "max_spread": MAX_SPREAD,
        "min_hours_to_close": MIN_HOURS_TO_CLOSE,
        "require_orderbook": REQUIRE_ORDERBOOK,
    }
    policy.update(_TYPE_FILTER_OVERRIDES.get(profile["market_type"], {}))
    return policy


def scoring_policy_for_market_type(market_type):
    policy = {
        "market_type": market_type,
        "min_confidence": MIN_CONFIDENCE,
        "min_gross_edge": MIN_GROSS_EDGE,
        "edge_threshold": EDGE_THRESHOLD,
        "watch_threshold": WATCH_THRESHOLD,
    }
    policy.update(_TYPE_SCORING_OVERRIDES.get(market_type, {}))
    return policy


def scoring_policy_for_market(market):
    profile = enrich_market_profile(market)
    return scoring_policy_for_market_type(profile["market_type"])


def signal_bucket(net_edge, policy):
    if net_edge is None:
        return None
    if net_edge > policy["edge_threshold"]:
        return "value"
    if net_edge > policy["watch_threshold"]:
        return "watch"
    return None


def filter_reason(market, entry_price=None, use_liquidity_filter=True):
    policy = filter_policy_for_market(market)
    question = str(market.get("question") or "").lower()
    for pattern in policy["excluded_patterns"]:
        if pattern and pattern in question:
            return "excluded_pattern"

    volume_ref = market.get("volume24h", 0.0) or market.get("volume", 0.0)
    ref_price = market.get("ref_price")
    price = entry_price if entry_price is not None else ref_price

    if use_liquidity_filter and market.get("liquidity", 0.0) < policy["min_liquidity"]:
        return "low_liquidity"

    if volume_ref < policy["min_volume"]:
        return "low_volume"

    if ref_price is None:
        return "no_price"

    if policy["require_orderbook"] and (
        market.get("best_bid") is None or market.get("best_ask") is None
    ):
        return "no_orderbook"

    if price is None or price < policy["min_price"] or price > policy["max_price"]:
        return "extreme_price"

    spread = market.get("spread")
    if spread is not None and spread > policy["max_spread"]:
        return "wide_spread"

    hours_to_close = market.get("hours_to_close")
    if hours_to_close is not None and hours_to_close < policy["min_hours_to_close"]:
        return "near_expiry"

    return None
