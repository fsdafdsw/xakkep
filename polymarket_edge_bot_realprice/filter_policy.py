from config import (
    EXCLUDED_QUESTION_PATTERNS,
    MAX_PRICE,
    MAX_SPREAD,
    MIN_HOURS_TO_CLOSE,
    MIN_LIQUIDITY,
    MIN_PRICE,
    MIN_VOLUME,
    REQUIRE_ORDERBOOK,
)
from market_profile import enrich_market_profile


_TYPE_FILTER_OVERRIDES = {
    # Historical diagnostics showed that the blanket pattern exclusion was
    # suppressing only dated binaries; those markets should still be scored
    # and rejected later by confidence/edge if they remain weak.
    "dated_binary": {
        "excluded_patterns": [],
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
