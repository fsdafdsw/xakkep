
import math


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _log_scale(value, scale):
    if value <= 0:
        return 0.0
    return _clamp(math.log1p(value) / math.log1p(scale))


def compute_quality(market):
    vol_score = _log_scale(market.get("volume24h", 0.0), 30000.0)
    liq_score = _log_scale(market.get("liquidity", 0.0), 30000.0)

    spread = market.get("spread")
    spread_score = 0.25
    if spread is not None:
        spread_score = _clamp(1 - (spread / 0.12))

    hours_to_close = market.get("hours_to_close")
    time_score = 0.6
    if hours_to_close is not None:
        # Penalize very short-dated markets where microstructure noise dominates.
        time_score = _clamp((hours_to_close - 1.0) / 48.0)

    return _clamp(
        (0.35 * vol_score)
        + (0.35 * liq_score)
        + (0.20 * spread_score)
        + (0.10 * time_score)
    )


def compute_momentum(market):
    one_hour = market.get("one_hour_change", 0.0)
    one_day = market.get("one_day_change", 0.0)
    one_week = market.get("one_week_change", 0.0)

    trend = (0.6 * one_hour) + (0.3 * one_day) + (0.1 * one_week)
    # Smooth and cap momentum to reduce over-reaction.
    return _clamp((math.tanh(trend * 5.0) + 1.0) / 2.0)


def compute_anomaly(market):
    ref_price = market.get("ref_price")
    last_trade = market.get("last_trade")
    if ref_price is None or last_trade is None:
        return 0.5

    deviation = abs(last_trade - ref_price)
    # Larger deviation can mean stale quote or temporary dislocation.
    return _clamp(deviation / 0.12)


def compute_orderbook_signal(market):
    bid = market.get("best_bid")
    ask = market.get("best_ask")
    if bid is None or ask is None:
        return 0.5
    if ask <= 0 or ask < bid:
        return 0.5

    spread = ask - bid
    # Narrow spread implies healthier executable price.
    return _clamp(1 - (spread / 0.10))
