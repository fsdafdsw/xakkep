
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


def compute_volume_anomaly(market):
    volume24h = float(market.get("volume24h") or 0.0)
    volume_total = float(market.get("volume") or volume24h or 0.0)
    liquidity = float(market.get("liquidity") or 0.0)
    spread = market.get("spread")
    spread = 0.04 if spread is None else float(spread)

    one_hour = float(market.get("one_hour_change") or 0.0)
    one_day = float(market.get("one_day_change") or 0.0)
    one_week = float(market.get("one_week_change") or 0.0)

    total_baseline = volume_total / 30.0 if volume_total > 0 else 0.0
    liquidity_baseline = liquidity * 0.12 if liquidity > 0 else 0.0
    expected_volume = max(75.0, total_baseline, liquidity_baseline)

    volume_ratio = volume24h / expected_volume if expected_volume > 0 else 0.0
    anomaly_score = _clamp((volume_ratio - 1.0) / 4.0)

    directional_move = (one_hour * 0.60) + (one_day * 0.30) + (one_week * 0.10)
    positive_move = _clamp(max(0.0, directional_move) / 0.08)
    move_strength = _clamp(max(abs(one_hour) * 8.0, abs(one_day) * 2.0, abs(one_week)))
    spread_quality = _clamp(1.0 - (spread / 0.10))

    confirmation = _clamp(
        (anomaly_score * 0.52)
        + (positive_move * 0.28)
        + (move_strength * 0.12)
        + (spread_quality * 0.08)
    )

    pressure = _clamp((math.tanh(directional_move * 8.0) + 1.0) / 2.0)

    return {
        "anomaly_score": anomaly_score,
        "confirmation": confirmation,
        "pressure": pressure,
        "volume_ratio": volume_ratio,
        "expected_volume": expected_volume,
    }


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
