def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _deadline_pressure(days_to_close):
    days = max(0.0, _safe_float(days_to_close, 0.0))
    if 3.0 <= days <= 45.0:
        return 1.0
    if 1.0 <= days < 3.0:
        return 0.62
    if 45.0 < days <= 90.0:
        return 0.78
    if 90.0 < days <= 180.0:
        return 0.46
    return 0.22


def build_repricing_context(
    *,
    entry_price,
    repricing_potential,
    catalyst_strength,
    spread=None,
    liquidity=None,
    volume24h=None,
    one_hour_change=None,
    one_day_change=None,
    one_week_change=None,
    hours_to_close=None,
    max_buy_price=0.35,
):
    entry_price = _safe_float(entry_price, 0.5)
    repricing_potential = _safe_float(repricing_potential, 0.0)
    catalyst_strength = _safe_float(catalyst_strength, 0.0)
    spread = _safe_float(spread, 0.0)
    liquidity = _safe_float(liquidity, 0.0)
    volume24h = _safe_float(volume24h, 0.0)
    one_hour_change = _safe_float(one_hour_change, 0.0)
    one_day_change = _safe_float(one_day_change, 0.0)
    one_week_change = _safe_float(one_week_change, 0.0)
    days_to_close = max(0.0, _safe_float(hours_to_close, 0.0) / 24.0)

    recent_runup = _clamp(
        max(
            0.0,
            one_hour_change * 4.0,
            one_day_change * 1.3,
            one_week_change * 0.9,
        )
    )
    recent_selloff = _clamp(
        max(
            0.0,
            (-one_hour_change) * 4.0,
            (-one_day_change) * 1.3,
            (-one_week_change) * 0.9,
        )
    )
    recent_abs_move = _clamp(
        max(
            abs(one_hour_change) * 4.0,
            abs(one_day_change) * 1.3,
            abs(one_week_change) * 0.9,
        )
    )

    compression_score = 0.0
    compression_score += _clamp(max(0.0, 0.025 - abs(one_hour_change)) / 0.025) * 0.55
    compression_score += _clamp(max(0.0, 0.080 - abs(one_day_change)) / 0.080) * 0.30
    compression_score += _clamp(max(0.0, 0.140 - abs(one_week_change)) / 0.140) * 0.15
    compression_score -= _clamp(max(0.0, spread - 0.035) * 6.0) * 0.25
    compression_score = _clamp(compression_score)

    deadline_pressure = _deadline_pressure(days_to_close)
    book_quality = _clamp(
        (_clamp(liquidity / 2500.0) * 0.55) + (_clamp(volume24h / 1200.0) * 0.45)
    )
    raw_attention_gap = _clamp(repricing_potential - entry_price)

    underreaction_score = _clamp(
        (raw_attention_gap * 0.54)
        + (compression_score * 0.14)
        + (recent_selloff * 0.16)
        + (deadline_pressure * 0.08)
        + (catalyst_strength * 0.14)
        - (recent_runup * 0.62)
    )
    fresh_catalyst_score = _clamp(
        (catalyst_strength * 0.38)
        + (deadline_pressure * 0.18)
        + (compression_score * 0.18)
        + (book_quality * 0.10)
        + (_clamp(max(0.0, 0.10 - recent_runup) / 0.10) * 0.16)
        - (_clamp(max(0.0, spread - 0.03) * 6.0) * 0.08)
    )

    attention_gap = _clamp(raw_attention_gap + (recent_selloff * 0.10) - (recent_runup * 0.18))
    stale_score = _clamp(
        (attention_gap * 0.44)
        + (compression_score * 0.28)
        + (_clamp(max(0.0, 0.08 - recent_runup) / 0.08) * 0.18)
        + (deadline_pressure * 0.10)
    )

    trend_chase_penalty = _clamp(
        (recent_runup * 0.72)
        + (_clamp(max(0.0, abs(one_hour_change) - 0.025) / 0.05) * 0.16)
        + (_clamp(max(0.0, recent_abs_move - 0.10) / 0.20) * 0.12)
    )
    already_priced_penalty = _clamp(
        (max(0.0, entry_price - max_buy_price) * 1.55)
        + (recent_runup * 0.58)
        + (_clamp(max(0.0, (entry_price + recent_runup) - repricing_potential) / 0.18) * 0.34)
    )

    return {
        "attention_gap": attention_gap,
        "stale_score": stale_score,
        "already_priced_penalty": already_priced_penalty,
        "underreaction_score": underreaction_score,
        "fresh_catalyst_score": fresh_catalyst_score,
        "trend_chase_penalty": trend_chase_penalty,
        "recent_runup": recent_runup,
        "recent_selloff": recent_selloff,
        "recent_abs_move": recent_abs_move,
        "compression_score": compression_score,
        "deadline_pressure": deadline_pressure,
        "book_quality": book_quality,
        "days_to_close": days_to_close,
    }
