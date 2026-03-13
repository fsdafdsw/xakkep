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


def _urgency_profile(days_to_close, action_family=None, catalyst_type=None, meeting_subtype=None, market_type=None):
    days = max(0.0, _safe_float(days_to_close, 0.0))
    action_family = str(action_family or "")
    catalyst_type = str(catalyst_type or "")
    meeting_subtype = str(meeting_subtype or "")
    market_type = str(market_type or "")

    score = _deadline_pressure(days)
    phase = "standard"
    decay_rate = 1.0

    if action_family == "conflict" or catalyst_type == "military_action":
        if days <= 1.0:
            score, phase, decay_rate = 0.98, "imminent", 3.0
        elif days <= 3.0:
            score, phase, decay_rate = 0.90, "urgent", 2.4
        elif days <= 7.0:
            score, phase, decay_rate = 0.78, "hot", 1.8
        elif days <= 21.0:
            score, phase, decay_rate = 0.56, "building", 1.2
        elif days <= 60.0:
            score, phase, decay_rate = 0.32, "early", 0.7
        else:
            score, phase, decay_rate = 0.16, "distant", 0.4
    elif action_family == "release" and catalyst_type in {"hearing", "court_ruling", "appeal"}:
        if days <= 2.0:
            score, phase, decay_rate = 0.95, "calendar_imminent", 2.8
        elif days <= 7.0:
            score, phase, decay_rate = 0.86, "calendar_urgent", 2.1
        elif days <= 21.0:
            score, phase, decay_rate = 0.74, "approaching_hearing", 1.5
        elif days <= 45.0:
            score, phase, decay_rate = 0.54, "on_calendar", 1.0
        elif days <= 90.0:
            score, phase, decay_rate = 0.34, "too_early", 0.6
        else:
            score, phase, decay_rate = 0.18, "distant", 0.35
    elif action_family == "release" and catalyst_type == "hostage_release":
        if days <= 3.0:
            score, phase, decay_rate = 0.82, "urgent_window", 1.9
        elif days <= 14.0:
            score, phase, decay_rate = 0.70, "active_window", 1.4
        elif days <= 45.0:
            score, phase, decay_rate = 0.52, "developing", 0.95
        elif days <= 90.0:
            score, phase, decay_rate = 0.34, "too_early", 0.6
        else:
            score, phase, decay_rate = 0.18, "distant", 0.35
    elif action_family == "diplomacy" and catalyst_type == "call_or_meeting":
        if meeting_subtype == "talk_call":
            if days <= 3.0:
                score, phase, decay_rate = 0.74, "call_window", 1.5
            elif days <= 14.0:
                score, phase, decay_rate = 0.84, "active_window", 1.6
            elif days <= 30.0:
                score, phase, decay_rate = 0.78, "ripe", 1.25
            elif days <= 60.0:
                score, phase, decay_rate = 0.58, "forming", 0.9
            elif days <= 120.0:
                score, phase, decay_rate = 0.34, "too_early", 0.55
            else:
                score, phase, decay_rate = 0.16, "distant", 0.30
        elif meeting_subtype == "meeting":
            if days <= 7.0:
                score, phase, decay_rate = 0.66, "meeting_window", 1.3
            elif days <= 21.0:
                score, phase, decay_rate = 0.78, "active_window", 1.35
            elif days <= 45.0:
                score, phase, decay_rate = 0.70, "forming", 1.05
            elif days <= 90.0:
                score, phase, decay_rate = 0.50, "early", 0.75
            else:
                score, phase, decay_rate = 0.22, "distant", 0.35
        elif meeting_subtype == "resume_talks":
            if days <= 14.0:
                score, phase, decay_rate = 0.72, "restart_window", 1.2
            elif days <= 45.0:
                score, phase, decay_rate = 0.80, "active_window", 1.3
            elif days <= 90.0:
                score, phase, decay_rate = 0.64, "forming", 0.95
            elif days <= 180.0:
                score, phase, decay_rate = 0.38, "early", 0.55
            else:
                score, phase, decay_rate = 0.18, "distant", 0.30
    elif action_family == "diplomacy" and catalyst_type == "ceasefire":
        if days <= 3.0:
            score, phase, decay_rate = 0.88, "urgent_window", 1.9
        elif days <= 14.0:
            score, phase, decay_rate = 0.80, "active_window", 1.5
        elif days <= 45.0:
            score, phase, decay_rate = 0.58, "developing", 1.0
        elif days <= 90.0:
            score, phase, decay_rate = 0.36, "early", 0.6
        else:
            score, phase, decay_rate = 0.18, "distant", 0.35
    elif market_type in {"dated_binary", "near_term_binary"}:
        if days <= 1.0:
            score, phase, decay_rate = 0.92, "imminent", 2.6
        elif days <= 3.0:
            score, phase, decay_rate = 0.80, "urgent", 1.8
        elif days <= 7.0:
            score, phase, decay_rate = 0.66, "approaching", 1.3
        elif days <= 30.0:
            score, phase, decay_rate = 0.48, "normal", 1.0
        else:
            score, phase, decay_rate = 0.24, "distant", 0.45

    edge_multiplier = 1.0 + ((score - 0.5) * 0.30)
    return {
        "urgency_score": _clamp(score),
        "phase": phase,
        "decay_rate": decay_rate,
        "edge_multiplier": edge_multiplier,
    }


def build_repricing_context(
    *,
    entry_price,
    repricing_potential,
    catalyst_strength,
    action_family=None,
    catalyst_type=None,
    meeting_subtype=None,
    market_type=None,
    spread=None,
    liquidity=None,
    volume24h=None,
    one_hour_change=None,
    one_day_change=None,
    one_week_change=None,
    hours_to_close=None,
    volume_anomaly=None,
    volume_confirmation=None,
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
    volume_anomaly = _safe_float(volume_anomaly, 0.0)
    volume_confirmation = _safe_float(volume_confirmation, 0.0)
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

    urgency = _urgency_profile(
        days_to_close,
        action_family=action_family,
        catalyst_type=catalyst_type,
        meeting_subtype=meeting_subtype,
        market_type=market_type,
    )
    deadline_pressure = urgency["urgency_score"]
    book_quality = _clamp(
        (_clamp(liquidity / 2500.0) * 0.55) + (_clamp(volume24h / 1200.0) * 0.45)
    )
    raw_attention_gap = _clamp(repricing_potential - entry_price)
    volume_support = _clamp((volume_anomaly * 0.60) + (volume_confirmation * 0.40))

    underreaction_score = _clamp(
        (raw_attention_gap * 0.54)
        + (compression_score * 0.14)
        + (recent_selloff * 0.16)
        + (deadline_pressure * 0.08)
        + (catalyst_strength * 0.14)
        + (volume_support * 0.10)
        - (recent_runup * 0.62)
    )
    fresh_catalyst_score = _clamp(
        (catalyst_strength * 0.38)
        + (deadline_pressure * 0.18)
        + (compression_score * 0.18)
        + (book_quality * 0.10)
        + (volume_support * 0.12)
        + (_clamp(max(0.0, 0.10 - recent_runup) / 0.10) * 0.16)
        - (_clamp(max(0.0, spread - 0.03) * 6.0) * 0.08)
    )

    attention_gap = _clamp(
        raw_attention_gap
        + (recent_selloff * 0.10)
        + (volume_support * 0.04)
        - (recent_runup * 0.18)
    )
    stale_score = _clamp(
        (attention_gap * 0.44)
        + (compression_score * 0.28)
        + (volume_anomaly * 0.08)
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
        "volume_support": volume_support,
        "volume_anomaly": volume_anomaly,
        "volume_confirmation": volume_confirmation,
        "days_to_close": days_to_close,
        "urgency_phase": urgency["phase"],
        "urgency_decay_rate": urgency["decay_rate"],
        "urgency_edge_multiplier": urgency["edge_multiplier"],
    }
