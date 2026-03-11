from config import (
    CONFLICT_FAST_LANE_BONUS,
    CONFLICT_MIN_SETUP_SCORE,
    CONFLICT_MIN_URGENCY_SCORE,
    CONFLICT_REPRICING_BUY_SCORE,
    DIPLOMACY_HIGH_UPSIDE_OPTIONALITY,
    DIPLOMACY_REPRICING_BUY_SCORE,
    MAX_SPREAD,
    MAX_REPRICING_BUY_PRICE,
    MIN_REPRICING_BUY_SCORE,
    MIN_REPRICING_WATCH_SCORE,
    RELEASE_REPRICING_BUY_SCORE,
)
from repricing_context import build_repricing_context


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _domain_components(model):
    external = (model or {}).get("external_components") or {}
    domain = external.get("domain") or {}
    return domain.get("components") or {}


def _family_policy(action_family, catalyst_hardness):
    policy = {
        "buy_threshold": MIN_REPRICING_BUY_SCORE,
        "watch_threshold": MIN_REPRICING_WATCH_SCORE,
        "min_underreaction": 0.48,
        "min_fresh": 0.58,
        "max_chase": 0.16,
        "max_already_priced": 0.34,
        "high_upside_optionality": 0.70,
        "high_upside_max_chase": 0.10,
        "high_upside_max_already_priced": 0.22,
        "allow_high_upside": True,
        "require_hard_for_buy": False,
        "require_official_source_for_buy": False,
        "min_setup_score": 0.0,
        "min_urgency_score": 0.0,
        "family_bonus": 0.0,
    }

    if action_family == "conflict":
        policy.update(
            {
                "buy_threshold": CONFLICT_REPRICING_BUY_SCORE,
                "min_underreaction": 0.52,
                "min_fresh": 0.66,
                "max_chase": 0.10,
                "max_already_priced": 0.24,
                "allow_high_upside": False,
                "min_setup_score": CONFLICT_MIN_SETUP_SCORE,
                "min_urgency_score": CONFLICT_MIN_URGENCY_SCORE,
                "family_bonus": CONFLICT_FAST_LANE_BONUS,
            }
        )
    elif action_family == "diplomacy":
        policy.update(
            {
                "buy_threshold": DIPLOMACY_REPRICING_BUY_SCORE,
                "min_underreaction": 0.58,
                "min_fresh": 0.70 if catalyst_hardness == "hard" else 0.78,
                "max_chase": 0.08,
                "max_already_priced": 0.20,
                "high_upside_optionality": DIPLOMACY_HIGH_UPSIDE_OPTIONALITY,
                "high_upside_max_chase": 0.12,
                "high_upside_max_already_priced": 0.26,
                "allow_high_upside": True,
                "require_hard_for_buy": True,
                "require_official_source_for_buy": True,
            }
        )
    elif action_family == "release":
        policy.update(
            {
                "buy_threshold": RELEASE_REPRICING_BUY_SCORE,
                "min_underreaction": 0.50,
                "min_fresh": 0.62,
                "max_chase": 0.12,
                "max_already_priced": 0.28,
                "allow_high_upside": True,
            }
        )
    elif action_family == "regime_shift":
        policy.update(
            {
                "buy_threshold": max(MIN_REPRICING_BUY_SCORE, 0.80),
                "min_underreaction": 0.54,
                "min_fresh": 0.68,
                "max_chase": 0.10,
                "max_already_priced": 0.22,
                "allow_high_upside": True,
            }
        )

    return policy


def score_repricing_signal(
    *,
    entry_price,
    confidence,
    net_edge,
    net_edge_lcb,
    spread,
    liquidity=None,
    volume24h=None,
    one_hour_change=None,
    one_day_change=None,
    one_week_change=None,
    hours_to_close=None,
    model,
    market_type=None,
    category_group=None,
):
    model = model or {}
    domain_name = model.get("domain_name")
    components = _domain_components(model)
    relation_residual = ((model.get("external_components") or {}).get("relation_residual") or {})

    if domain_name != "geopolitical_repricing":
        return {
            "score": 0.0,
            "watch_score": 0.0,
            "verdict": "ignore",
            "reason": "not repricing domain",
            "buy_now": False,
            "watch": False,
            "attention_gap": 0.0,
            "stale_score": 0.0,
            "already_priced_penalty": 0.0,
            "catalyst_type": components.get("catalyst_type") or "generic",
            "catalyst_strength": _safe_float(components.get("catalyst_strength")),
            "action_family": components.get("action_family"),
        }

    entry_price = _safe_float(entry_price, 0.5)
    spread = _safe_float(spread, 0.0)
    confidence = _safe_float(confidence, 0.5)
    net_edge = _safe_float(net_edge, 0.0)
    net_edge_lcb = _safe_float(net_edge_lcb, 0.0)
    domain_confidence = _safe_float(model.get("domain_confidence"), 0.5)
    repricing_potential = _safe_float(components.get("repricing_potential"), 0.0)
    catalyst_strength = _safe_float(components.get("catalyst_strength"), 0.0)
    catalyst_type = str(components.get("catalyst_type") or "generic")
    catalyst_hardness = str(components.get("catalyst_hardness") or "soft")
    catalyst_reversibility = str(components.get("catalyst_reversibility") or "high")
    catalyst_has_official_source = bool(components.get("catalyst_has_official_source"))
    liquidity = _safe_float(liquidity, _safe_float(components.get("liquidity"), 0.0))
    volume24h = _safe_float(volume24h, _safe_float(components.get("volume24h"), 0.0))
    one_hour_change = _safe_float(one_hour_change, _safe_float(components.get("one_hour_change"), 0.0))
    one_day_change = _safe_float(one_day_change, _safe_float(components.get("one_day_change"), 0.0))
    one_week_change = _safe_float(one_week_change, _safe_float(components.get("one_week_change"), 0.0))
    hours_to_close = _safe_float(hours_to_close, _safe_float(components.get("hours_to_close"), 0.0))
    relation_support_confidence = _safe_float(relation_residual.get("support_confidence"), 0.0)
    relation_residual_gap = _safe_float(relation_residual.get("residual"), 0.0)

    repricing_context = build_repricing_context(
        entry_price=entry_price,
        repricing_potential=repricing_potential,
        catalyst_strength=catalyst_strength,
        spread=spread,
        liquidity=liquidity,
        volume24h=volume24h,
        one_hour_change=one_hour_change,
        one_day_change=one_day_change,
        one_week_change=one_week_change,
        hours_to_close=hours_to_close,
        max_buy_price=MAX_REPRICING_BUY_PRICE,
    )
    attention_gap = repricing_context["attention_gap"]
    stale_score = repricing_context["stale_score"]
    already_priced_penalty = repricing_context["already_priced_penalty"]
    underreaction_score = repricing_context["underreaction_score"]
    fresh_catalyst_score = repricing_context["fresh_catalyst_score"]
    trend_chase_penalty = repricing_context["trend_chase_penalty"]
    compression_score = repricing_context["compression_score"]
    deadline_pressure = repricing_context["deadline_pressure"]
    spread_penalty = _clamp(max(0.0, spread - min(MAX_SPREAD, 0.05)) * 6.0)
    liquidity_penalty = _clamp(max(0.0, (250.0 - liquidity) / 250.0) * 0.25)
    volume_penalty = _clamp(max(0.0, (200.0 - volume24h) / 200.0) * 0.18)
    low_price_optionality = _clamp(max(0.0, 0.24 - entry_price) / 0.24)

    catalyst_bonus = catalyst_strength * 0.24
    if catalyst_hardness == "hard":
        catalyst_bonus += 0.05
    if catalyst_reversibility == "low":
        catalyst_bonus += 0.03
    if catalyst_has_official_source:
        catalyst_bonus += 0.04

    score = 0.18
    score += repricing_potential * 0.17
    score += underreaction_score * 0.20
    score += fresh_catalyst_score * 0.16
    score += attention_gap * 0.12
    score += stale_score * 0.10
    score += catalyst_bonus
    score += (confidence - 0.5) * 0.18
    score += (domain_confidence - 0.5) * 0.12
    score += max(-0.03, min(0.03, relation_residual_gap)) * 1.1
    score += relation_support_confidence * 0.05
    score += max(0.0, min(0.03, net_edge)) * 1.8
    score -= max(0.0, -net_edge_lcb) * 0.35
    score -= already_priced_penalty * 0.28
    score -= trend_chase_penalty * 0.18
    score -= spread_penalty * 0.18
    score -= liquidity_penalty
    score -= volume_penalty

    watch_score = (
        score
        + (attention_gap * 0.08)
        + (underreaction_score * 0.08)
        + (fresh_catalyst_score * 0.06)
        + (0.04 if catalyst_type in {"hostage_release", "appeal", "court_ruling", "military_action"} else 0.0)
    )
    optionality_score = _clamp(
        (repricing_potential * 0.18)
        + (underreaction_score * 0.24)
        + (attention_gap * 0.16)
        + (low_price_optionality * 0.18)
        + (_clamp(1.0 - trend_chase_penalty) * 0.12)
        + (_clamp(1.0 - already_priced_penalty) * 0.12)
    )
    score = _clamp(score)
    watch_score = _clamp(watch_score)
    action_family = components.get("action_family")
    family_policy = _family_policy(action_family, catalyst_hardness)
    conflict_setup_score = 0.0
    conflict_urgency_score = 0.0
    if action_family == "conflict":
        conflict_setup_score = _clamp(
            (underreaction_score * 0.32)
            + (attention_gap * 0.20)
            + (fresh_catalyst_score * 0.18)
            + (stale_score * 0.12)
            + (confidence * 0.10)
            + (domain_confidence * 0.08)
        )
        conflict_urgency_score = _clamp(
            (deadline_pressure * 0.34)
            + (compression_score * 0.18)
            + (_clamp(1.0 - trend_chase_penalty) * 0.18)
            + (_clamp(1.0 - already_priced_penalty) * 0.18)
            + (_clamp(1.0 - entry_price / max(MAX_REPRICING_BUY_PRICE, 0.01)) * 0.12)
        )
        score = _clamp(
            score
            + (conflict_setup_score * 0.08)
            + (conflict_urgency_score * 0.07)
            + family_policy["family_bonus"]
        )
        watch_score = _clamp(
            watch_score
            + (conflict_setup_score * 0.05)
            + (conflict_urgency_score * 0.04)
            + (family_policy["family_bonus"] * 0.5)
        )
    clean_entry = (
        entry_price <= MAX_REPRICING_BUY_PRICE
        and underreaction_score >= family_policy["min_underreaction"]
        and fresh_catalyst_score >= family_policy["min_fresh"]
        and trend_chase_penalty <= family_policy["max_chase"]
        and already_priced_penalty <= family_policy["max_already_priced"]
        and conflict_setup_score >= family_policy["min_setup_score"]
        and conflict_urgency_score >= family_policy["min_urgency_score"]
        and (
            not family_policy["require_hard_for_buy"]
            or catalyst_hardness == "hard"
        )
        and (
            not family_policy["require_official_source_for_buy"]
            or catalyst_has_official_source
        )
    )

    verdict = "ignore"
    reason = "repricing score too low"
    if score >= family_policy["buy_threshold"] and clean_entry:
        verdict = "buy_now"
        reason = "fresh catalyst and market still underreacted"
    elif watch_score >= family_policy["watch_threshold"]:
        if (
            family_policy["allow_high_upside"]
            and catalyst_hardness != "hard"
            and optionality_score >= family_policy["high_upside_optionality"]
            and trend_chase_penalty <= family_policy["high_upside_max_chase"]
            and already_priced_penalty <= family_policy["high_upside_max_already_priced"]
        ):
            verdict = "watch_high_upside"
            reason = "large repricing optionality, but catalyst still soft"
        if (
            entry_price > MAX_REPRICING_BUY_PRICE
            or trend_chase_penalty > family_policy["max_chase"]
            or already_priced_penalty > family_policy["max_already_priced"]
        ):
            verdict = "watch_late"
            reason = "strong catalyst, but recent move suggests part of repricing is gone"
        elif verdict != "watch_high_upside":
            verdict = "watch"
            reason = "strong catalyst, but waiting for cleaner entry or confirmation"
    elif trend_chase_penalty >= 0.32:
        reason = "market already moved too much for a clean repricing entry"
    elif underreaction_score < 0.35:
        reason = "catalyst is not strong enough versus current market pricing"

    return {
        "score": score,
        "watch_score": watch_score,
        "verdict": verdict,
        "reason": reason,
        "buy_now": verdict == "buy_now",
        "watch": verdict in {"buy_now", "watch", "watch_high_upside", "watch_late"},
        "attention_gap": attention_gap,
        "stale_score": stale_score,
        "already_priced_penalty": already_priced_penalty,
        "underreaction_score": underreaction_score,
        "fresh_catalyst_score": fresh_catalyst_score,
        "trend_chase_penalty": trend_chase_penalty,
        "optionality_score": optionality_score,
        "recent_runup": repricing_context["recent_runup"],
        "recent_selloff": repricing_context["recent_selloff"],
        "compression_score": repricing_context["compression_score"],
        "deadline_pressure": repricing_context["deadline_pressure"],
        "book_quality": repricing_context["book_quality"],
        "family_policy": family_policy,
        "conflict_setup_score": conflict_setup_score,
        "conflict_urgency_score": conflict_urgency_score,
        "catalyst_type": catalyst_type,
        "catalyst_strength": catalyst_strength,
        "action_family": action_family,
        "hardness": catalyst_hardness,
        "reversibility": catalyst_reversibility,
        "has_official_source": catalyst_has_official_source,
        "market_type": market_type,
        "category_group": category_group,
    }
