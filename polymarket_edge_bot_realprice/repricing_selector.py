from config import (
    MAX_SPREAD,
    MAX_REPRICING_BUY_PRICE,
    MIN_REPRICING_BUY_SCORE,
    MIN_REPRICING_WATCH_SCORE,
)


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


def score_repricing_signal(
    *,
    entry_price,
    confidence,
    net_edge,
    net_edge_lcb,
    spread,
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
    liquidity = _safe_float(components.get("liquidity"), 0.0)
    volume24h = _safe_float(components.get("volume24h"), 0.0)
    relation_support_confidence = _safe_float(relation_residual.get("support_confidence"), 0.0)
    relation_residual_gap = _safe_float(relation_residual.get("residual"), 0.0)

    attention_gap = _clamp(repricing_potential - entry_price)
    stale_score = _clamp((attention_gap * 0.70) + (max(0.0, 0.18 - entry_price) * 1.10))
    already_priced_penalty = _clamp(max(0.0, entry_price - MAX_REPRICING_BUY_PRICE) * 1.7)
    spread_penalty = _clamp(max(0.0, spread - min(MAX_SPREAD, 0.05)) * 6.0)
    liquidity_penalty = _clamp(max(0.0, (250.0 - liquidity) / 250.0) * 0.25)
    volume_penalty = _clamp(max(0.0, (200.0 - volume24h) / 200.0) * 0.18)

    catalyst_bonus = catalyst_strength * 0.24
    if catalyst_hardness == "hard":
        catalyst_bonus += 0.05
    if catalyst_reversibility == "low":
        catalyst_bonus += 0.03
    if catalyst_has_official_source:
        catalyst_bonus += 0.04

    score = 0.18
    score += repricing_potential * 0.24
    score += attention_gap * 0.22
    score += stale_score * 0.14
    score += catalyst_bonus
    score += (confidence - 0.5) * 0.18
    score += (domain_confidence - 0.5) * 0.12
    score += max(-0.03, min(0.03, relation_residual_gap)) * 1.1
    score += relation_support_confidence * 0.05
    score += max(0.0, min(0.03, net_edge)) * 1.8
    score -= max(0.0, -net_edge_lcb) * 0.35
    score -= already_priced_penalty * 0.28
    score -= spread_penalty * 0.18
    score -= liquidity_penalty
    score -= volume_penalty

    watch_score = score + (attention_gap * 0.10) + (0.04 if catalyst_type in {"hostage_release", "appeal", "court_ruling", "military_action"} else 0.0)
    score = _clamp(score)
    watch_score = _clamp(watch_score)

    verdict = "ignore"
    reason = "repricing score too low"
    if score >= MIN_REPRICING_BUY_SCORE and entry_price <= MAX_REPRICING_BUY_PRICE:
        verdict = "buy_now"
        reason = "strong catalyst with enough attention gap"
    elif watch_score >= MIN_REPRICING_WATCH_SCORE:
        if entry_price > MAX_REPRICING_BUY_PRICE:
            verdict = "watch_late"
            reason = "interesting catalyst, but market may already be partially repriced"
        else:
            verdict = "watch"
            reason = "strong catalyst, waiting for cleaner entry or confirmation"

    return {
        "score": score,
        "watch_score": watch_score,
        "verdict": verdict,
        "reason": reason,
        "buy_now": verdict == "buy_now",
        "watch": verdict in {"buy_now", "watch", "watch_late"},
        "attention_gap": attention_gap,
        "stale_score": stale_score,
        "already_priced_penalty": already_priced_penalty,
        "catalyst_type": catalyst_type,
        "catalyst_strength": catalyst_strength,
        "action_family": components.get("action_family"),
        "hardness": catalyst_hardness,
        "reversibility": catalyst_reversibility,
        "has_official_source": catalyst_has_official_source,
        "market_type": market_type,
        "category_group": category_group,
    }
