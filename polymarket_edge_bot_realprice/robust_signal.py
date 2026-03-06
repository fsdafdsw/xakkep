from config import (
    ESTIMATED_SLIPPAGE_BPS,
    TAKER_FEE_BPS,
    UNCERTAINTY_ANOMALY_WEIGHT,
    UNCERTAINTY_BASE,
    UNCERTAINTY_CATEGORY_WEIGHT,
    UNCERTAINTY_CONFIDENCE_WEIGHT,
    UNCERTAINTY_EXTERNAL_CONF_WEIGHT,
    UNCERTAINTY_HORIZON_WEIGHT,
    UNCERTAINTY_MULTI_OUTCOME_WEIGHT,
    UNCERTAINTY_RELATION_WEIGHT,
    UNCERTAINTY_SPREAD_WEIGHT,
)
from market_profile import as_int
from probability_model import net_edge_after_costs


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def compute_robust_signal(market, metrics, fair, graph_metrics=None):
    implied = market.get("ref_price")
    entry = market.get("best_ask")
    if entry is None:
        entry = implied

    if implied is None or fair is None or entry is None:
        return {
            "meta_confidence": 0.0,
            "uncertainty": 1.0,
            "fair_lcb": None,
            "gross_edge_lcb": None,
            "net_edge_lcb": None,
            "robustness_score": 0.0,
            "components": {},
        }

    quality = _safe_float(metrics.get("quality"), 0.5)
    orderbook = _safe_float(metrics.get("orderbook"), 0.5)
    anomaly = _safe_float(metrics.get("anomaly"), 0.5)
    base_confidence = _safe_float(metrics.get("confidence"), 0.5)
    external_confidence = _safe_float(metrics.get("external_confidence"), 0.5)
    domain_confidence = _safe_float(metrics.get("domain_confidence"), external_confidence)
    external_components = metrics.get("external_components") or {}
    domain = external_components.get("domain") or {}
    domain_components = domain.get("components") or {}
    relation_metrics = external_components.get("relation_metrics") or {}
    relation_residual = external_components.get("relation_residual") or {}
    resolution_metadata = external_components.get("resolution_metadata") or {}
    resolution_quality = _safe_float(external_components.get("resolution_quality"), 0.5)
    specificity = _safe_float(external_components.get("specificity"), 0.5)
    semantic_confidence = _safe_float(resolution_metadata.get("confidence"), 0.0)
    relation_confidence = _safe_float(relation_metrics.get("relation_confidence"), 0.0)
    relation_degree = _safe_float(relation_metrics.get("relation_degree"), 0.0)
    relation_support_confidence = _safe_float(relation_residual.get("support_confidence"), 0.0)
    relation_constraint_violation = _safe_float(relation_residual.get("constraint_violation"), 0.0)
    relation_inconsistency = _safe_float(relation_residual.get("inconsistency_score"), 0.0)
    relation_residual_gap = _safe_float(relation_residual.get("residual"), 0.0)
    horizon_risk = _safe_float(external_components.get("horizon_risk"), 0.25)
    category_risk = _safe_float(external_components.get("category_risk"), 0.0)
    competition_risk = _safe_float(external_components.get("competition_risk"), 0.0)
    domain_draw_penalty = _safe_float(domain_components.get("structural_penalty"), 0.0)
    domain_line_confirmation = _safe_float(domain_components.get("line_confirmation"), 0.0)
    domain_noise_flag = 1.0 if domain.get("name") == "intraday_noise_penalty" else 0.0
    odds_match_quality = _safe_float(domain_components.get("match_quality"), 0.0)
    odds_bookmaker_count = _safe_float(domain_components.get("bookmaker_count"), 0.0)
    odds_dispersion = _safe_float(domain_components.get("probability_dispersion"), 0.0)
    odds_feed_flag = 1.0 if domain.get("name") == "sports_odds_feed" else 0.0
    price_extremeness = _clamp(abs(float(implied) - 0.5) / 0.5)
    spread = market.get("spread")
    spread_risk = _clamp((_safe_float(spread, 0.0)) / 0.10)
    event_market_count = max(1, as_int(market.get("event_market_count"), default=1))
    multi_outcome_risk = _clamp((event_market_count - 1) / 8.0)
    raw_adjustment = abs(float(fair) - float(implied))

    reliability = 0.0
    reliability += quality * 0.26
    reliability += orderbook * 0.16
    reliability += (1.0 - anomaly) * 0.18
    reliability += external_confidence * 0.14
    reliability += domain_confidence * 0.08
    reliability += resolution_quality * 0.10
    reliability += specificity * 0.08
    reliability += base_confidence * 0.08
    reliability += max(0.0, domain_line_confirmation) * 0.08
    reliability += semantic_confidence * 0.08
    reliability += relation_confidence * 0.08
    reliability += min(0.06, relation_degree * 0.01)
    reliability += relation_support_confidence * 0.12
    reliability += min(0.06, max(0.0, relation_residual_gap) * 1.5)
    reliability += odds_match_quality * 0.10
    reliability += min(0.10, odds_bookmaker_count * 0.015)
    reliability += odds_feed_flag * 0.06

    skepticism = 0.0
    skepticism += horizon_risk * 0.24
    skepticism += category_risk * 0.16
    skepticism += competition_risk * 0.12
    skepticism += spread_risk * 0.12
    skepticism += price_extremeness * 0.10
    skepticism += multi_outcome_risk * 0.08
    skepticism += domain_draw_penalty * 0.20
    skepticism += domain_noise_flag * 0.18
    skepticism += min(0.12, odds_dispersion * 2.8)
    skepticism += relation_inconsistency * 0.18
    skepticism += min(0.12, relation_constraint_violation * 1.8)

    supported_adjustment = min(raw_adjustment, 0.035) / 0.035
    overreach = max(0.0, raw_adjustment - (0.008 + (0.030 * reliability)))
    meta_confidence = 0.18 + (reliability * 0.74) + (supported_adjustment * 0.08)
    meta_confidence -= skepticism * 0.32
    meta_confidence -= overreach * 4.5
    meta_confidence = _clamp(meta_confidence)

    uncertainty = UNCERTAINTY_BASE
    uncertainty += (1.0 - base_confidence) * UNCERTAINTY_CONFIDENCE_WEIGHT
    uncertainty += (1.0 - external_confidence) * UNCERTAINTY_EXTERNAL_CONF_WEIGHT
    uncertainty += anomaly * UNCERTAINTY_ANOMALY_WEIGHT
    uncertainty += spread_risk * UNCERTAINTY_SPREAD_WEIGHT
    uncertainty += horizon_risk * UNCERTAINTY_HORIZON_WEIGHT
    uncertainty += category_risk * UNCERTAINTY_CATEGORY_WEIGHT
    uncertainty += multi_outcome_risk * UNCERTAINTY_MULTI_OUTCOME_WEIGHT
    uncertainty += relation_inconsistency * UNCERTAINTY_RELATION_WEIGHT
    uncertainty += competition_risk * 0.012
    uncertainty += price_extremeness * 0.015
    uncertainty += domain_draw_penalty * 0.012
    uncertainty += domain_noise_flag * 0.018
    uncertainty += min(0.015, odds_dispersion * 0.50)
    uncertainty -= min(0.012, odds_match_quality * 0.012)
    uncertainty -= min(0.010, odds_bookmaker_count * 0.0015)
    uncertainty += max(0.0, 0.60 - meta_confidence) * 0.040
    uncertainty = max(0.004, min(uncertainty, 0.18))

    graph = graph_metrics or {}
    graph_consistency = _safe_float(graph.get("consistency"), 0.58)
    correlation_penalty = _safe_float(graph.get("correlation_penalty"), 0.0)
    graph_penalty = max(0.0, 0.60 - graph_consistency) * 0.050
    regime_penalty = max(0.0, horizon_risk - 0.55) * 0.030
    regime_penalty += max(0.0, category_risk - 0.20) * 0.020
    total_penalty = uncertainty + correlation_penalty + graph_penalty + regime_penalty

    fair_lcb = _clamp(float(fair) - total_penalty, low=0.01, high=0.99)
    gross_edge_lcb = fair_lcb - float(entry)
    net_edge_lcb = net_edge_after_costs(
        fair_probability=fair_lcb,
        entry_price=float(entry),
        taker_fee_bps=TAKER_FEE_BPS,
        slippage_bps=ESTIMATED_SLIPPAGE_BPS,
        spread=spread,
    )

    robustness_score = 0.0
    robustness_score += meta_confidence * 0.42
    robustness_score += graph_consistency * 0.24
    robustness_score += base_confidence * 0.18
    robustness_score += quality * 0.10
    robustness_score += (1.0 - category_risk) * 0.06
    robustness_score -= correlation_penalty * 2.2
    robustness_score = _clamp(robustness_score)

    return {
        "meta_confidence": meta_confidence,
        "uncertainty": uncertainty,
        "fair_lcb": fair_lcb,
        "gross_edge_lcb": gross_edge_lcb,
        "net_edge_lcb": net_edge_lcb,
        "robustness_score": robustness_score,
        "components": {
            "reliability": reliability,
            "skepticism": skepticism,
            "raw_adjustment": raw_adjustment,
            "supported_adjustment": supported_adjustment,
            "overreach": overreach,
            "price_extremeness": price_extremeness,
            "spread_risk": spread_risk,
            "multi_outcome_risk": multi_outcome_risk,
            "graph_consistency": graph_consistency,
            "correlation_penalty": correlation_penalty,
            "graph_penalty": graph_penalty,
            "regime_penalty": regime_penalty,
            "total_penalty": total_penalty,
            "domain_name": domain.get("name"),
            "domain_draw_penalty": domain_draw_penalty,
            "domain_line_confirmation": domain_line_confirmation,
            "domain_noise_flag": domain_noise_flag,
            "odds_feed_flag": odds_feed_flag,
            "odds_match_quality": odds_match_quality,
            "odds_bookmaker_count": odds_bookmaker_count,
            "odds_dispersion": odds_dispersion,
            "semantic_confidence": semantic_confidence,
            "relation_confidence": relation_confidence,
            "relation_degree": relation_degree,
            "relation_support_confidence": relation_support_confidence,
            "relation_constraint_violation": relation_constraint_violation,
            "relation_inconsistency": relation_inconsistency,
            "relation_residual_gap": relation_residual_gap,
        },
    }
