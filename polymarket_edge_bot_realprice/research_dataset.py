import json
from pathlib import Path

from config import ESTIMATED_SLIPPAGE_BPS, REPORTS_DIR, TAKER_FEE_BPS
from utils import safe_float as _safe_float


def _to_utc_str(ts):
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def cost_per_share(entry_price, spread):
    entry = _safe_float(entry_price, default=None)
    if entry is None:
        return None

    fee = entry * ((TAKER_FEE_BPS + ESTIMATED_SLIPPAGE_BPS) / 10000.0)
    half_spread = (_safe_float(spread, default=0.0) or 0.0) / 2.0
    return fee + half_spread


def realized_pnl_per_share(candidate):
    entry = _safe_float(getattr(candidate, "entry", None), default=None)
    resolved = _safe_float(getattr(candidate, "resolved_outcome", None), default=None)
    if entry is None or resolved is None:
        return None

    cost = cost_per_share(entry, getattr(candidate, "spread", None)) or 0.0
    return resolved - entry - cost


def resolve_dataset_output(path_or_dir, start_date, end_date):
    if not path_or_dir or str(path_or_dir).strip().lower() == "auto":
        return REPORTS_DIR / "research" / f"snapshots_{start_date}_{end_date}.jsonl"

    path = Path(path_or_dir)
    if path.suffix.lower() == ".jsonl":
        return path
    return path / f"snapshots_{start_date}_{end_date}.jsonl"


def build_snapshot_row(candidate, decision, context):
    model = getattr(candidate, "model", {}) or {}
    external_components = model.get("external_components") or {}
    domain_components = ((external_components.get("domain") or {}).get("components") or {})
    relation_metrics = external_components.get("relation_metrics") or {}
    relation_residual = external_components.get("relation_residual") or {}
    resolution_metadata = external_components.get("resolution_metadata") or {}
    robust = model.get("robust") or {}
    robust_components = robust.get("components") or {}
    graph_metrics = model.get("graph") or {}
    repricing = model.get("repricing") or {}
    pnl_per_share = realized_pnl_per_share(candidate)

    return {
        "dataset_version": 1,
        "snapshot_id": f"{getattr(candidate, 'token_id', 'unknown')}:{getattr(candidate, 'entry_ts', 0)}",
        "period_start_date": context["start_date"],
        "period_end_date": context["end_date"],
        "entry_timing_hours_before_close": context["entry_hours_before_close"],
        "event_id": getattr(candidate, "event_id", None),
        "event_slug": getattr(candidate, "event_slug", None),
        "market_slug": getattr(candidate, "market_slug", None),
        "token_id": getattr(candidate, "token_id", None),
        "question": getattr(candidate, "question", None),
        "market_type": getattr(candidate, "market_type", None),
        "category_group": getattr(candidate, "category_group", None),
        "entry_ts": getattr(candidate, "entry_ts", None),
        "entry_utc": _to_utc_str(getattr(candidate, "entry_ts", 0)) if getattr(candidate, "entry_ts", None) else None,
        "settle_ts": getattr(candidate, "settle_ts", None),
        "settle_utc": _to_utc_str(getattr(candidate, "settle_ts", 0)) if getattr(candidate, "settle_ts", None) else None,
        "entry_price": getattr(candidate, "entry", None),
        "market_implied": getattr(candidate, "entry", None),
        "spread": getattr(candidate, "spread", None),
        "fair": getattr(candidate, "fair", None),
        "fair_lcb": getattr(candidate, "fair_lcb", None),
        "gross_edge": getattr(candidate, "gross_edge", None),
        "net_edge": getattr(candidate, "net_edge", None),
        "gross_edge_lcb": getattr(candidate, "gross_edge_lcb", None),
        "net_edge_lcb": getattr(candidate, "net_edge_lcb", None),
        "confidence": getattr(candidate, "confidence", None),
        "meta_confidence": getattr(candidate, "meta_confidence", None),
        "meta_trade_prob": getattr(candidate, "meta_trade_prob", None),
        "meta_trade_score": getattr(candidate, "meta_trade_score", None),
        "graph_consistency": getattr(candidate, "graph_consistency", None),
        "robustness_score": getattr(candidate, "robustness_score", None),
        "resolved_outcome": getattr(candidate, "resolved_outcome", None),
        "realized_pnl_per_share": pnl_per_share,
        "realized_positive_after_costs": bool(pnl_per_share is not None and pnl_per_share > 0.0),
        "expected_positive_after_costs": bool((_safe_float(getattr(candidate, "net_edge", None), 0.0) or 0.0) > 0.0),
        "decision_status": decision.get("status"),
        "reject_reason": decision.get("reject_reason"),
        "selected_for_trade": bool(decision.get("selected_for_trade")),
        "trade_bucket": decision.get("trade_bucket"),
        "domain_name": model.get("domain_name"),
        "domain_signal": model.get("domain_signal"),
        "domain_confidence": model.get("domain_confidence"),
        "domain_action_family": domain_components.get("action_family"),
        "catalyst_type": domain_components.get("catalyst_type"),
        "meeting_subtype": repricing.get("meeting_subtype"),
        "catalyst_strength": domain_components.get("catalyst_strength"),
        "catalyst_hardness": domain_components.get("catalyst_hardness"),
        "catalyst_reversibility": domain_components.get("catalyst_reversibility"),
        "catalyst_has_official_source": domain_components.get("catalyst_has_official_source"),
        "repricing_potential": domain_components.get("repricing_potential"),
        "repricing_score": repricing.get("score"),
        "repricing_watch_score": repricing.get("watch_score"),
        "repricing_verdict": repricing.get("verdict"),
        "repricing_reason": repricing.get("reason"),
        "repricing_lane_key": repricing.get("lane_key"),
        "repricing_lane_label": repricing.get("lane_label"),
        "repricing_lane_prior": repricing.get("lane_prior"),
        "repricing_size_multiplier": getattr(candidate, "repricing_size_multiplier", None),
        "repricing_attention_gap": repricing.get("attention_gap"),
        "repricing_stale_score": repricing.get("stale_score"),
        "repricing_already_priced_penalty": repricing.get("already_priced_penalty"),
        "repricing_underreaction_score": repricing.get("underreaction_score"),
        "repricing_fresh_catalyst_score": repricing.get("fresh_catalyst_score"),
        "repricing_trend_chase_penalty": repricing.get("trend_chase_penalty"),
        "repricing_optionality_score": repricing.get("optionality_score"),
        "repricing_conflict_setup_score": repricing.get("conflict_setup_score"),
        "repricing_conflict_urgency_score": repricing.get("conflict_urgency_score"),
        "repricing_release_subject_score": repricing.get("release_subject_score"),
        "repricing_release_legitimacy_score": repricing.get("release_legitimacy_score"),
        "repricing_recent_runup": repricing.get("recent_runup"),
        "repricing_recent_selloff": repricing.get("recent_selloff"),
        "repricing_compression_score": repricing.get("compression_score"),
        "repricing_deadline_pressure": repricing.get("deadline_pressure"),
        "repricing_book_quality": repricing.get("book_quality"),
        "repricing_volume_support": repricing.get("volume_support"),
        "semantic_family": resolution_metadata.get("family"),
        "semantic_confidence": resolution_metadata.get("confidence"),
        "relation_degree": relation_metrics.get("relation_degree"),
        "relation_confidence": relation_metrics.get("relation_confidence"),
        "relation_support_price": relation_residual.get("support_price"),
        "relation_support_confidence": relation_residual.get("support_confidence"),
        "relation_residual": relation_residual.get("residual"),
        "relation_inconsistency": relation_residual.get("inconsistency_score"),
        "cost_per_share": cost_per_share(getattr(candidate, "entry", None), getattr(candidate, "spread", None)),
        "features": {
            "quality": model.get("quality"),
            "momentum": model.get("momentum"),
            "anomaly": model.get("anomaly"),
            "volume_anomaly": model.get("volume_anomaly"),
            "volume_confirmation": model.get("volume_confirmation"),
            "volume_pressure": model.get("volume_pressure"),
            "orderbook": model.get("orderbook"),
            "news": model.get("news"),
            "external": model.get("external"),
            "external_confidence": model.get("external_confidence"),
            "adjustment_multiplier": model.get("adjustment_multiplier"),
            "factor_weights": model.get("factor_weights"),
        },
        "policy": dict(getattr(candidate, "policy", {}) or {}),
        "graph_metrics": graph_metrics,
        "external_components": external_components,
        "robust_components": robust_components,
        "repricing_components": repricing,
    }


def write_jsonl(rows, output_path):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")
