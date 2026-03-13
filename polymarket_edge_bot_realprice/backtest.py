import argparse
import heapq
import json
import os
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from config import (
    ESTIMATED_SLIPPAGE_BPS,
    EVENT_NEUTRALIZATION_STRENGTH,
    KELLY_FRACTION,
    MAX_BET_USD,
    MAX_SIGNALS_PER_EVENT,
    MAX_TOTAL_EXPOSURE_PCT,
    META_MODEL_ARTIFACT_PATH,
    MIN_META_TRADE_PROB,
    MODEL_ADJUSTMENT_SCALE,
    REQUEST_BACKOFF_SECONDS,
    REQUEST_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    TAKER_FEE_BPS,
    USE_META_MODEL_SELECTOR,
)
from diagnostics import distribution_stats
from entity_normalization import extract_market_entities
from event_graph import compute_event_graph_metrics
from filter_policy import (
    filter_reason,
    scoring_policy_for_market_type,
    signal_bucket,
)
from graph_residuals import annotate_relation_residuals
from http_client import fetch_json
from market_profile import enrich_market_profile
from probability_model import estimated_probability, kelly_bet_fraction, net_edge_after_costs
from research_dataset import build_snapshot_row, resolve_dataset_output, write_jsonl
from meta_model import build_meta_feature_row, load_meta_model, score_meta_row
from repricing_selector import score_repricing_signal
from relations import annotate_market_relations
from resolution_parser import parse_resolution_semantics
from robust_signal import compute_robust_signal
from strategy import evaluate_market
from utils import clamp as _clamp
from utils import safe_float as _safe_float

GAMMA_EVENTS_API = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY_API = "https://clob.polymarket.com/prices-history"
_META_MODEL_CACHE = {}


def _get_meta_model_artifact():
    path = (META_MODEL_ARTIFACT_PATH or "").strip()
    if not path:
        return None
    cached = _META_MODEL_CACHE.get(path)
    if cached is None:
        cached = load_meta_model(path)
        _META_MODEL_CACHE[path] = cached
    return cached


def _parse_json_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            data = json.loads(value)
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _to_unix(iso_str: str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        return None


def _to_utc_str(ts: int):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _request_json(url: str):
    return fetch_json(
        url,
        timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        retries=REQUEST_RETRIES,
        backoff_seconds=REQUEST_BACKOFF_SECONDS,
        user_agent="Mozilla/5.0 (compatible; edge-bot-backtest/1.0)",
    )


def _closed_event_exists(offset: int):
    params = {"closed": "true", "limit": 1, "offset": offset}
    url = f"{GAMMA_EVENTS_API}?{urllib.parse.urlencode(params)}"
    data = _request_json(url)
    return bool(data)


def find_total_closed_events():
    low = 0
    high = 1
    while _closed_event_exists(high):
        high *= 2
        if high > 2_000_000:
            break

    while low < high:
        mid = (low + high + 1) // 2
        if _closed_event_exists(mid):
            low = mid
        else:
            high = mid - 1
    return low + 1


def fetch_closed_events(start_offset: int, max_events: int, page_size: int = 500):
    events = []
    offset = start_offset
    while len(events) < max_events:
        params = {"closed": "true", "limit": page_size, "offset": offset}
        url = f"{GAMMA_EVENTS_API}?{urllib.parse.urlencode(params)}"
        batch = _request_json(url)
        if not isinstance(batch, list) or not batch:
            break
        events.extend(batch)
        offset += page_size
    return events[:max_events]


def fetch_price_history(token_id: str, start_ts: int, end_ts: int, fidelity: int):
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity,
    }
    url = f"{CLOB_HISTORY_API}?{urllib.parse.urlencode(params)}"
    payload = _request_json(url)
    history = payload.get("history", []) if isinstance(payload, dict) else []
    parsed = []
    for point in history:
        if not isinstance(point, dict):
            continue
        ts = int(point.get("t", 0))
        price = _safe_float(point.get("p"))
        if ts > 0 and price is not None and 0 <= price <= 1:
            parsed.append((ts, price))
    parsed.sort(key=lambda x: x[0])
    return parsed


def price_at_or_before(history, ts):
    candidate = None
    for point_ts, point_price in history:
        if point_ts <= ts:
            candidate = point_price
        else:
            break
    return candidate


def change_over(history, ts, horizon_seconds):
    now_price = price_at_or_before(history, ts)
    prev_price = price_at_or_before(history, ts - horizon_seconds)
    if now_price is None or prev_price is None:
        return 0.0
    return now_price - prev_price


def _select_outcome_index(outcomes):
    for i, outcome in enumerate(outcomes):
        if isinstance(outcome, str) and outcome.strip().lower() in {"yes", "true"}:
            return i
    return 0


def _resolved_binary_outcome(price):
    if price is None:
        return None
    if price >= 0.99:
        return 1
    if price <= 0.01:
        return 0
    return None


def _candidate_event_key(candidate):
    return candidate.event_id or candidate.event_slug or candidate.market_slug


def _candidate_key(candidate):
    return (
        candidate.event_id,
        candidate.token_id,
        candidate.entry_ts,
    )


def _recompute_candidate_edges(candidate):
    candidate.gross_edge = candidate.fair - candidate.entry
    candidate.net_edge = net_edge_after_costs(
        fair_probability=candidate.fair,
        entry_price=candidate.entry,
        taker_fee_bps=TAKER_FEE_BPS,
        slippage_bps=ESTIMATED_SLIPPAGE_BPS,
        spread=candidate.spread,
    )


def _neutralize_candidates_by_event(candidates):
    grouped = {}
    for c in candidates:
        grouped.setdefault(_candidate_event_key(c), []).append(c)

    for group in grouped.values():
        if len(group) <= 1:
            continue
        deltas = [c.fair - c.entry for c in group]
        mean_delta = sum(deltas) / len(deltas)
        for c in group:
            centered = (c.fair - c.entry) - mean_delta
            c.fair = _clamp(c.entry + (centered * EVENT_NEUTRALIZATION_STRENGTH))
            _recompute_candidate_edges(c)


def _dedupe_per_event(candidates):
    grouped = {}
    for c in candidates:
        grouped.setdefault(_candidate_event_key(c), []).append(c)

    selected = []
    prepared_snapshots = []
    for group in grouped.values():
        group = sorted(group, key=lambda x: x.net_edge, reverse=True)
        selected.extend(group[:MAX_SIGNALS_PER_EVENT])
    return selected


def _passes_backtest_filters(market, rejects, use_liquidity_filter):
    reason = filter_reason(
        market,
        entry_price=market.get("ref_price"),
        use_liquidity_filter=use_liquidity_filter,
    )
    if reason:
        rejects[reason] += 1
        return False

    return True


@dataclass
class Candidate:
    event_id: str
    question: str
    event_slug: str
    market_slug: str
    market_type: str
    category_group: str
    token_id: str
    entry_ts: int
    settle_ts: int
    entry: float
    fair: float
    fair_lcb: float
    gross_edge: float
    net_edge: float
    gross_edge_lcb: float
    net_edge_lcb: float
    confidence: float
    meta_confidence: float
    meta_trade_prob: Optional[float]
    meta_trade_score: Optional[float]
    graph_consistency: float
    robustness_score: float
    resolved_outcome: int
    spread: Optional[float]
    liquidity: float
    volume24h: float
    one_hour_change: float
    one_day_change: float
    one_week_change: float
    hours_to_close: float
    policy: dict
    model: dict


@dataclass
class OpenPosition:
    question: str
    settle_ts: int
    shares: float
    cost_basis: float
    resolved_outcome: int
    entry_price: float


def _bump_stage(diag, stage_name, market_type=None, category_group=None):
    diag["stage_counts"][stage_name] += 1
    if market_type:
        diag["market_type_stage_counts"][stage_name][market_type] += 1
    if category_group:
        diag["category_stage_counts"][stage_name][category_group] += 1


def _bump_reject(diag, reason, market_type=None, category_group=None):
    if market_type:
        diag["rejects_by_market_type"][reason][market_type] += 1
    if category_group:
        diag["rejects_by_category"][reason][category_group] += 1


def _finalize_stage_map(stage_map):
    return {
        stage_name: dict(sorted(counts.items()))
        for stage_name, counts in stage_map.items()
    }


def _build_rejection_payload(candidate, reason):
    robust = candidate.model.get("robust") or {}
    robust_components = robust.get("components") or {}
    return {
        "question": candidate.question,
        "event_slug": candidate.event_slug,
        "market_slug": candidate.market_slug,
        "market_type": candidate.market_type,
        "category_group": candidate.category_group,
        "entry_utc": _to_utc_str(candidate.entry_ts),
        "entry": candidate.entry,
        "fair": candidate.fair,
        "fair_lcb": candidate.fair_lcb,
        "gross_edge": candidate.gross_edge,
        "net_edge": candidate.net_edge,
        "gross_edge_lcb": candidate.gross_edge_lcb,
        "net_edge_lcb": candidate.net_edge_lcb,
        "confidence": candidate.confidence,
        "meta_confidence": candidate.meta_confidence,
        "meta_trade_prob": candidate.meta_trade_prob,
        "meta_trade_score": candidate.meta_trade_score,
        "graph_consistency": candidate.graph_consistency,
        "robustness_score": candidate.robustness_score,
        "domain_name": candidate.model.get("domain_name"),
        "domain_signal": candidate.model.get("domain_signal"),
        "domain_confidence": candidate.model.get("domain_confidence"),
        "relation_metrics": (
            (candidate.model.get("external_components") or {}).get("relation_metrics") or {}
        ),
        "relation_residual": (
            (candidate.model.get("external_components") or {}).get("relation_residual") or {}
        ),
        "resolution_metadata": (
            (candidate.model.get("external_components") or {}).get("resolution_metadata") or {}
        ),
        "robust_components": robust_components,
        "reject_reason": reason,
        "policy": dict(candidate.policy),
        "link": f"https://polymarket.com/event/{candidate.event_slug}?tid={candidate.token_id}",
    }


def _top_edge_rejections(rejections, limit=8):
    ranked = sorted(
        rejections,
        key=lambda item: (
            item.get("net_edge_lcb"),
            item["net_edge"] if item["net_edge"] is not None else item["gross_edge"],
            item["gross_edge"],
        ),
        reverse=True,
    )
    return ranked[:limit]


def _mean(values):
    return (sum(values) / len(values)) if values else 0.0


def _summarize_low_lcb_edge(rejections):
    rejected = [item for item in rejections if item.get("reject_reason") == "low_lcb_edge"]
    if not rejected:
        return {}

    by_market_type = defaultdict(int)
    by_category = defaultdict(int)
    by_domain = defaultdict(int)
    by_semantic_family = defaultdict(int)
    question_samples = []

    uncertainty_values = []
    correlation_penalties = []
    graph_penalties = []
    regime_penalties = []
    total_penalties = []
    raw_adjustments = []
    relation_inconsistency = []
    relation_constraint_violation = []
    relation_support_confidence = []
    cost_gaps = []
    lcb_shortfalls = []
    net_edges = []
    gross_edges = []
    confidences = []
    meta_confidences = []

    for item in rejected:
        by_market_type[item.get("market_type") or "unknown"] += 1
        by_category[item.get("category_group") or "other"] += 1
        by_domain[item.get("domain_name") or "unknown"] += 1
        semantic_family = (item.get("resolution_metadata") or {}).get("family") or "unknown"
        by_semantic_family[semantic_family] += 1

        robust = item.get("robust_components") or {}
        rel = item.get("relation_residual") or {}
        uncertainty_values.append(
            float(robust.get("uncertainty") or robust.get("components", {}).get("uncertainty") or 0.0)
        )
        correlation_penalties.append(float(robust.get("correlation_penalty") or 0.0))
        graph_penalties.append(float(robust.get("graph_penalty") or 0.0))
        regime_penalties.append(float(robust.get("regime_penalty") or 0.0))
        total_penalties.append(float(robust.get("total_penalty") or 0.0))
        raw_adjustments.append(float(robust.get("raw_adjustment") or 0.0))
        relation_inconsistency.append(float(rel.get("inconsistency_score") or 0.0))
        relation_constraint_violation.append(float(rel.get("constraint_violation") or 0.0))
        relation_support_confidence.append(float(rel.get("support_confidence") or 0.0))
        net_edges.append(float(item.get("net_edge") or 0.0))
        gross_edges.append(float(item.get("gross_edge") or 0.0))
        confidences.append(float(item.get("confidence") or 0.0))
        meta_confidences.append(float(item.get("meta_confidence") or 0.0))
        cost_gaps.append(float(item.get("gross_edge") or 0.0) - float(item.get("net_edge") or 0.0))
        lcb_shortfalls.append((float(item.get("net_edge") or 0.0) - float(item.get("net_edge_lcb") or 0.0)))

        if len(question_samples) < 5:
            question_samples.append(
                {
                    "question": item.get("question"),
                    "market_type": item.get("market_type"),
                    "domain_name": item.get("domain_name"),
                    "net_edge": item.get("net_edge"),
                    "net_edge_lcb": item.get("net_edge_lcb"),
                }
            )

    count = len(rejected)
    return {
        "count": count,
        "by_market_type": dict(sorted(by_market_type.items())),
        "by_category_group": dict(sorted(by_category.items())),
        "by_domain_name": dict(sorted(by_domain.items())),
        "by_semantic_family": dict(sorted(by_semantic_family.items())),
        "mean_net_edge": _mean(net_edges),
        "mean_gross_edge": _mean(gross_edges),
        "mean_confidence": _mean(confidences),
        "mean_meta_confidence": _mean(meta_confidences),
        "mean_raw_adjustment": _mean(raw_adjustments),
        "mean_cost_gap": _mean(cost_gaps),
        "mean_lcb_shortfall": _mean(lcb_shortfalls),
        "mean_uncertainty": _mean(uncertainty_values),
        "mean_correlation_penalty": _mean(correlation_penalties),
        "mean_graph_penalty": _mean(graph_penalties),
        "mean_regime_penalty": _mean(regime_penalties),
        "mean_total_penalty": _mean(total_penalties),
        "mean_relation_inconsistency": _mean(relation_inconsistency),
        "mean_relation_constraint_violation": _mean(relation_constraint_violation),
        "mean_relation_support_confidence": _mean(relation_support_confidence),
        "sample_questions": question_samples,
    }


def _annotate_candidates_with_graph_and_robust_signal(candidates):
    nodes = []
    for candidate in candidates:
        nodes.append(
            {
                "event_key": candidate.event_id or candidate.event_slug or candidate.market_slug,
                "implied": candidate.entry,
                "fair": candidate.fair,
                "market_type": candidate.market_type,
                "event_market_count": None,
            }
        )

    graph_metrics = compute_event_graph_metrics(nodes)
    for candidate, graph in zip(candidates, graph_metrics):
        graph = graph or {}
        candidate.graph_consistency = graph.get("consistency", 0.0)
        candidate.policy = scoring_policy_for_market_type(candidate.market_type)
        robust = compute_robust_signal(
            market={
                "ref_price": candidate.entry,
                "best_ask": candidate.entry,
                "spread": candidate.spread,
                "event_market_count": graph.get("event_size"),
            },
            metrics=candidate.model,
            fair=candidate.fair,
            graph_metrics=graph,
        )
        candidate.meta_confidence = robust["meta_confidence"]
        candidate.fair_lcb = robust["fair_lcb"]
        candidate.gross_edge_lcb = robust["gross_edge_lcb"]
        candidate.net_edge_lcb = robust["net_edge_lcb"]
        candidate.robustness_score = robust["robustness_score"]
        candidate.model["graph"] = graph
        candidate.model["robust"] = robust


def _annotate_candidates_with_meta_model(candidates):
    artifact = _get_meta_model_artifact()
    if not artifact:
        return

    for candidate in candidates:
        prediction = score_meta_row(build_meta_feature_row(candidate), artifact)
        candidate.meta_trade_prob = prediction["probability"]
        candidate.meta_trade_score = prediction["trade_score"]
        candidate.model["meta_model"] = prediction


def _annotate_candidates_with_repricing_selector(candidates):
    for candidate in candidates:
        prediction = score_repricing_signal(
            entry_price=candidate.entry,
            confidence=candidate.confidence,
            net_edge=candidate.net_edge,
            net_edge_lcb=candidate.net_edge_lcb,
            spread=candidate.spread,
            liquidity=candidate.liquidity,
            volume24h=candidate.volume24h,
            one_hour_change=candidate.one_hour_change,
            one_day_change=candidate.one_day_change,
            one_week_change=candidate.one_week_change,
            hours_to_close=candidate.hours_to_close,
            volume_anomaly=candidate.model.get("volume_anomaly"),
            volume_confirmation=candidate.model.get("volume_confirmation"),
            model=candidate.model,
            market_type=candidate.market_type,
            category_group=candidate.category_group,
            question=candidate.question,
        )
        candidate.meeting_subtype = prediction.get("meeting_subtype")
        candidate.model["repricing"] = prediction


_STAGE_NAMES = (
    "markets_seen",
    "markets_in_time_window",
    "binary_markets",
    "resolved_binary_markets",
    "history_requests",
    "history_available",
    "snapshot_ready",
    "passed_filters",
    "scored",
    "post_neutralization",
    "after_confidence",
    "after_gross_edge",
    "after_meta_confidence",
    "after_meta_model",
    "after_graph_consistency",
    "after_robustness",
    "after_lcb_edge",
    "after_net_edge",
    "final_candidates",
)


def build_candidates(
    events,
    start_ts: int,
    end_ts: int,
    entry_hours_before_close: int,
    history_window_days: int,
    max_markets: int,
    fidelity: int,
    use_liquidity_filter: bool,
    max_history_requests: int,
    skip_base_filters: bool = False,
    skip_score_filters: bool = False,
):
    rejects = {
        "low_liquidity": 0,
        "low_volume": 0,
        "excluded_intraday_crypto": 0,
        "excluded_pattern": 0,
        "no_price": 0,
        "no_orderbook": 0,
        "extreme_price": 0,
        "wide_spread": 0,
        "near_expiry": 0,
        "low_confidence": 0,
        "low_gross_edge": 0,
        "low_net_edge": 0,
        "low_meta_confidence": 0,
        "low_meta_model_prob": 0,
        "low_graph_consistency": 0,
        "low_robustness": 0,
        "low_lcb_edge": 0,
    }
    reasons = {
        "not_binary": 0,
        "not_resolved": 0,
        "missing_history": 0,
        "history_request_error": 0,
        "no_entry_price": 0,
    }
    diagnostics = {
        "stage_counts": defaultdict(int, {name: 0 for name in _STAGE_NAMES}),
        "market_type_stage_counts": defaultdict(lambda: defaultdict(int)),
        "category_stage_counts": defaultdict(lambda: defaultdict(int)),
        "rejects_by_market_type": defaultdict(lambda: defaultdict(int)),
        "rejects_by_category": defaultdict(lambda: defaultdict(int)),
    }

    selected = []
    prepared_snapshots = []
    decision_map = {}
    edge_rejections = []
    history_requests = 0
    stop_scan = False
    for event in events:
        event_slug = event.get("slug") or ""
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue

        for m in markets:
            diagnostics["stage_counts"]["markets_seen"] += 1
            settle_ts = _to_unix(m.get("endDate") or event.get("endDate"))
            if settle_ts is None:
                continue
            if settle_ts < start_ts or settle_ts > end_ts:
                continue
            diagnostics["stage_counts"]["markets_in_time_window"] += 1

            outcomes = _parse_json_list(m.get("outcomes"))
            outcome_prices = _parse_json_list(m.get("outcomePrices"))
            token_ids = _parse_json_list(m.get("clobTokenIds"))
            if len(outcomes) != 2 or len(outcome_prices) != 2 or len(token_ids) != 2:
                reasons["not_binary"] += 1
                continue
            diagnostics["stage_counts"]["binary_markets"] += 1

            outcome_idx = _select_outcome_index(outcomes)
            if outcome_idx >= len(token_ids) or outcome_idx >= len(outcome_prices):
                reasons["not_binary"] += 1
                continue

            final_price = _safe_float(outcome_prices[outcome_idx])
            resolved_outcome = _resolved_binary_outcome(final_price)
            if resolved_outcome is None:
                reasons["not_resolved"] += 1
                continue
            diagnostics["stage_counts"]["resolved_binary_markets"] += 1

            token_id = str(token_ids[outcome_idx])
            entry_ts = settle_ts - (entry_hours_before_close * 3600)
            if entry_ts <= 0:
                continue

            if history_requests >= max_history_requests:
                stop_scan = True
                break

            hist_start_ts = entry_ts - (history_window_days * 24 * 3600)
            history_requests += 1
            diagnostics["stage_counts"]["history_requests"] = history_requests
            try:
                history = fetch_price_history(token_id, hist_start_ts, entry_ts, fidelity=fidelity)
            except RuntimeError:
                reasons["history_request_error"] += 1
                continue
            if not history:
                reasons["missing_history"] += 1
                continue
            diagnostics["stage_counts"]["history_available"] += 1

            entry_price = price_at_or_before(history, entry_ts)
            if entry_price is None:
                reasons["no_entry_price"] += 1
                continue

            best_bid = _safe_float(m.get("bestBid"))
            best_ask = _safe_float(m.get("bestAsk"))
            spread = None
            if (
                best_bid is not None
                and best_ask is not None
                and 0 <= best_bid <= 1
                and 0 <= best_ask <= 1
                and best_ask >= best_bid
            ):
                spread = best_ask - best_bid
            else:
                best_bid = None
                best_ask = None
                spread = _safe_float(m.get("spread"))
                if spread is not None and (spread < 0 or spread > 1):
                    spread = None

            volume_total = _safe_float(m.get("volume")) or _safe_float(event.get("volume")) or 0.0
            volume_1wk = _safe_float(m.get("volume1wk")) or 0.0
            volume24h_proxy = (volume_1wk / 7.0) if volume_1wk > 0 else (volume_total / 30.0)

            snapshot = {
                "id": m.get("id") or token_id,
                "event_id": str(event.get("id") or ""),
                "question": m.get("question") or "",
                "slug": m.get("slug") or "",
                "event_slug": event_slug,
                "event_title": event.get("title") or event.get("question") or "",
                "event_description": event.get("description") or m.get("description") or "",
                "event_category": event.get("category") or m.get("category") or "",
                "resolution_source": event.get("resolutionSource") or m.get("resolutionSource") or "",
                "event_market_count": len(markets),
                "outcome_count": len(outcomes),
                "outcomes": outcomes,
                "selected_outcome": outcomes[outcome_idx] if outcome_idx < len(outcomes) else "",
                "selected_outcome_index": outcome_idx,
                "token_yes": token_id,
                "volume": volume_total,
                "volume24h": volume24h_proxy,
                "liquidity": _safe_float(m.get("liquidity")) or 0.0,
                "ref_price": entry_price,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "last_trade": entry_price,
                "one_hour_change": change_over(history, entry_ts, 3600),
                "one_day_change": change_over(history, entry_ts, 24 * 3600),
                "one_week_change": change_over(history, entry_ts, 7 * 24 * 3600),
                "hours_to_close": float(entry_hours_before_close),
            }

            if spread is not None and best_bid is None and best_ask is None:
                snapshot["best_bid"] = max(0.0, entry_price - spread / 2.0)
                snapshot["best_ask"] = min(1.0, entry_price + spread / 2.0)

            profile = enrich_market_profile(snapshot)
            snapshot["resolution_metadata"] = parse_resolution_semantics(snapshot)
            snapshot["entity_metadata"] = extract_market_entities(snapshot)
            snapshot["primary_entity_key"] = (
                snapshot["resolution_metadata"].get("subject_entity_key")
                or next(iter(snapshot["entity_metadata"].get("entity_keys") or []), None)
            )
            _bump_stage(
                diagnostics,
                "snapshot_ready",
                market_type=profile["market_type"],
                category_group=profile["category_group"],
            )
            if not skip_base_filters:
                reject_snapshot = dict(rejects)
                if not _passes_backtest_filters(snapshot, rejects, use_liquidity_filter):
                    for reason_name, count in rejects.items():
                        if count != reject_snapshot.get(reason_name, 0):
                            diagnostics["rejects_by_market_type"][reason_name][profile["market_type"]] += 1
                            diagnostics["rejects_by_category"][reason_name][profile["category_group"]] += 1
                            break
                    continue
            _bump_stage(
                diagnostics,
                "passed_filters",
                market_type=profile["market_type"],
                category_group=profile["category_group"],
            )
            prepared_snapshots.append(
                {
                    "snapshot": snapshot,
                    "event_id": str(event.get("id") or ""),
                    "event_slug": event_slug,
                    "token_id": token_id,
                    "entry_ts": entry_ts,
                    "settle_ts": settle_ts,
                    "entry_price": entry_price,
                    "resolved_outcome": resolved_outcome,
                    "spread": spread,
                }
            )
            if len(prepared_snapshots) >= max_markets:
                break
        if stop_scan:
            break
        if len(prepared_snapshots) >= max_markets:
            break

    relation_graph = annotate_market_relations([item["snapshot"] for item in prepared_snapshots])
    annotate_relation_residuals([item["snapshot"] for item in prepared_snapshots], relation_graph=relation_graph)

    for item in prepared_snapshots:
        snapshot = item["snapshot"]
        metrics = evaluate_market(snapshot)
        fair = estimated_probability(
            snapshot,
            metrics,
            adjustment_scale=MODEL_ADJUSTMENT_SCALE,
        )
        if fair is None:
            continue
        _bump_stage(
            diagnostics,
            "scored",
            market_type=metrics.get("market_type"),
            category_group=metrics.get("category_group"),
        )

        selected.append(
            Candidate(
                event_id=item["event_id"],
                question=snapshot["question"],
                event_slug=item["event_slug"],
                market_slug=snapshot["slug"],
                market_type=metrics.get("market_type", "unknown"),
                category_group=metrics.get("category_group", "other"),
                token_id=item["token_id"],
                entry_ts=item["entry_ts"],
                settle_ts=item["settle_ts"],
                entry=item["entry_price"],
                fair=fair,
                fair_lcb=0.0,
                gross_edge=0.0,
                net_edge=0.0,
                gross_edge_lcb=0.0,
                net_edge_lcb=0.0,
                confidence=metrics.get("confidence", 0.5),
                meta_confidence=0.0,
                meta_trade_prob=None,
                meta_trade_score=None,
                graph_consistency=0.0,
                robustness_score=0.0,
                resolved_outcome=item["resolved_outcome"],
                spread=item["spread"],
                liquidity=snapshot.get("liquidity") or 0.0,
                volume24h=snapshot.get("volume24h") or 0.0,
                one_hour_change=snapshot.get("one_hour_change") or 0.0,
                one_day_change=snapshot.get("one_day_change") or 0.0,
                one_week_change=snapshot.get("one_week_change") or 0.0,
                hours_to_close=snapshot.get("hours_to_close") or 0.0,
                policy={},
                model=dict(metrics),
            )
        )

    for candidate in selected:
        _recompute_candidate_edges(candidate)

    _neutralize_candidates_by_event(selected)
    _annotate_candidates_with_graph_and_robust_signal(selected)
    _annotate_candidates_with_meta_model(selected)
    _annotate_candidates_with_repricing_selector(selected)

    for candidate in selected:
        decision_map[_candidate_key(candidate)] = {
            "status": "scored",
            "reject_reason": None,
            "selected_for_trade": False,
            "trade_bucket": None,
        }

    for candidate in selected:
        _bump_stage(
            diagnostics,
            "post_neutralization",
            market_type=candidate.market_type,
            category_group=candidate.category_group,
        )

    filtered = []
    if skip_score_filters:
        for candidate in selected:
            score_policy = scoring_policy_for_market_type(candidate.market_type)
            candidate.policy = dict(score_policy)
            decision_map[_candidate_key(candidate)]["status"] = "research_candidate"
            decision_map[_candidate_key(candidate)]["selected_for_trade"] = False
            decision_map[_candidate_key(candidate)]["trade_bucket"] = "research"
    else:
        for candidate in selected:
            score_policy = scoring_policy_for_market_type(candidate.market_type)
            candidate.policy = dict(score_policy)
            if candidate.confidence < score_policy["min_confidence"]:
                rejects["low_confidence"] += 1
                _bump_reject(diagnostics, "low_confidence", candidate.market_type, candidate.category_group)
                decision_map[_candidate_key(candidate)]["status"] = "rejected"
                decision_map[_candidate_key(candidate)]["reject_reason"] = "low_confidence"
                continue
            _bump_stage(
                diagnostics,
                "after_confidence",
                market_type=candidate.market_type,
                category_group=candidate.category_group,
            )

            if candidate.gross_edge < score_policy["min_gross_edge"]:
                rejects["low_gross_edge"] += 1
                _bump_reject(diagnostics, "low_gross_edge", candidate.market_type, candidate.category_group)
                edge_rejections.append(_build_rejection_payload(candidate, "low_gross_edge"))
                decision_map[_candidate_key(candidate)]["status"] = "rejected"
                decision_map[_candidate_key(candidate)]["reject_reason"] = "low_gross_edge"
                continue
            _bump_stage(
                diagnostics,
                "after_gross_edge",
                market_type=candidate.market_type,
                category_group=candidate.category_group,
            )

            if candidate.meta_confidence < score_policy["min_meta_confidence"]:
                rejects["low_meta_confidence"] += 1
                _bump_reject(diagnostics, "low_meta_confidence", candidate.market_type, candidate.category_group)
                edge_rejections.append(_build_rejection_payload(candidate, "low_meta_confidence"))
                decision_map[_candidate_key(candidate)]["status"] = "rejected"
                decision_map[_candidate_key(candidate)]["reject_reason"] = "low_meta_confidence"
                continue
            _bump_stage(
                diagnostics,
                "after_meta_confidence",
                market_type=candidate.market_type,
                category_group=candidate.category_group,
            )

            if USE_META_MODEL_SELECTOR and candidate.meta_trade_prob is not None:
                if candidate.meta_trade_prob < MIN_META_TRADE_PROB:
                    rejects["low_meta_model_prob"] += 1
                    _bump_reject(diagnostics, "low_meta_model_prob", candidate.market_type, candidate.category_group)
                    edge_rejections.append(_build_rejection_payload(candidate, "low_meta_model_prob"))
                    decision_map[_candidate_key(candidate)]["status"] = "rejected"
                    decision_map[_candidate_key(candidate)]["reject_reason"] = "low_meta_model_prob"
                    continue
                _bump_stage(
                    diagnostics,
                    "after_meta_model",
                    market_type=candidate.market_type,
                    category_group=candidate.category_group,
                )

            if candidate.graph_consistency < score_policy["min_graph_consistency"]:
                rejects["low_graph_consistency"] += 1
                _bump_reject(diagnostics, "low_graph_consistency", candidate.market_type, candidate.category_group)
                edge_rejections.append(_build_rejection_payload(candidate, "low_graph_consistency"))
                decision_map[_candidate_key(candidate)]["status"] = "rejected"
                decision_map[_candidate_key(candidate)]["reject_reason"] = "low_graph_consistency"
                continue
            _bump_stage(
                diagnostics,
                "after_graph_consistency",
                market_type=candidate.market_type,
                category_group=candidate.category_group,
            )

            if candidate.robustness_score < score_policy["min_robustness_score"]:
                rejects["low_robustness"] += 1
                _bump_reject(diagnostics, "low_robustness", candidate.market_type, candidate.category_group)
                edge_rejections.append(_build_rejection_payload(candidate, "low_robustness"))
                decision_map[_candidate_key(candidate)]["status"] = "rejected"
                decision_map[_candidate_key(candidate)]["reject_reason"] = "low_robustness"
                continue
            _bump_stage(
                diagnostics,
                "after_robustness",
                market_type=candidate.market_type,
                category_group=candidate.category_group,
            )

            if candidate.net_edge_lcb is None or candidate.net_edge_lcb <= score_policy["min_lcb_edge"]:
                rejects["low_lcb_edge"] += 1
                _bump_reject(diagnostics, "low_lcb_edge", candidate.market_type, candidate.category_group)
                edge_rejections.append(_build_rejection_payload(candidate, "low_lcb_edge"))
                decision_map[_candidate_key(candidate)]["status"] = "rejected"
                decision_map[_candidate_key(candidate)]["reject_reason"] = "low_lcb_edge"
                continue
            _bump_stage(
                diagnostics,
                "after_lcb_edge",
                market_type=candidate.market_type,
                category_group=candidate.category_group,
            )

            bucket = signal_bucket(candidate.net_edge, score_policy, net_edge_lcb=candidate.net_edge_lcb)
            if bucket != "value":
                rejects["low_net_edge"] += 1
                _bump_reject(diagnostics, "low_net_edge", candidate.market_type, candidate.category_group)
                edge_rejections.append(_build_rejection_payload(candidate, "low_net_edge"))
                decision_map[_candidate_key(candidate)]["status"] = "rejected"
                decision_map[_candidate_key(candidate)]["reject_reason"] = "low_net_edge"
                continue
            _bump_stage(
                diagnostics,
                "after_net_edge",
                market_type=candidate.market_type,
                category_group=candidate.category_group,
            )
            filtered.append(candidate)

    pre_dedupe = list(filtered)
    filtered = _dedupe_per_event(filtered)
    kept_keys = {_candidate_key(candidate) for candidate in filtered}
    for candidate in pre_dedupe:
        key = _candidate_key(candidate)
        if key not in kept_keys:
            decision_map[key]["status"] = "rejected"
            decision_map[key]["reject_reason"] = "event_deduped"

    filtered = sorted(
        filtered,
        key=lambda x: (
            x.meta_trade_score if x.meta_trade_score is not None else float("-inf"),
            x.net_edge_lcb,
            x.robustness_score,
            x.net_edge,
        ),
        reverse=True,
    )[:max_markets]
    final_keys = {_candidate_key(candidate) for candidate in filtered}
    for candidate in filtered:
        decision_map[_candidate_key(candidate)]["status"] = "final_candidate"
        decision_map[_candidate_key(candidate)]["selected_for_trade"] = True
        decision_map[_candidate_key(candidate)]["trade_bucket"] = "value"
        _bump_stage(
            diagnostics,
            "final_candidates",
            market_type=candidate.market_type,
            category_group=candidate.category_group,
        )

    for candidate in pre_dedupe:
        key = _candidate_key(candidate)
        if key in kept_keys and key not in final_keys:
            decision_map[key]["status"] = "rejected"
            decision_map[key]["reject_reason"] = "rank_truncated"

    diagnostics_payload = {
        "stage_counts": dict(sorted(diagnostics["stage_counts"].items())),
        "market_type_stage_counts": _finalize_stage_map(diagnostics["market_type_stage_counts"]),
        "category_stage_counts": _finalize_stage_map(diagnostics["category_stage_counts"]),
        "rejects_by_market_type": _finalize_stage_map(diagnostics["rejects_by_market_type"]),
        "rejects_by_category": _finalize_stage_map(diagnostics["rejects_by_category"]),
        "edge_distributions": {
            "confidence": distribution_stats([candidate.confidence for candidate in selected]),
            "meta_confidence": distribution_stats([candidate.meta_confidence for candidate in selected]),
            "meta_trade_prob": distribution_stats(
                [candidate.meta_trade_prob for candidate in selected if candidate.meta_trade_prob is not None]
            ),
            "meta_trade_score": distribution_stats(
                [candidate.meta_trade_score for candidate in selected if candidate.meta_trade_score is not None]
            ),
            "domain_confidence": distribution_stats(
                [candidate.model.get("domain_confidence", 0.5) for candidate in selected]
            ),
            "relation_degree": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_metrics") or {}).get(
                        "relation_degree",
                        0,
                    )
                    for candidate in selected
                ]
            ),
            "relation_confidence": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_metrics") or {}).get(
                        "relation_confidence",
                        0.0,
                    )
                    for candidate in selected
                ]
            ),
            "relation_support_confidence": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_residual") or {}).get(
                        "support_confidence",
                        0.0,
                    )
                    for candidate in selected
                ]
            ),
            "relation_residual": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_residual") or {}).get(
                        "residual",
                        0.0,
                    )
                    for candidate in selected
                ]
            ),
            "relation_inconsistency": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_residual") or {}).get(
                        "inconsistency_score",
                        0.0,
                    )
                    for candidate in selected
                ]
            ),
            "graph_consistency": distribution_stats([candidate.graph_consistency for candidate in selected]),
            "robustness_score": distribution_stats([candidate.robustness_score for candidate in selected]),
            "gross_edge": distribution_stats([candidate.gross_edge for candidate in selected]),
            "net_edge": distribution_stats([candidate.net_edge for candidate in selected]),
            "gross_edge_lcb": distribution_stats([candidate.gross_edge_lcb for candidate in selected]),
            "net_edge_lcb": distribution_stats([candidate.net_edge_lcb for candidate in selected]),
        },
        "market_type_edge_summary": {},
        "top_rejected_by_edge": _top_edge_rejections(edge_rejections),
        "low_lcb_edge_analysis": _summarize_low_lcb_edge(edge_rejections),
    }

    grouped_by_type = defaultdict(list)
    for candidate in selected:
        grouped_by_type[candidate.market_type].append(candidate)
    for market_type, group in grouped_by_type.items():
        diagnostics_payload["market_type_edge_summary"][market_type] = {
            "count": len(group),
            "confidence": distribution_stats([candidate.confidence for candidate in group]),
            "meta_confidence": distribution_stats([candidate.meta_confidence for candidate in group]),
            "meta_trade_prob": distribution_stats(
                [candidate.meta_trade_prob for candidate in group if candidate.meta_trade_prob is not None]
            ),
            "meta_trade_score": distribution_stats(
                [candidate.meta_trade_score for candidate in group if candidate.meta_trade_score is not None]
            ),
            "domain_confidence": distribution_stats(
                [candidate.model.get("domain_confidence", 0.5) for candidate in group]
            ),
            "relation_degree": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_metrics") or {}).get(
                        "relation_degree",
                        0,
                    )
                    for candidate in group
                ]
            ),
            "relation_confidence": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_metrics") or {}).get(
                        "relation_confidence",
                        0.0,
                    )
                    for candidate in group
                ]
            ),
            "relation_support_confidence": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_residual") or {}).get(
                        "support_confidence",
                        0.0,
                    )
                    for candidate in group
                ]
            ),
            "relation_residual": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_residual") or {}).get(
                        "residual",
                        0.0,
                    )
                    for candidate in group
                ]
            ),
            "relation_inconsistency": distribution_stats(
                [
                    ((candidate.model.get("external_components") or {}).get("relation_residual") or {}).get(
                        "inconsistency_score",
                        0.0,
                    )
                    for candidate in group
                ]
            ),
            "graph_consistency": distribution_stats([candidate.graph_consistency for candidate in group]),
            "robustness_score": distribution_stats([candidate.robustness_score for candidate in group]),
            "gross_edge": distribution_stats([candidate.gross_edge for candidate in group]),
            "net_edge": distribution_stats([candidate.net_edge for candidate in group]),
            "net_edge_lcb": distribution_stats([candidate.net_edge_lcb for candidate in group]),
        }

    dataset_rows = [
        build_snapshot_row(
            candidate,
            decision=decision_map.get(_candidate_key(candidate), {"status": "scored"}),
            context={
                "start_date": datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                "end_date": datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                "entry_hours_before_close": entry_hours_before_close,
            },
        )
        for candidate in selected
    ]

    return filtered, rejects, reasons, diagnostics_payload, dataset_rows


def run_simulation(candidates, initial_bankroll: float):
    candidates = sorted(candidates, key=lambda x: x.entry_ts)
    open_positions = []
    seq = 0

    cash = initial_bankroll
    equity_peak = initial_bankroll
    max_drawdown = 0.0
    total_trades = 0
    winning_trades = 0
    realized_pnl = 0.0
    skipped_no_cash = 0
    skipped_exposure = 0
    executed = []

    fee_rate = (TAKER_FEE_BPS + ESTIMATED_SLIPPAGE_BPS) / 10000.0

    def settle_up_to(ts):
        nonlocal cash, winning_trades, realized_pnl, equity_peak, max_drawdown
        while open_positions and open_positions[0][0] <= ts:
            _, _, pos = heapq.heappop(open_positions)
            payout = pos.shares * float(pos.resolved_outcome)
            pnl = payout - pos.cost_basis
            realized_pnl += pnl
            if pnl > 0:
                winning_trades += 1
            cash += payout
            if cash > equity_peak:
                equity_peak = cash
            if equity_peak > 0:
                dd = (equity_peak - cash) / equity_peak
                if dd > max_drawdown:
                    max_drawdown = dd

    for c in candidates:
        settle_up_to(c.entry_ts)

        kelly_probability = c.fair_lcb if c.fair_lcb is not None else c.fair
        kelly = kelly_bet_fraction(kelly_probability, c.entry)
        proposed_stake = cash * kelly * KELLY_FRACTION * min(c.confidence, c.robustness_score)
        proposed_stake = min(MAX_BET_USD, proposed_stake)
        if proposed_stake <= 0:
            continue

        shares = proposed_stake / c.entry
        half_spread = (c.spread / 2.0) if c.spread is not None else 0.0
        spread_cost = shares * half_spread
        fee_cost = proposed_stake * fee_rate
        total_outlay = proposed_stake + spread_cost + fee_cost

        open_cost_basis = sum(position.cost_basis for _, _, position in open_positions)
        exposure_cap = (cash + open_cost_basis) * MAX_TOTAL_EXPOSURE_PCT
        if open_cost_basis + total_outlay > exposure_cap:
            skipped_exposure += 1
            continue

        if total_outlay > cash:
            skipped_no_cash += 1
            continue

        cash -= total_outlay
        pos = OpenPosition(
            question=c.question,
            settle_ts=c.settle_ts,
            shares=shares,
            cost_basis=total_outlay,
            resolved_outcome=c.resolved_outcome,
            entry_price=c.entry,
        )
        seq += 1
        heapq.heappush(open_positions, (c.settle_ts, seq, pos))
        total_trades += 1
        executed.append((c, total_outlay))

    settle_up_to(10**12)

    roi = (cash / initial_bankroll) - 1 if initial_bankroll > 0 else 0.0
    win_rate = (winning_trades / total_trades) if total_trades > 0 else 0.0

    return {
        "final_bankroll": cash,
        "roi": roi,
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "win_rate": win_rate,
        "realized_pnl": realized_pnl,
        "max_drawdown": max_drawdown,
        "skipped_no_cash": skipped_no_cash,
        "skipped_exposure": skipped_exposure,
        "executed": executed,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Backtest polymarket_edge_bot_realprice on closed markets.")
    parser.add_argument("--start-date", default="2026-01-01", help="UTC date, e.g. 2026-01-01")
    parser.add_argument("--end-date", default="2026-03-01", help="UTC date, e.g. 2026-03-01")
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--lookback-events", type=int, default=50000)
    parser.add_argument("--max-events-fetch", type=int, default=50000)
    parser.add_argument("--max-candidate-markets", type=int, default=1500)
    parser.add_argument("--max-history-requests", type=int, default=1200)
    parser.add_argument("--initial-bankroll", type=float, default=10.0)
    parser.add_argument("--json-output", default=None, help="Write machine-readable summary to a JSON file.")
    parser.add_argument(
        "--dataset-output",
        default=None,
        help="Write research snapshot dataset to JSONL. Pass a .jsonl path or a directory.",
    )
    parser.add_argument(
        "--use-liquidity-filter",
        action="store_true",
        help="Enable liquidity filter (usually too strict on closed snapshots).",
    )
    return parser.parse_args()


def _parse_date_to_ts(date_text: str):
    dt = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def main():
    args = parse_args()
    start_ts = _parse_date_to_ts(args.start_date)
    end_ts = _parse_date_to_ts(args.end_date) + (24 * 3600) - 1

    if args.start_offset is None:
        print("Finding total number of closed events...")
        total_closed = find_total_closed_events()
        print(f"Closed events total: {total_closed}")
        start_offset = max(0, total_closed - args.lookback_events)
        max_events = min(args.max_events_fetch, total_closed - start_offset)
    else:
        start_offset = max(0, args.start_offset)
        max_events = args.max_events_fetch

    print(f"Fetching closed events from offset {start_offset} (max {max_events})...")
    events = fetch_closed_events(start_offset=start_offset, max_events=max_events, page_size=args.page_size)
    print(f"Fetched events: {len(events)}")

    print("Building candidates and replaying model signals...")
    candidates, rejects, reasons, diagnostics, dataset_rows = build_candidates(
        events=events,
        start_ts=start_ts,
        end_ts=end_ts,
        entry_hours_before_close=args.entry_hours_before_close,
        history_window_days=args.history_window_days,
        max_markets=args.max_candidate_markets,
        fidelity=args.history_fidelity,
        use_liquidity_filter=args.use_liquidity_filter,
        max_history_requests=args.max_history_requests,
    )
    print(f"Candidates passing edge threshold: {len(candidates)}")
    print(f"Scored snapshots exported-ready: {len(dataset_rows)}")

    summary = run_simulation(candidates, initial_bankroll=args.initial_bankroll)

    print("\n=== Backtest Summary ===")
    print(f"Period: {args.start_date} .. {args.end_date} (UTC)")
    print(f"Entry timing: {args.entry_hours_before_close}h before market close")
    print(f"Initial bankroll: ${args.initial_bankroll:.2f}")
    print(f"Final bankroll: ${summary['final_bankroll']:.2f}")
    print(f"Realized PnL: ${summary['realized_pnl']:.2f}")
    print(f"ROI: {summary['roi']:.2%}")
    print(f"Trades: {summary['total_trades']}")
    print(f"Wins: {summary['winning_trades']} (win rate {summary['win_rate']:.2%})")
    print(f"Max drawdown (realized-cash): {summary['max_drawdown']:.2%}")
    print(f"Skipped (cash): {summary['skipped_no_cash']}")
    print(f"Skipped (exposure cap): {summary['skipped_exposure']}")

    print("\nReject counters:")
    print(rejects)
    print("Drop reasons before scoring:")
    print(reasons)
    print("Pipeline stage counts:")
    print(diagnostics["stage_counts"])
    rejects_by_type = diagnostics.get("rejects_by_market_type", {})
    if rejects_by_type:
        print("Reject reasons by market type:")
        print(rejects_by_type)

    market_type_final = diagnostics["market_type_stage_counts"].get("final_candidates", {})
    if market_type_final:
        print("Final candidates by market type:")
        print(market_type_final)

    print("Edge distributions after neutralization:")
    print(diagnostics["edge_distributions"])

    top_rejected = diagnostics.get("top_rejected_by_edge", [])
    if top_rejected:
        print("Top rejected by edge:")
        for item in top_rejected[:5]:
            print(
                f"- {item['reject_reason']} | {item['market_type']} | "
                f"net_edge={item['net_edge']:.3f} gross_edge={item['gross_edge']:.3f} "
                f"net_edge_lcb={item['net_edge_lcb']:.3f} "
                f"conf={item['confidence']:.2f} meta={item['meta_confidence']:.2f} "
                f"graph={item['graph_consistency']:.2f} domain={item['domain_name']}\n"
                f"  {item['question']}\n"
                f"  {item['link']}"
            )

    low_lcb_analysis = diagnostics.get("low_lcb_edge_analysis", {})
    if low_lcb_analysis:
        print("Low LCB edge breakdown:")
        print(
            {
                "count": low_lcb_analysis.get("count"),
                "by_market_type": low_lcb_analysis.get("by_market_type"),
                "by_domain_name": low_lcb_analysis.get("by_domain_name"),
                "mean_net_edge": low_lcb_analysis.get("mean_net_edge"),
                "mean_lcb_shortfall": low_lcb_analysis.get("mean_lcb_shortfall"),
                "mean_cost_gap": low_lcb_analysis.get("mean_cost_gap"),
                "mean_uncertainty": low_lcb_analysis.get("mean_uncertainty"),
                "mean_correlation_penalty": low_lcb_analysis.get("mean_correlation_penalty"),
                "mean_graph_penalty": low_lcb_analysis.get("mean_graph_penalty"),
                "mean_regime_penalty": low_lcb_analysis.get("mean_regime_penalty"),
                "mean_total_penalty": low_lcb_analysis.get("mean_total_penalty"),
                "mean_relation_inconsistency": low_lcb_analysis.get("mean_relation_inconsistency"),
            }
        )

    top = sorted(
        summary["executed"],
        key=lambda x: (x[0].net_edge_lcb, x[0].robustness_score, x[0].net_edge),
        reverse=True,
    )[:5]
    top_payload = []
    if top:
        print("\nTop executed signals by net_edge:")
        for c, outlay in top:
            event_link = f"https://polymarket.com/event/{c.event_slug}?tid={c.token_id}"
            top_payload.append(
                {
                    "question": c.question,
                    "event_slug": c.event_slug,
                    "token_id": c.token_id,
                    "entry_ts": c.entry_ts,
                    "entry_utc": _to_utc_str(c.entry_ts),
                    "entry": c.entry,
                    "fair": c.fair,
                    "fair_lcb": c.fair_lcb,
                    "gross_edge": c.gross_edge,
                    "net_edge": c.net_edge,
                    "net_edge_lcb": c.net_edge_lcb,
                    "confidence": c.confidence,
                    "meta_confidence": c.meta_confidence,
                    "graph_consistency": c.graph_consistency,
                    "robustness_score": c.robustness_score,
                    "domain_name": c.model.get("domain_name"),
                    "domain_signal": c.model.get("domain_signal"),
                    "domain_confidence": c.model.get("domain_confidence"),
                    "market_type": c.market_type,
                    "category_group": c.category_group,
                    "outlay_usd": outlay,
                    "resolved_outcome": c.resolved_outcome,
                    "link": event_link,
                }
            )
            print(
                f"- {_to_utc_str(c.entry_ts)} | edge={c.net_edge:.3f} | "
                f"edge_lcb={c.net_edge_lcb:.3f} | entry={c.entry:.3f} "
                f"fair={c.fair:.3f} fair_lcb={c.fair_lcb:.3f} outlay=${outlay:.2f}\n"
                f"  {c.question}\n  {event_link}"
            )

    if args.json_output:
        payload = {
            "train_or_test_period": {
                "start_date": args.start_date,
                "end_date": args.end_date,
            },
            "parameters": {
                "entry_hours_before_close": args.entry_hours_before_close,
                "history_window_days": args.history_window_days,
                "history_fidelity": args.history_fidelity,
                "start_offset": args.start_offset,
                "page_size": args.page_size,
                "lookback_events": args.lookback_events,
                "max_events_fetch": args.max_events_fetch,
                "max_candidate_markets": args.max_candidate_markets,
                "max_history_requests": args.max_history_requests,
                "initial_bankroll": args.initial_bankroll,
                "use_liquidity_filter": args.use_liquidity_filter,
                "env_overrides": {
                    "MIN_CONFIDENCE": os.getenv("MIN_CONFIDENCE"),
                    "MIN_GROSS_EDGE": os.getenv("MIN_GROSS_EDGE"),
                    "EDGE_THRESHOLD": os.getenv("EDGE_THRESHOLD"),
                    "WATCH_THRESHOLD": os.getenv("WATCH_THRESHOLD"),
                    "MIN_META_CONFIDENCE": os.getenv("MIN_META_CONFIDENCE"),
                    "MIN_GRAPH_CONSISTENCY": os.getenv("MIN_GRAPH_CONSISTENCY"),
                    "MIN_ROBUSTNESS_SCORE": os.getenv("MIN_ROBUSTNESS_SCORE"),
                    "MIN_LCB_EDGE": os.getenv("MIN_LCB_EDGE"),
                    "WATCH_LCB_FLOOR": os.getenv("WATCH_LCB_FLOOR"),
                    "MODEL_ADJUSTMENT_SCALE": os.getenv("MODEL_ADJUSTMENT_SCALE"),
                    "MAX_SIGNALS_PER_EVENT": os.getenv("MAX_SIGNALS_PER_EVENT"),
                },
            },
            "summary": {
                "final_bankroll": summary["final_bankroll"],
                "roi": summary["roi"],
                "total_trades": summary["total_trades"],
                "winning_trades": summary["winning_trades"],
                "win_rate": summary["win_rate"],
                "realized_pnl": summary["realized_pnl"],
                "max_drawdown": summary["max_drawdown"],
                "skipped_no_cash": summary["skipped_no_cash"],
                "skipped_exposure": summary["skipped_exposure"],
                "candidate_count": len(candidates),
                "dataset_row_count": len(dataset_rows),
            },
            "rejects": rejects,
            "drop_reasons": reasons,
            "diagnostics": diagnostics,
            "top_executed": top_payload,
        }
        with open(args.json_output, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=True)

    if args.dataset_output:
        dataset_path = resolve_dataset_output(args.dataset_output, args.start_date, args.end_date)
        write_jsonl(dataset_rows, dataset_path)
        print(f"Research dataset written: {dataset_path}")


if __name__ == "__main__":
    main()
