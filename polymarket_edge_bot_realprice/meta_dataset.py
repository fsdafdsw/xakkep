import argparse
import json
from collections import defaultdict
from pathlib import Path

from calibration_report import load_jsonl
from config import REPORTS_DIR


META_DATASET_VERSION = 2


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _bool_flag(value):
    return 1 if bool(value) else 0


def _expand_inputs(inputs):
    paths = []
    for item in inputs:
        path = Path(item)
        if path.is_dir():
            paths.extend(sorted(candidate for candidate in path.rglob("*.jsonl") if candidate.is_file()))
        elif path.is_file():
            paths.append(path)

    seen = set()
    unique = []
    for path in paths:
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _non_null_count(row):
    count = 0
    for value in row.values():
        if isinstance(value, dict):
            count += _non_null_count(value)
        elif value is not None and value != "":
            count += 1
    return count


def _decision_rank(row):
    status = str(row.get("decision_status") or "")
    if row.get("selected_for_trade") or status == "final_candidate":
        return 3
    if status == "rejected":
        return 2
    if status == "scored":
        return 1
    return 0


def _pool_preference(row):
    return (
        _decision_rank(row),
        _non_null_count(row),
        _safe_int(row.get("entry_ts"), 0) or 0,
        str(row.get("_source_path") or ""),
    )


def load_snapshot_pool(inputs):
    paths = _expand_inputs(inputs)
    deduped = {}
    duplicate_count = 0
    loaded_rows = 0

    for path in paths:
        for row in load_jsonl(path):
            if not isinstance(row, dict):
                continue
            loaded_rows += 1
            normalized = dict(row)
            normalized["_source_path"] = str(path.resolve())
            snapshot_id = normalized.get("snapshot_id")
            if not snapshot_id:
                continue
            existing = deduped.get(snapshot_id)
            if existing is None or _pool_preference(normalized) > _pool_preference(existing):
                if existing is not None:
                    duplicate_count += 1
                deduped[snapshot_id] = normalized
            else:
                duplicate_count += 1

    ordered_rows = sorted(
        deduped.values(),
        key=lambda row: (
            _safe_int(row.get("entry_ts"), 0) or 0,
            str(row.get("snapshot_id") or ""),
        ),
    )
    summary = {
        "input_count": len(paths),
        "input_paths": [str(path.resolve()) for path in paths],
        "loaded_rows": loaded_rows,
        "unique_snapshot_rows": len(ordered_rows),
        "duplicate_rows_collapsed": duplicate_count,
    }
    return ordered_rows, summary


def _gap(value_a, value_b):
    left = _safe_float(value_a)
    right = _safe_float(value_b)
    if left is None or right is None:
        return None
    return left - right


def _label_bucket(realized_pnl):
    if realized_pnl is None:
        return "unknown"
    if realized_pnl > 0.05:
        return "strong_positive"
    if realized_pnl > 0.0:
        return "positive"
    if realized_pnl >= -0.01:
        return "flat"
    return "negative"


def _timestamp_features(snapshot):
    entry_ts = _safe_int(snapshot.get("entry_ts"))
    settle_ts = _safe_int(snapshot.get("settle_ts"))
    horizon_hours = _safe_float(snapshot.get("entry_timing_hours_before_close"))

    if horizon_hours is None and entry_ts is not None and settle_ts is not None and settle_ts >= entry_ts:
        horizon_hours = (settle_ts - entry_ts) / 3600.0

    if entry_ts is None:
        return {
            "entry_hour_utc": None,
            "entry_weekday_utc": None,
            "entry_month_utc": None,
            "horizon_hours": horizon_hours,
        }

    from datetime import datetime, timezone

    dt = datetime.fromtimestamp(entry_ts, tz=timezone.utc)
    return {
        "entry_hour_utc": dt.hour,
        "entry_weekday_utc": dt.weekday(),
        "entry_month_utc": dt.month,
        "horizon_hours": horizon_hours,
    }


def build_meta_row(snapshot):
    features = snapshot.get("features") or {}
    graph = snapshot.get("graph_metrics") or {}
    robust = snapshot.get("robust_components") or {}
    policy = snapshot.get("policy") or {}
    external_components = snapshot.get("external_components") or {}
    resolution_metadata = external_components.get("resolution_metadata") or {}
    relation_metrics = external_components.get("relation_metrics") or {}
    relation_residual = external_components.get("relation_residual") or {}
    factor_weights = features.get("factor_weights") or {}
    timing = _timestamp_features(snapshot)

    realized_pnl = _safe_float(snapshot.get("realized_pnl_per_share"))
    net_edge = _safe_float(snapshot.get("net_edge"))
    net_edge_lcb = _safe_float(snapshot.get("net_edge_lcb"))
    gross_edge = _safe_float(snapshot.get("gross_edge"))
    fair = _safe_float(snapshot.get("fair"))
    market_implied = _safe_float(snapshot.get("market_implied"))
    selected_for_trade = bool(snapshot.get("selected_for_trade"))
    decision_status = snapshot.get("decision_status")

    return {
        "meta_dataset_version": META_DATASET_VERSION,
        "snapshot_id": snapshot.get("snapshot_id"),
        "source_snapshot_path": snapshot.get("_source_path"),
        "period_start_date": snapshot.get("period_start_date"),
        "period_end_date": snapshot.get("period_end_date"),
        "entry_utc": snapshot.get("entry_utc"),
        "settle_utc": snapshot.get("settle_utc"),
        "entry_hour_utc": timing["entry_hour_utc"],
        "entry_weekday_utc": timing["entry_weekday_utc"],
        "entry_month_utc": timing["entry_month_utc"],
        "horizon_hours": timing["horizon_hours"],
        "market_type": snapshot.get("market_type"),
        "category_group": snapshot.get("category_group"),
        "domain_name": snapshot.get("domain_name"),
        "semantic_family": snapshot.get("semantic_family"),
        "semantic_confidence": snapshot.get("semantic_confidence"),
        "semantic_comparator": resolution_metadata.get("comparator"),
        "semantic_target_year": resolution_metadata.get("target_year"),
        "semantic_threshold": resolution_metadata.get("threshold"),
        "decision_status": decision_status,
        "reject_reason": snapshot.get("reject_reason"),
        "selected_for_trade": _bool_flag(selected_for_trade),
        "trade_bucket": snapshot.get("trade_bucket"),
        "market_implied": market_implied,
        "fair": fair,
        "fair_lcb": snapshot.get("fair_lcb"),
        "gross_edge": gross_edge,
        "net_edge": snapshot.get("net_edge"),
        "gross_edge_lcb": snapshot.get("gross_edge_lcb"),
        "net_edge_lcb": snapshot.get("net_edge_lcb"),
        "confidence": snapshot.get("confidence"),
        "meta_confidence": snapshot.get("meta_confidence"),
        "meta_trade_prob": snapshot.get("meta_trade_prob"),
        "meta_trade_score": snapshot.get("meta_trade_score"),
        "graph_consistency": snapshot.get("graph_consistency"),
        "robustness_score": snapshot.get("robustness_score"),
        "spread": snapshot.get("spread"),
        "cost_per_share": snapshot.get("cost_per_share"),
        "quality": features.get("quality"),
        "momentum": features.get("momentum"),
        "anomaly": features.get("anomaly"),
        "orderbook": features.get("orderbook"),
        "news": features.get("news"),
        "external": features.get("external"),
        "external_confidence": features.get("external_confidence"),
        "adjustment_multiplier": features.get("adjustment_multiplier"),
        "factor_weight_quality": factor_weights.get("quality"),
        "factor_weight_momentum": factor_weights.get("momentum"),
        "factor_weight_anomaly": factor_weights.get("anomaly"),
        "factor_weight_orderbook": factor_weights.get("orderbook"),
        "factor_weight_news": factor_weights.get("news"),
        "factor_weight_external": factor_weights.get("external"),
        "domain_signal": snapshot.get("domain_signal"),
        "domain_confidence": snapshot.get("domain_confidence"),
        "relation_degree": snapshot.get("relation_degree"),
        "relation_confidence": snapshot.get("relation_confidence"),
        "relation_support_price": snapshot.get("relation_support_price"),
        "relation_support_confidence": snapshot.get("relation_support_confidence"),
        "relation_residual": snapshot.get("relation_residual"),
        "relation_inconsistency": snapshot.get("relation_inconsistency"),
        "relation_constraint_violation": relation_residual.get("constraint_violation"),
        "event_size": graph.get("event_size"),
        "rank_in_event": graph.get("rank_in_event"),
        "overround": graph.get("overround"),
        "underround": graph.get("underround"),
        "crowdedness": graph.get("crowdedness"),
        "correlation_penalty": robust.get("correlation_penalty"),
        "graph_penalty": robust.get("graph_penalty"),
        "regime_penalty": robust.get("regime_penalty"),
        "uncertainty": robust.get("uncertainty"),
        "total_penalty": robust.get("total_penalty"),
        "price_extremeness": robust.get("price_extremeness"),
        "supported_adjustment": robust.get("supported_adjustment"),
        "overreach": robust.get("overreach"),
        "raw_adjustment": robust.get("raw_adjustment"),
        "policy_min_confidence": policy.get("min_confidence"),
        "policy_min_gross_edge": policy.get("min_gross_edge"),
        "policy_edge_threshold": policy.get("edge_threshold"),
        "policy_watch_threshold": policy.get("watch_threshold"),
        "policy_min_meta_confidence": policy.get("min_meta_confidence"),
        "policy_min_graph_consistency": policy.get("min_graph_consistency"),
        "policy_min_robustness_score": policy.get("min_robustness_score"),
        "policy_min_lcb_edge": policy.get("min_lcb_edge"),
        "resolved_outcome": snapshot.get("resolved_outcome"),
        "realized_pnl_per_share": realized_pnl,
        "realized_edge_gap_vs_net": _gap(realized_pnl, net_edge),
        "realized_edge_gap_vs_lcb": _gap(realized_pnl, net_edge_lcb),
        "fair_minus_market": _gap(fair, market_implied),
        "label_trade_positive": _bool_flag(realized_pnl is not None and realized_pnl > 0.0),
        "label_trade_nonnegative": _bool_flag(realized_pnl is not None and realized_pnl >= 0.0),
        "label_expected_edge_positive": _bool_flag(net_edge is not None and net_edge > 0.0),
        "label_lcb_edge_positive": _bool_flag(net_edge_lcb is not None and net_edge_lcb > 0.0),
        "label_selected_and_positive": _bool_flag(selected_for_trade and realized_pnl is not None and realized_pnl > 0.0),
        "label_final_candidate": _bool_flag(decision_status == "final_candidate"),
        "label_rejected": _bool_flag(decision_status == "rejected"),
        "realized_pnl_bucket": _label_bucket(realized_pnl),
    }


def build_meta_dataset(snapshot_rows):
    return [build_meta_row(row) for row in snapshot_rows]


def write_jsonl(rows, output_path):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")


def _count(rows, field):
    counts = defaultdict(int)
    for row in rows:
        value = row.get(field)
        if value is None:
            value = "null"
        counts[str(value)] += 1
    return dict(sorted(counts.items()))


def _mean(rows, field):
    values = [_safe_float(row.get(field)) for row in rows]
    values = [value for value in values if value is not None]
    return (sum(values) / len(values)) if values else None


def build_summary(snapshot_rows, meta_rows, pool_info):
    family_rows = defaultdict(list)
    for row in meta_rows:
        family_rows[row.get("market_type") or "unknown"].append(row)

    families = {}
    for family, rows in sorted(family_rows.items()):
        families[family] = {
            "row_count": len(rows),
            "decision_status_counts": _count(rows, "decision_status"),
            "reject_reason_counts": _count(rows, "reject_reason"),
            "trade_positive_counts": _count(rows, "label_trade_positive"),
            "selected_counts": _count(rows, "selected_for_trade"),
            "mean_net_edge": _mean(rows, "net_edge"),
            "mean_net_edge_lcb": _mean(rows, "net_edge_lcb"),
            "mean_realized_pnl_per_share": _mean(rows, "realized_pnl_per_share"),
        }

    return {
        "meta_dataset_version": META_DATASET_VERSION,
        "pool": dict(pool_info),
        "overall": {
            "snapshot_row_count": len(snapshot_rows),
            "meta_row_count": len(meta_rows),
            "market_type_counts": _count(meta_rows, "market_type"),
            "category_group_counts": _count(meta_rows, "category_group"),
            "domain_name_counts": _count(meta_rows, "domain_name"),
            "decision_status_counts": _count(meta_rows, "decision_status"),
            "reject_reason_counts": _count(meta_rows, "reject_reason"),
            "trade_positive_counts": _count(meta_rows, "label_trade_positive"),
            "realized_pnl_bucket_counts": _count(meta_rows, "realized_pnl_bucket"),
            "selected_counts": _count(meta_rows, "selected_for_trade"),
            "mean_net_edge": _mean(meta_rows, "net_edge"),
            "mean_net_edge_lcb": _mean(meta_rows, "net_edge_lcb"),
            "mean_realized_pnl_per_share": _mean(meta_rows, "realized_pnl_per_share"),
        },
        "families": families,
    }


def default_output_path():
    return REPORTS_DIR / "research" / "meta_dataset.jsonl"


def default_pool_output_path():
    return REPORTS_DIR / "research" / "snapshot_pool.jsonl"


def default_summary_output_path():
    return REPORTS_DIR / "research" / "meta_dataset_summary.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Build meta-label dataset from research snapshot JSONLs.")
    parser.add_argument("--inputs", nargs="+", required=True, help="JSONL files or directories with research snapshots.")
    parser.add_argument("--output", default=None, help="Where to write meta dataset JSONL.")
    parser.add_argument("--pool-output", default=None, help="Where to write merged deduped snapshot pool JSONL.")
    parser.add_argument("--summary-output", default=None, help="Where to write summary JSON.")
    return parser.parse_args()


def main():
    args = parse_args()
    snapshot_rows, pool_info = load_snapshot_pool(args.inputs)
    meta_rows = build_meta_dataset(snapshot_rows)
    summary = build_summary(snapshot_rows, meta_rows, pool_info)

    pool_output = Path(args.pool_output) if args.pool_output else default_pool_output_path()
    meta_output = Path(args.output) if args.output else default_output_path()
    summary_output = Path(args.summary_output) if args.summary_output else default_summary_output_path()

    write_jsonl(snapshot_rows, pool_output)
    write_jsonl(meta_rows, meta_output)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    with summary_output.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=True)

    print(f"Snapshot pool rows: {len(snapshot_rows)}")
    print(f"Meta dataset rows: {len(meta_rows)}")
    print(f"Duplicate rows collapsed: {pool_info['duplicate_rows_collapsed']}")
    print(f"Snapshot pool written: {pool_output}")
    print(f"Meta dataset written: {meta_output}")
    print(f"Summary written: {summary_output}")
    print(f"Market type counts: {summary['overall']['market_type_counts']}")
    print(f"Reject reason counts: {summary['overall']['reject_reason_counts']}")
    print(f"Trade-positive label counts: {summary['overall']['trade_positive_counts']}")


if __name__ == "__main__":
    main()
