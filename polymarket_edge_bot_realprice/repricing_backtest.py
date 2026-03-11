import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from backtest import (
    _parse_date_to_ts,
    build_candidates,
    change_over,
    fetch_closed_events,
    fetch_price_history,
    find_total_closed_events,
    resolve_dataset_output,
    write_jsonl,
)
from calibration_report import load_jsonl
from catalyst_parser import parse_catalyst
from config import MIN_GEOPOLITICAL_REPRICING, REPORTS_DIR
from repricing_selector import score_repricing_signal


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


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _parse_csv_floats(text):
    values = []
    for item in str(text or "").split(","):
        item = item.strip()
        if not item:
            continue
        values.append(float(item))
    return values


def _parse_csv_ints(text):
    values = []
    for item in str(text or "").split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values


def _to_utc_str(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _max_price_up_to(history, end_ts):
    points = [(ts, price) for ts, price in history if ts <= end_ts]
    if not points:
        return None, None
    return max(points, key=lambda item: item[1])


def _min_price_up_to(history, end_ts):
    points = [(ts, price) for ts, price in history if ts <= end_ts]
    if not points:
        return None, None
    return min(points, key=lambda item: item[1])


def _first_cross_time(history, threshold_price, end_ts):
    for ts, price in history:
        if ts > end_ts:
            break
        if price >= threshold_price:
            return ts
    return None


def _distribution(values):
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "p50": None,
            "p90": None,
        }
    ordered = sorted(clean)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
        "p50": ordered[len(ordered) // 2],
        "p90": ordered[min(len(ordered) - 1, int(len(ordered) * 0.9))],
    }


def _extract_domain_components(row):
    external = row.get("external_components") or {}
    if "domain" in external and isinstance(external["domain"], dict):
        return external["domain"].get("components") or {}
    return {}


def _extract_repricing_potential(row):
    direct = _safe_float(row.get("repricing_potential"))
    if direct is not None:
        return direct
    components = _extract_domain_components(row)
    return _safe_float(components.get("repricing_potential"), default=0.0) or 0.0


def _extract_domain_action_family(row):
    direct = row.get("domain_action_family")
    if direct:
        return str(direct)
    components = _extract_domain_components(row)
    action = components.get("action_family")
    return str(action) if action else None


def _extract_catalyst_type(row):
    direct = row.get("catalyst_type")
    if direct:
        return str(direct)
    components = _extract_domain_components(row)
    catalyst_type = components.get("catalyst_type")
    return str(catalyst_type) if catalyst_type else None


def _extract_catalyst_strength(row):
    direct = _safe_float(row.get("catalyst_strength"))
    if direct is not None:
        return direct
    components = _extract_domain_components(row)
    return _safe_float(components.get("catalyst_strength"), default=0.0) or 0.0


def _extract_catalyst_hardness(row):
    direct = row.get("catalyst_hardness")
    if direct:
        return str(direct)
    components = _extract_domain_components(row)
    hardness = components.get("catalyst_hardness")
    return str(hardness) if hardness else None


def _extract_repricing_score(row):
    return _safe_float(row.get("repricing_score"), default=0.0) or 0.0


def _extract_repricing_watch_score(row):
    return _safe_float(row.get("repricing_watch_score"), default=0.0) or 0.0


def _extract_repricing_verdict(row):
    verdict = row.get("repricing_verdict")
    return str(verdict) if verdict else None


def _extract_repricing_metric(row, key, default=0.0):
    return _safe_float(row.get(key), default=default) or 0.0


def _rebuild_repricing_prediction(row, *, one_hour_change, one_day_change, one_week_change, hours_to_close):
    domain_components = dict(_extract_domain_components(row))
    direct_potential = _safe_float(row.get("repricing_potential"))
    if direct_potential is not None:
        domain_components["repricing_potential"] = direct_potential

    catalyst = parse_catalyst(row.get("question"))
    if catalyst:
        domain_components.setdefault("catalyst_type", catalyst.get("catalyst_type"))
        domain_components.setdefault("action_family", catalyst.get("catalyst_family"))
        domain_components.setdefault("catalyst_strength", catalyst.get("catalyst_strength"))
        domain_components.setdefault("catalyst_hardness", catalyst.get("catalyst_hardness"))
        domain_components.setdefault("catalyst_reversibility", catalyst.get("catalyst_reversibility"))
        domain_components.setdefault("catalyst_has_official_source", catalyst.get("catalyst_has_official_source"))

    external_components = row.get("external_components") or {}
    model = {
        "domain_name": row.get("domain_name"),
        "domain_confidence": _safe_float(row.get("domain_confidence"), default=0.5) or 0.5,
        "external_components": {
            "domain": {
                "components": domain_components,
            },
            "relation_residual": external_components.get("relation_residual") or {},
        },
    }

    return score_repricing_signal(
        entry_price=_safe_float(row.get("entry_price") or row.get("market_implied"), default=0.5) or 0.5,
        confidence=_safe_float(row.get("confidence"), default=0.5) or 0.5,
        net_edge=_safe_float(row.get("net_edge"), default=0.0) or 0.0,
        net_edge_lcb=_safe_float(row.get("net_edge_lcb"), default=0.0) or 0.0,
        spread=_safe_float(row.get("spread"), default=0.0) or 0.0,
        liquidity=domain_components.get("liquidity"),
        volume24h=domain_components.get("volume24h"),
        one_hour_change=one_hour_change,
        one_day_change=one_day_change,
        one_week_change=one_week_change,
        hours_to_close=hours_to_close,
        model=model,
        market_type=row.get("market_type"),
        category_group=row.get("category_group"),
    )


def _default_json_output(start_date, end_date):
    return REPORTS_DIR / "research" / f"repricing_backtest_{start_date}_{end_date}.json"


def _default_repricing_dataset_output(start_date, end_date):
    return REPORTS_DIR / "research" / f"repricing_snapshots_{start_date}_{end_date}.jsonl"


def _row_matches_filters(row, args):
    if args.domain_name and args.domain_name.lower() != "any":
        if str(row.get("domain_name") or "") != args.domain_name:
            return False
    if args.market_type and args.market_type.lower() != "any":
        if str(row.get("market_type") or "") != args.market_type:
            return False
    if args.category_group and args.category_group.lower() != "any":
        if str(row.get("category_group") or "") != args.category_group:
            return False
    if args.action_family and args.action_family.lower() != "any":
        if str(_extract_domain_action_family(row) or "") != args.action_family:
            return False
    if args.catalyst_type and args.catalyst_type.lower() != "any":
        if str(_extract_catalyst_type(row) or "") != args.catalyst_type:
            return False
    if _extract_repricing_potential(row) < args.min_repricing_potential:
        return False
    return True


def _load_or_build_snapshot_rows(args):
    if args.dataset_input:
        rows = [row for row in load_jsonl(args.dataset_input) if isinstance(row, dict)]
        return rows, {"source": "dataset_input", "row_count": len(rows)}

    start_ts = _parse_date_to_ts(args.start_date)
    end_ts = _parse_date_to_ts(args.end_date) + (24 * 3600) - 1

    if args.start_offset is None:
        total_closed = find_total_closed_events()
        start_offset = max(0, total_closed - args.lookback_events)
        max_events = min(args.max_events_fetch, total_closed - start_offset)
    else:
        start_offset = max(0, args.start_offset)
        max_events = args.max_events_fetch

    events = fetch_closed_events(start_offset=start_offset, max_events=max_events, page_size=args.page_size)
    _, rejects, reasons, diagnostics, dataset_rows = build_candidates(
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

    if args.dataset_output:
        dataset_path = resolve_dataset_output(args.dataset_output, args.start_date, args.end_date)
        write_jsonl(dataset_rows, dataset_path)

    return dataset_rows, {
        "source": "rebuilt_from_events",
        "event_count": len(events),
        "rejects": rejects,
        "drop_reasons": reasons,
        "diagnostics": diagnostics,
    }


def _repricing_labels(entry_price, max_price, take_profit_levels, target_prices):
    labels = {}
    if entry_price is None or max_price is None or entry_price <= 0:
        for level in take_profit_levels:
            labels[f"repriced_{int(round(level * 100))}pct"] = False
        for target in target_prices:
            cents = int(round(target * 100))
            labels[f"repriced_to_{cents}c"] = False
        return labels

    for level in take_profit_levels:
        labels[f"repriced_{int(round(level * 100))}pct"] = max_price >= (entry_price * (1.0 + level))
    for target in target_prices:
        cents = int(round(target * 100))
        labels[f"repriced_to_{cents}c"] = max_price >= target
    return labels


def _conflict_repricing_labels(entry_price, max_price, runup_levels, target_prices):
    labels = {}
    if entry_price is None or max_price is None or entry_price <= 0:
        for level in runup_levels:
            labels[f"conflict_repriced_{int(round(level * 100))}pct"] = False
        for target in target_prices:
            labels[f"conflict_repriced_to_{int(round(target * 100))}c"] = False
        return labels

    for level in runup_levels:
        labels[f"conflict_repriced_{int(round(level * 100))}pct"] = max_price >= (entry_price * (1.0 + level))
    for target in target_prices:
        labels[f"conflict_repriced_to_{int(round(target * 100))}c"] = max_price >= target
    return labels


def analyze_repricing(rows, args):
    windows_days = _parse_csv_ints(args.windows_days)
    take_profit_levels = _parse_csv_floats(args.take_profit_levels)
    target_prices = _parse_csv_floats(args.target_prices)
    conflict_runup_levels = _parse_csv_floats(args.conflict_runup_levels)
    conflict_target_prices = _parse_csv_floats(args.conflict_target_prices)
    max_window_days = max(windows_days)
    filtered = [row for row in rows if _row_matches_filters(row, args)]

    analyses = []
    by_market_type = defaultdict(list)
    by_domain_name = defaultdict(list)
    by_action_family = defaultdict(list)
    by_catalyst_type = defaultdict(list)
    by_repricing_verdict = defaultdict(list)

    for idx, row in enumerate(filtered, start=1):
        token_id = row.get("token_id")
        entry_ts = _safe_int(row.get("entry_ts"))
        settle_ts = _safe_int(row.get("settle_ts"))
        entry_price = _safe_float(row.get("entry_price") or row.get("market_implied"))
        if not token_id or entry_ts is None or settle_ts is None or entry_price is None:
            continue

        history_start_ts = max(0, entry_ts - (args.pre_entry_lookback_days * 24 * 3600))
        forward_end_ts = min(settle_ts, entry_ts + (max_window_days * 24 * 3600))
        try:
            history = fetch_price_history(token_id, history_start_ts, forward_end_ts, fidelity=args.history_fidelity)
        except RuntimeError as exc:
            analyses.append(
                {
                    "snapshot_id": row.get("snapshot_id"),
                    "question": row.get("question"),
                    "domain_name": row.get("domain_name"),
                    "market_type": row.get("market_type"),
                    "entry_price": entry_price,
                    "error": str(exc),
                }
            )
            continue

        forward_history = [(ts, price) for ts, price in history if ts >= entry_ts]
        if not forward_history or forward_history[0][0] != entry_ts:
            forward_history = [(entry_ts, entry_price)] + forward_history

        one_hour_change = change_over(history, entry_ts, 3600)
        one_day_change = change_over(history, entry_ts, 24 * 3600)
        one_week_change = change_over(history, entry_ts, 7 * 24 * 3600)
        hours_to_close = max(0.0, (settle_ts - entry_ts) / 3600.0)
        rebuilt_repricing = _rebuild_repricing_prediction(
            row,
            one_hour_change=one_hour_change,
            one_day_change=one_day_change,
            one_week_change=one_week_change,
            hours_to_close=hours_to_close,
        )

        windows = {}
        runups = []
        hit_counts = []
        conflict_hit_counts = []
        action_family = _extract_domain_action_family(row)
        for window in windows_days:
            window_end_ts = min(settle_ts, entry_ts + (window * 24 * 3600))
            max_ts, max_price = _max_price_up_to(forward_history, window_end_ts)
            min_ts, min_price = _min_price_up_to(forward_history, window_end_ts)
            runup_abs = (max_price - entry_price) if max_price is not None else None
            runup_pct = ((max_price / entry_price) - 1.0) if max_price is not None and entry_price > 0 else None
            drawdown_abs = (min_price - entry_price) if min_price is not None else None
            drawdown_pct = ((min_price / entry_price) - 1.0) if min_price is not None and entry_price > 0 else None

            labels = _repricing_labels(entry_price, max_price, take_profit_levels, target_prices)
            conflict_labels = _conflict_repricing_labels(entry_price, max_price, conflict_runup_levels, conflict_target_prices) if action_family == "conflict" else {}
            time_to_first_target = {}
            for level in take_profit_levels:
                threshold_price = entry_price * (1.0 + level)
                hit_ts = _first_cross_time(forward_history, threshold_price, window_end_ts)
                label_key = f"repriced_{int(round(level * 100))}pct"
                time_to_first_target[label_key] = (hit_ts - entry_ts) if hit_ts is not None else None
                hit_counts.append(int(labels[label_key]))

            for target in target_prices:
                cents = int(round(target * 100))
                hit_ts = _first_cross_time(forward_history, target, window_end_ts)
                label_key = f"repriced_to_{cents}c"
                time_to_first_target[label_key] = (hit_ts - entry_ts) if hit_ts is not None else None
                hit_counts.append(int(labels[label_key]))

            if action_family == "conflict":
                for level in conflict_runup_levels:
                    label_key = f"conflict_repriced_{int(round(level * 100))}pct"
                    threshold_price = entry_price * (1.0 + level)
                    hit_ts = _first_cross_time(forward_history, threshold_price, window_end_ts)
                    time_to_first_target[label_key] = (hit_ts - entry_ts) if hit_ts is not None else None
                    conflict_hit_counts.append(int(conflict_labels[label_key]))
                for target in conflict_target_prices:
                    label_key = f"conflict_repriced_to_{int(round(target * 100))}c"
                    hit_ts = _first_cross_time(forward_history, target, window_end_ts)
                    time_to_first_target[label_key] = (hit_ts - entry_ts) if hit_ts is not None else None
                    conflict_hit_counts.append(int(conflict_labels[label_key]))

            windows[f"{window}d"] = {
                "window_days": window,
                "window_end_ts": window_end_ts,
                "window_end_utc": _to_utc_str(window_end_ts),
                "max_price": max_price,
                "max_price_utc": _to_utc_str(max_ts),
                "min_price": min_price,
                "min_price_utc": _to_utc_str(min_ts),
                "runup_abs": runup_abs,
                "runup_pct": runup_pct,
                "drawdown_abs": drawdown_abs,
                "drawdown_pct": drawdown_pct,
                "labels": labels,
                "conflict_labels": conflict_labels,
                "time_to_first_target_seconds": time_to_first_target,
            }
            if runup_pct is not None:
                runups.append(runup_pct)

        analysis = {
            "snapshot_id": row.get("snapshot_id"),
            "question": row.get("question"),
            "token_id": token_id,
            "event_slug": row.get("event_slug"),
            "market_slug": row.get("market_slug"),
            "link": f"https://polymarket.com/event/{row.get('event_slug')}?tid={token_id}" if row.get("event_slug") else None,
            "domain_name": row.get("domain_name"),
            "domain_action_family": _extract_domain_action_family(row),
            "catalyst_type": _extract_catalyst_type(row),
            "catalyst_strength": _extract_catalyst_strength(row),
            "catalyst_hardness": _extract_catalyst_hardness(row),
            "repricing_potential": _extract_repricing_potential(row),
            "repricing_score": rebuilt_repricing.get("score", _extract_repricing_score(row)),
            "repricing_watch_score": rebuilt_repricing.get("watch_score", _extract_repricing_watch_score(row)),
            "repricing_verdict": rebuilt_repricing.get("verdict", _extract_repricing_verdict(row)),
            "repricing_reason": rebuilt_repricing.get("reason") or row.get("repricing_reason"),
            "repricing_attention_gap": rebuilt_repricing.get("attention_gap", _extract_repricing_metric(row, "repricing_attention_gap")),
            "repricing_underreaction_score": rebuilt_repricing.get("underreaction_score", _extract_repricing_metric(row, "repricing_underreaction_score")),
            "repricing_fresh_catalyst_score": rebuilt_repricing.get("fresh_catalyst_score", _extract_repricing_metric(row, "repricing_fresh_catalyst_score")),
            "repricing_trend_chase_penalty": rebuilt_repricing.get("trend_chase_penalty", _extract_repricing_metric(row, "repricing_trend_chase_penalty")),
            "repricing_optionality_score": rebuilt_repricing.get("optionality_score", _extract_repricing_metric(row, "repricing_optionality_score")),
            "repricing_recent_runup": rebuilt_repricing.get("recent_runup", _extract_repricing_metric(row, "repricing_recent_runup")),
            "repricing_recent_selloff": rebuilt_repricing.get("recent_selloff", _extract_repricing_metric(row, "repricing_recent_selloff")),
            "repricing_compression_score": rebuilt_repricing.get("compression_score", _extract_repricing_metric(row, "repricing_compression_score")),
            "repricing_deadline_pressure": rebuilt_repricing.get("deadline_pressure", _extract_repricing_metric(row, "repricing_deadline_pressure")),
            "repricing_book_quality": rebuilt_repricing.get("book_quality", _extract_repricing_metric(row, "repricing_book_quality")),
            "repricing_stale_score": rebuilt_repricing.get("stale_score", _extract_repricing_metric(row, "repricing_stale_score")),
            "repricing_already_priced_penalty": rebuilt_repricing.get("already_priced_penalty", _extract_repricing_metric(row, "repricing_already_priced_penalty")),
            "market_type": row.get("market_type"),
            "category_group": row.get("category_group"),
            "decision_status": row.get("decision_status"),
            "reject_reason": row.get("reject_reason"),
            "entry_ts": entry_ts,
            "entry_utc": row.get("entry_utc") or _to_utc_str(entry_ts),
            "settle_ts": settle_ts,
            "settle_utc": row.get("settle_utc") or _to_utc_str(settle_ts),
            "entry_price": entry_price,
            "one_hour_change": one_hour_change,
            "one_day_change": one_day_change,
            "one_week_change": one_week_change,
            "hours_to_close": hours_to_close,
            "confidence": _safe_float(row.get("confidence")),
            "fair": _safe_float(row.get("fair")),
            "net_edge": _safe_float(row.get("net_edge")),
            "net_edge_lcb": _safe_float(row.get("net_edge_lcb")),
            "window_metrics": windows,
            "best_runup_pct": max(runups) if runups else None,
            "repricing_hit_count": sum(hit_counts),
            "conflict_repricing_hit_count": sum(conflict_hit_counts),
        }
        analyses.append(analysis)
        by_market_type[str(analysis.get("market_type") or "unknown")].append(analysis)
        by_domain_name[str(analysis.get("domain_name") or "unknown")].append(analysis)
        by_action_family[str(analysis.get("domain_action_family") or "unknown")].append(analysis)
        by_catalyst_type[str(analysis.get("catalyst_type") or "unknown")].append(analysis)
        by_repricing_verdict[str(analysis.get("repricing_verdict") or "unknown")].append(analysis)

        if idx % 25 == 0:
            print(f"Processed repricing forward history: {idx}/{len(filtered)}")

    return (
        analyses,
        windows_days,
        take_profit_levels,
        target_prices,
        conflict_runup_levels,
        conflict_target_prices,
        by_market_type,
        by_domain_name,
        by_action_family,
        by_catalyst_type,
        by_repricing_verdict,
    )


def _summarize_group(rows, windows_days, take_profit_levels, target_prices, conflict_runup_levels, conflict_target_prices):
    summary = {
        "count": len(rows),
        "best_runup_pct": _distribution([row.get("best_runup_pct") for row in rows]),
        "repricing_features": {
            "attention_gap": _distribution([row.get("repricing_attention_gap") for row in rows]),
            "underreaction_score": _distribution([row.get("repricing_underreaction_score") for row in rows]),
            "fresh_catalyst_score": _distribution([row.get("repricing_fresh_catalyst_score") for row in rows]),
            "trend_chase_penalty": _distribution([row.get("repricing_trend_chase_penalty") for row in rows]),
            "optionality_score": _distribution([row.get("repricing_optionality_score") for row in rows]),
            "recent_runup": _distribution([row.get("repricing_recent_runup") for row in rows]),
            "recent_selloff": _distribution([row.get("repricing_recent_selloff") for row in rows]),
            "compression_score": _distribution([row.get("repricing_compression_score") for row in rows]),
            "deadline_pressure": _distribution([row.get("repricing_deadline_pressure") for row in rows]),
            "book_quality": _distribution([row.get("repricing_book_quality") for row in rows]),
            "stale_score": _distribution([row.get("repricing_stale_score") for row in rows]),
            "already_priced_penalty": _distribution([row.get("repricing_already_priced_penalty") for row in rows]),
        },
        "windows": {},
    }
    for window in windows_days:
        key = f"{window}d"
        window_rows = [row.get("window_metrics", {}).get(key) or {} for row in rows]
        window_summary = {
            "runup_pct": _distribution([item.get("runup_pct") for item in window_rows]),
            "drawdown_pct": _distribution([item.get("drawdown_pct") for item in window_rows]),
            "max_price": _distribution([item.get("max_price") for item in window_rows]),
            "label_rates": {},
            "conflict_label_rates": {},
        }
        for level in take_profit_levels:
            label_key = f"repriced_{int(round(level * 100))}pct"
            values = [1 if (item.get("labels") or {}).get(label_key) else 0 for item in window_rows]
            window_summary["label_rates"][label_key] = (sum(values) / len(values)) if values else None
        for target in target_prices:
            label_key = f"repriced_to_{int(round(target * 100))}c"
            values = [1 if (item.get("labels") or {}).get(label_key) else 0 for item in window_rows]
            window_summary["label_rates"][label_key] = (sum(values) / len(values)) if values else None
        for level in conflict_runup_levels:
            label_key = f"conflict_repriced_{int(round(level * 100))}pct"
            conflict_rows = [item for item in window_rows if item.get("conflict_labels")]
            values = [1 if (item.get("conflict_labels") or {}).get(label_key) else 0 for item in conflict_rows]
            window_summary["conflict_label_rates"][label_key] = (sum(values) / len(values)) if values else None
        for target in conflict_target_prices:
            label_key = f"conflict_repriced_to_{int(round(target * 100))}c"
            conflict_rows = [item for item in window_rows if item.get("conflict_labels")]
            values = [1 if (item.get("conflict_labels") or {}).get(label_key) else 0 for item in conflict_rows]
            window_summary["conflict_label_rates"][label_key] = (sum(values) / len(values)) if values else None
        summary["windows"][key] = window_summary
    return summary


def parse_args():
    parser = argparse.ArgumentParser(description="Backtest repricing potential after a model signal.")
    parser.add_argument("--start-date", default="2026-01-01", help="UTC date, e.g. 2026-01-01")
    parser.add_argument("--end-date", default="2026-03-01", help="UTC date, e.g. 2026-03-01")
    parser.add_argument("--dataset-input", default=None, help="Existing snapshot JSONL to analyze instead of rebuilding.")
    parser.add_argument("--dataset-output", default=None, help="Optional snapshot JSONL output when rebuilding from events.")
    parser.add_argument("--repricing-output", default=None, help="Optional JSONL path for repricing-enriched rows.")
    parser.add_argument("--json-output", default=None, help="Optional JSON summary output path.")
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--lookback-events", type=int, default=50000)
    parser.add_argument("--max-events-fetch", type=int, default=50000)
    parser.add_argument("--max-candidate-markets", type=int, default=1500)
    parser.add_argument("--max-history-requests", type=int, default=1200)
    parser.add_argument("--use-liquidity-filter", action="store_true")
    parser.add_argument("--domain-name", default="geopolitical_repricing")
    parser.add_argument("--market-type", default="any")
    parser.add_argument("--category-group", default="any")
    parser.add_argument("--action-family", default="any")
    parser.add_argument("--catalyst-type", default="any")
    parser.add_argument("--min-repricing-potential", type=float, default=MIN_GEOPOLITICAL_REPRICING)
    parser.add_argument("--windows-days", default="3,7,14")
    parser.add_argument("--take-profit-levels", default="0.25,0.50")
    parser.add_argument("--target-prices", default="0.10,0.20")
    parser.add_argument("--conflict-runup-levels", default="0.10,0.25")
    parser.add_argument("--conflict-target-prices", default="0.40,0.60")
    parser.add_argument("--top-limit", type=int, default=10)
    parser.add_argument("--pre-entry-lookback-days", type=int, default=7)
    return parser.parse_args()


def main():
    args = parse_args()
    rows, source_meta = _load_or_build_snapshot_rows(args)
    print(f"Snapshot rows loaded: {len(rows)}")
    filtered_rows = [row for row in rows if _row_matches_filters(row, args)]
    print(f"Rows after filters: {len(filtered_rows)}")

    (
        analyses,
        windows_days,
        take_profit_levels,
        target_prices,
        conflict_runup_levels,
        conflict_target_prices,
        by_market_type,
        by_domain_name,
        by_action_family,
        by_catalyst_type,
        by_repricing_verdict,
    ) = analyze_repricing(rows, args)
    analyses_ok = [row for row in analyses if not row.get("error")]
    print(f"Rows with forward repricing history: {len(analyses_ok)}")

    overall = _summarize_group(analyses_ok, windows_days, take_profit_levels, target_prices, conflict_runup_levels, conflict_target_prices)
    by_market_type_summary = {
        key: _summarize_group(value, windows_days, take_profit_levels, target_prices, conflict_runup_levels, conflict_target_prices)
        for key, value in sorted(by_market_type.items())
    }
    by_domain_name_summary = {
        key: _summarize_group(value, windows_days, take_profit_levels, target_prices, conflict_runup_levels, conflict_target_prices)
        for key, value in sorted(by_domain_name.items())
    }
    by_action_family_summary = {
        key: _summarize_group(value, windows_days, take_profit_levels, target_prices, conflict_runup_levels, conflict_target_prices)
        for key, value in sorted(by_action_family.items())
    }
    by_catalyst_type_summary = {
        key: _summarize_group(value, windows_days, take_profit_levels, target_prices, conflict_runup_levels, conflict_target_prices)
        for key, value in sorted(by_catalyst_type.items())
    }
    by_repricing_verdict_summary = {
        key: _summarize_group(value, windows_days, take_profit_levels, target_prices, conflict_runup_levels, conflict_target_prices)
        for key, value in sorted(by_repricing_verdict.items())
    }

    top_repricing = sorted(
        analyses_ok,
        key=lambda row: (
            row.get("best_runup_pct") if row.get("best_runup_pct") is not None else float("-inf"),
            row.get("repricing_score") or 0.0,
            row.get("repricing_potential") or 0.0,
            row.get("confidence") or 0.0,
        ),
        reverse=True,
    )[: args.top_limit]

    summary = {
        "dataset_source": source_meta,
        "filters": {
            "domain_name": args.domain_name,
            "market_type": args.market_type,
            "category_group": args.category_group,
            "action_family": args.action_family,
            "catalyst_type": args.catalyst_type,
            "min_repricing_potential": args.min_repricing_potential,
        },
        "row_count": len(rows),
        "filtered_row_count": len(filtered_rows),
        "analyzed_row_count": len(analyses_ok),
        "windows_days": windows_days,
        "take_profit_levels": take_profit_levels,
        "target_prices": target_prices,
        "conflict_runup_levels": conflict_runup_levels,
        "conflict_target_prices": conflict_target_prices,
        "overall": overall,
        "by_market_type": by_market_type_summary,
        "by_domain_name": by_domain_name_summary,
        "by_action_family": by_action_family_summary,
        "by_catalyst_type": by_catalyst_type_summary,
        "by_repricing_verdict": by_repricing_verdict_summary,
        "top_repricing": top_repricing,
        "errors": [row for row in analyses if row.get("error")],
    }

    print("\n=== Repricing Summary ===")
    print(f"Overall analyzed rows: {summary['analyzed_row_count']}")
    for window in windows_days:
        key = f"{window}d"
        window_summary = overall["windows"][key]
        print(
            f"{key}: mean_runup={window_summary['runup_pct']['mean']} "
            f"tp25={window_summary['label_rates'].get('repriced_25pct')} "
            f"tp50={window_summary['label_rates'].get('repriced_50pct')}"
        )

    if top_repricing:
        print("\nTop repricing candidates:")
        for item in top_repricing[:5]:
            print(
                f"- runup={item.get('best_runup_pct')} repricing={item.get('repricing_potential')} "
                f"score={item.get('repricing_score')} conf={item.get('confidence')} "
                f"action={item.get('domain_action_family')} catalyst={item.get('catalyst_type')}\n"
                f"  {item.get('question')}"
            )

    json_output = Path(args.json_output) if args.json_output else _default_json_output(args.start_date, args.end_date)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    with json_output.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=True)
    print(f"Repricing summary written: {json_output}")

    if args.repricing_output:
        repricing_output = Path(args.repricing_output)
    else:
        repricing_output = _default_repricing_dataset_output(args.start_date, args.end_date)
    write_jsonl(analyses, repricing_output)
    print(f"Repricing dataset written: {repricing_output}")


if __name__ == "__main__":
    main()
