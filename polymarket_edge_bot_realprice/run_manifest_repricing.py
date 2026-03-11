import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from types import SimpleNamespace

from build_ceasefire_manifest import _index_rows, _join_keys
from repricing_backtest import (
    _diplomacy_catalyst_leaderboard,
    _has_real_forward_history,
    _summarize_group,
    _uses_settlement_fallback,
    analyze_repricing,
)
from research_dataset import write_jsonl


def _load_jsonl(path):
    rows = []
    p = Path(path)
    if not p.exists():
        return rows
    with p.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _parse_csv_set(text):
    values = set()
    for item in str(text or "").split(","):
        item = item.strip()
        if not item or item.lower() == "any":
            continue
        values.add(item)
    return values


def _match_snapshot_rows(manifest_rows, snapshot_rows):
    snapshot_index = _index_rows(snapshot_rows)
    matched = []
    seen = set()
    for row in manifest_rows:
        for key in _join_keys(row):
            for candidate in snapshot_index.get(key) or []:
                snapshot_id = str(candidate.get("snapshot_id") or "")
                if snapshot_id and snapshot_id in seen:
                    continue
                if snapshot_id:
                    seen.add(snapshot_id)
                matched.append(candidate)
                break
            else:
                continue
            break
    return matched


def _summary(analyses, args):
    analyses_ok = [row for row in analyses if not row.get("error")]
    windows_days = [int(item) for item in str(args.windows_days).split(",") if item.strip()]
    take_profit_levels = [float(item) for item in str(args.take_profit_levels).split(",") if item.strip()]
    target_prices = [float(item) for item in str(args.target_prices).split(",") if item.strip()]
    conflict_runup_levels = [float(item) for item in str(args.conflict_runup_levels).split(",") if item.strip()]
    conflict_target_prices = [float(item) for item in str(args.conflict_target_prices).split(",") if item.strip()]

    overall = _summarize_group(
        analyses_ok,
        windows_days,
        take_profit_levels,
        target_prices,
        conflict_runup_levels,
        conflict_target_prices,
    )

    by_catalyst = defaultdict(list)
    for row in analyses_ok:
        by_catalyst[str(row.get("catalyst_type") or "unknown")].append(row)

    by_catalyst_summary = {
        key: _summarize_group(
            value,
            windows_days,
            take_profit_levels,
            target_prices,
            conflict_runup_levels,
            conflict_target_prices,
        )
        for key, value in sorted(by_catalyst.items())
    }

    diplomacy_leaderboard = _diplomacy_catalyst_leaderboard(
        analyses_ok,
        windows_days,
        take_profit_levels,
        target_prices,
        conflict_runup_levels,
        conflict_target_prices,
        limit=args.top_limit,
    )

    return {
        "selected_manifest_rows": args.selected_manifest_rows,
        "matched_snapshot_rows": args.matched_snapshot_rows,
        "analyzed_row_count": len(analyses_ok),
        "error_row_count": len([row for row in analyses if row.get("error")]),
        "history_quality_counts": {
            "real_forward": sum(1 for row in analyses_ok if _has_real_forward_history(row)),
            "settlement_fallback": sum(1 for row in analyses_ok if _uses_settlement_fallback(row)),
        },
        "repricing_verdict_counts": dict(sorted(Counter(str(row.get("repricing_verdict") or "unknown") for row in analyses_ok).items())),
        "overall": overall,
        "by_catalyst_type": by_catalyst_summary,
        "diplomacy_catalyst_leaderboard": diplomacy_leaderboard,
        "top_cases": sorted(
            analyses_ok,
            key=lambda row: (
                row.get("best_runup_pct") if row.get("best_runup_pct") is not None else float("-inf"),
                row.get("repricing_watch_score") or 0.0,
                row.get("repricing_score") or 0.0,
            ),
            reverse=True,
        )[: args.top_limit],
        "errors": [row for row in analyses if row.get("error")],
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Run a focused repricing pass for manifest-selected markets.")
    parser.add_argument("--manifest-input", required=True)
    parser.add_argument("--snapshot-input", required=True)
    parser.add_argument("--research-actions", default="run_repricing_backtest")
    parser.add_argument("--catalyst-type", default="any")
    parser.add_argument("--focused-snapshot-output", required=True)
    parser.add_argument("--repricing-output", required=True)
    parser.add_argument("--json-output", required=True)
    parser.add_argument("--domain-name", default="any")
    parser.add_argument("--market-type", default="any")
    parser.add_argument("--category-group", default="any")
    parser.add_argument("--action-family", default="any")
    parser.add_argument("--min-repricing-potential", type=float, default=0.0)
    parser.add_argument("--pre-entry-lookback-days", type=int, default=7)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--windows-days", default="3,7,14")
    parser.add_argument("--take-profit-levels", default="0.25,0.50")
    parser.add_argument("--target-prices", default="0.10,0.20")
    parser.add_argument("--conflict-runup-levels", default="0.10,0.25")
    parser.add_argument("--conflict-target-prices", default="0.40,0.60")
    parser.add_argument("--top-limit", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()
    action_set = _parse_csv_set(args.research_actions)
    manifest_rows = _load_jsonl(args.manifest_input)
    selected_manifest_rows = []
    for row in manifest_rows:
        if action_set and str(row.get("research_action") or "") not in action_set:
            continue
        if args.catalyst_type.lower() != "any" and str(row.get("catalyst_type") or "") != args.catalyst_type:
            continue
        selected_manifest_rows.append(row)

    snapshot_rows = _load_jsonl(args.snapshot_input)
    matched_snapshot_rows = _match_snapshot_rows(selected_manifest_rows, snapshot_rows)
    write_jsonl(matched_snapshot_rows, args.focused_snapshot_output)

    runtime_args = SimpleNamespace(
        domain_name=args.domain_name,
        market_type=args.market_type,
        category_group=args.category_group,
        action_family=args.action_family,
        catalyst_type=args.catalyst_type,
        min_repricing_potential=args.min_repricing_potential,
        pre_entry_lookback_days=args.pre_entry_lookback_days,
        history_fidelity=args.history_fidelity,
        windows_days=args.windows_days,
        take_profit_levels=args.take_profit_levels,
        target_prices=args.target_prices,
        conflict_runup_levels=args.conflict_runup_levels,
        conflict_target_prices=args.conflict_target_prices,
        top_limit=args.top_limit,
    )

    analyses, *_ = analyze_repricing(matched_snapshot_rows, runtime_args)
    write_jsonl(analyses, args.repricing_output)

    runtime_args.selected_manifest_rows = len(selected_manifest_rows)
    runtime_args.matched_snapshot_rows = len(matched_snapshot_rows)
    summary = {
        "inputs": {
            "manifest_input": args.manifest_input,
            "snapshot_input": args.snapshot_input,
            "research_actions": sorted(action_set),
            "catalyst_type": args.catalyst_type,
            "min_repricing_potential": args.min_repricing_potential,
        },
        "summary": _summary(analyses, runtime_args),
    }

    output_json = Path(args.json_output)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    with output_json.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=True)

    print(f"Focused snapshot rows: {len(matched_snapshot_rows)}")
    print(f"Focused repricing analyses: {summary['summary']['analyzed_row_count']}")
    print(f"History quality: {summary['summary']['history_quality_counts']}")
    print(f"Summary written: {output_json}")


if __name__ == "__main__":
    main()
