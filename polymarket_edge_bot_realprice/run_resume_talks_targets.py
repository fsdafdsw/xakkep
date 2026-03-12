import argparse
import json
from pathlib import Path
from types import SimpleNamespace

from backtest import _parse_date_to_ts, build_candidates
from build_diplomacy_pool import _date_label, _filter_events_by_end_date_floor, _infer_event_window, _to_unix
from build_resume_talks_pool import (
    _DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS,
    _DEFAULT_RESUME_DISCOVERY_KEYWORDS,
    _coarse_keyword_prefilter_events,
    _fetch_events_by_slugs,
    _filter_events_to_resume_talks,
    _parse_csv_list,
    _parse_csv_set,
)
from repricing_backtest import (
    _diplomacy_catalyst_leaderboard,
    _meeting_subtype_leaderboard,
    _resume_talks_leaderboard,
    _summarize_group,
    analyze_repricing,
)
from research_dataset import resolve_dataset_output, write_jsonl


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


def _select_manifest_rows(rows, allowed_actions, explicit_event_slugs, top_limit):
    selected = []
    seen_slugs = set()

    ordered = sorted(
        rows,
        key=lambda row: (
            -(float(row.get("priority_score") or 0.0)),
            -(float(row.get("best_runup_pct") or 0.0)),
            -(float(row.get("match_score") or 0.0)),
        ),
    )

    for row in ordered:
        if str(row.get("meeting_subtype") or "") != "resume_talks":
            continue
        event_slug = str(row.get("event_slug") or "").strip().lower()
        if not event_slug:
            continue
        if explicit_event_slugs and event_slug not in explicit_event_slugs:
            continue
        if allowed_actions and str(row.get("research_action") or "") not in allowed_actions:
            continue
        if event_slug in seen_slugs:
            continue
        selected.append(row)
        seen_slugs.add(event_slug)
        if top_limit and len(selected) >= top_limit:
            break

    return selected


def _repricing_summary(analyses, args):
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
    return {
        "analyzed_row_count": len(analyses_ok),
        "overall": overall,
        "diplomacy_catalyst_leaderboard": _diplomacy_catalyst_leaderboard(
            analyses_ok,
            windows_days,
            take_profit_levels,
            target_prices,
            conflict_runup_levels,
            conflict_target_prices,
            limit=args.top_limit,
        ),
        "meeting_subtype_leaderboard": _meeting_subtype_leaderboard(
            analyses_ok,
            windows_days,
            take_profit_levels,
            target_prices,
            conflict_runup_levels,
            conflict_target_prices,
            limit=args.top_limit,
        ),
        "resume_talks_leaderboard": _resume_talks_leaderboard(
            analyses_ok,
            windows_days,
            take_profit_levels,
            target_prices,
            conflict_runup_levels,
            conflict_target_prices,
            limit=args.top_limit,
        ),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Build focused resume-talks snapshot/repricing bundles from a manifest-selected slug list.")
    parser.add_argument("--manifest-input", required=True)
    parser.add_argument("--event-slugs", default="")
    parser.add_argument("--research-actions", default="ready_for_selector_tuning,rebuild_snapshot_window,run_repricing_backtest")
    parser.add_argument("--top-limit", type=int, default=10)
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--max-candidate-markets", type=int, default=120)
    parser.add_argument("--max-history-requests", type=int, default=120)
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--min-match-score", type=float, default=1.55)
    parser.add_argument("--coarse-min-score", type=float, default=1.10)
    parser.add_argument("--align-window-padding-days", type=int, default=21)
    parser.add_argument("--min-resume-end-date", default="2025-06-01")
    parser.add_argument("--dataset-output", required=True)
    parser.add_argument("--selected-manifest-output", required=True)
    parser.add_argument("--discovered-manifest-output", required=True)
    parser.add_argument("--repricing-output", required=True)
    parser.add_argument("--repricing-json-output", required=True)
    parser.add_argument("--summary-output", required=True)
    parser.add_argument("--domain-name", default="geopolitical_repricing")
    parser.add_argument("--market-type", default="any")
    parser.add_argument("--category-group", default="any")
    parser.add_argument("--action-family", default="diplomacy")
    parser.add_argument("--catalyst-type", default="any")
    parser.add_argument("--min-repricing-potential", type=float, default=0.60)
    parser.add_argument("--pre-entry-lookback-days", type=int, default=7)
    parser.add_argument("--windows-days", default="3,7,14")
    parser.add_argument("--take-profit-levels", default="0.25,0.50")
    parser.add_argument("--target-prices", default="0.10,0.20")
    parser.add_argument("--conflict-runup-levels", default="0.10,0.25")
    parser.add_argument("--conflict-target-prices", default="0.40,0.60")
    return parser.parse_args()


def main():
    args = parse_args()
    manifest_rows = _load_jsonl(args.manifest_input)
    explicit_event_slugs = _parse_csv_set(args.event_slugs)
    allowed_actions = _parse_csv_set(args.research_actions)

    selected_rows = _select_manifest_rows(
        manifest_rows,
        allowed_actions=allowed_actions,
        explicit_event_slugs=explicit_event_slugs,
        top_limit=args.top_limit,
    )
    selected_event_slugs = [str(row.get("event_slug") or "").strip().lower() for row in selected_rows if row.get("event_slug")]
    write_jsonl(selected_rows, args.selected_manifest_output)

    events, fetched_meta = _fetch_events_by_slugs(selected_event_slugs, page_size=args.page_size)
    discovery_keywords = _parse_csv_list(",".join(_DEFAULT_RESUME_DISCOVERY_KEYWORDS))
    discovery_exclusion_keywords = _parse_csv_list(",".join(_DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS))
    coarse_events, coarse_summary = _coarse_keyword_prefilter_events(
        events,
        discovery_keywords,
        discovery_exclusion_keywords,
        args.coarse_min_score,
    )
    resume_events, discovered_manifest_rows, filter_summary = _filter_events_to_resume_talks(
        coarse_events,
        min_match_score=args.min_match_score,
    )

    min_end_ts = _to_unix(f"{args.min_resume_end_date}T00:00:00+00:00") if args.min_resume_end_date else None
    resume_events, discovered_manifest_rows = _filter_events_by_end_date_floor(resume_events, discovered_manifest_rows, min_end_ts)

    min_event_ts, max_event_ts = _infer_event_window(resume_events)
    if min_event_ts is None or max_event_ts is None:
        start_date = ""
        end_date = ""
        dataset_rows = []
        candidates = []
        rejects = {}
        reasons = {}
        diagnostics = {}
    else:
        padding = args.align_window_padding_days * 24 * 3600
        start_ts = max(0, min_event_ts - padding)
        end_ts = max_event_ts + padding
        start_date = _date_label(start_ts)
        end_date = _date_label(end_ts)
        candidates, rejects, reasons, diagnostics, dataset_rows = build_candidates(
            events=resume_events,
            start_ts=start_ts,
            end_ts=end_ts,
            entry_hours_before_close=args.entry_hours_before_close,
            history_window_days=args.history_window_days,
            max_markets=args.max_candidate_markets,
            fidelity=args.history_fidelity,
            use_liquidity_filter=False,
            max_history_requests=args.max_history_requests,
            skip_base_filters=True,
            skip_score_filters=True,
        )

    discovered_manifest_path = Path(args.discovered_manifest_output)
    discovered_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(discovered_manifest_rows, discovered_manifest_path)

    if start_date and end_date:
        dataset_path = resolve_dataset_output(args.dataset_output, start_date, end_date)
        write_jsonl(dataset_rows, dataset_path)
    else:
        dataset_path = Path(args.dataset_output)
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        write_jsonl([], dataset_path if dataset_path.suffix == ".jsonl" else dataset_path / "snapshots_empty.jsonl")
        if dataset_path.suffix != ".jsonl":
            dataset_path = dataset_path / "snapshots_empty.jsonl"

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
    analyses, *_ = analyze_repricing(dataset_rows, runtime_args)
    write_jsonl(analyses, args.repricing_output)

    summary = {
        "inputs": {
            "manifest_input": args.manifest_input,
            "research_actions": sorted(allowed_actions),
            "explicit_event_slugs": sorted(explicit_event_slugs),
        },
        "selected_event_slugs": selected_event_slugs,
        "selected_manifest_count": len(selected_rows),
        "fetched_event_count": len(events),
        "period": {"start_date": start_date, "end_date": end_date},
        "fetch_summary": fetched_meta,
        "coarse_prefilter": coarse_summary,
        "filter_summary": filter_summary,
        "discovered_manifest_count": len(discovered_manifest_rows),
        "dataset_row_count": len(dataset_rows),
        "final_candidate_count": len(candidates),
        "rejects": rejects,
        "drop_reasons": reasons,
        "diagnostics": diagnostics,
        "repricing": _repricing_summary(analyses, runtime_args),
        "dataset_output": str(dataset_path),
        "selected_manifest_output": args.selected_manifest_output,
        "discovered_manifest_output": args.discovered_manifest_output,
        "repricing_output": args.repricing_output,
    }

    summary_path = Path(args.summary_output)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=True)

    repricing_json_path = Path(args.repricing_json_output)
    repricing_json_path.parent.mkdir(parents=True, exist_ok=True)
    with repricing_json_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "selected_event_slugs": selected_event_slugs,
                "repricing": summary["repricing"],
            },
            fh,
            indent=2,
            ensure_ascii=True,
        )

    print(f"Selected manifest rows: {len(selected_rows)}")
    print(f"Selected event slugs: {len(selected_event_slugs)}")
    print(f"Dataset rows: {len(dataset_rows)}")
    print(f"Repricing analyses: {summary['repricing']['analyzed_row_count']}")
    print(f"Summary written: {summary_path}")


if __name__ == "__main__":
    main()
