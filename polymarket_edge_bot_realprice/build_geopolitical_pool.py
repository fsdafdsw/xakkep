import argparse
import json
from collections import Counter
from pathlib import Path

from backtest import (
    _parse_date_to_ts,
    build_candidates,
    fetch_closed_events,
    find_total_closed_events,
)
from geopolitical_context import build_geopolitical_context
from research_dataset import resolve_dataset_output, write_jsonl


def _parse_offset_list(text):
    values = []
    for item in str(text or "").split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values


def _filter_events_to_geopolitical(events, min_match_score):
    filtered_events = []
    market_counter = Counter()
    action_counter = Counter()
    kept_markets = 0
    scanned_markets = 0

    for event in events:
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue

        kept = []
        for market in markets:
            scanned_markets += 1
            context = build_geopolitical_context(
                market.get("question"),
                event.get("title") or event.get("question"),
                event.get("description"),
                event.get("category"),
                event.get("resolutionSource"),
            )
            if not context["is_geopolitical"] or context["match_score"] < min_match_score:
                continue

            enriched = dict(market)
            enriched["geopolitical_context"] = context
            kept.append(enriched)
            kept_markets += 1
            market_counter[str(context["action_family"] or "generic_geo")] += 1
            for keyword in context["geo_keywords"]:
                action_counter[keyword] += 1

        if kept:
            cloned = dict(event)
            cloned["markets"] = kept
            filtered_events.append(cloned)

    return filtered_events, {
        "events_with_geopolitical_markets": len(filtered_events),
        "markets_scanned": scanned_markets,
        "markets_kept": kept_markets,
        "action_family_counts": dict(sorted(market_counter.items())),
        "top_geo_keywords": dict(action_counter.most_common(15)),
    }


def _fetch_events_for_offsets(offsets, max_events_fetch, page_size):
    combined = []
    seen_event_ids = set()
    fetched_meta = []
    for offset in offsets:
        print(f"Fetching closed events from offset {offset} (max {max_events_fetch})...")
        batch = fetch_closed_events(start_offset=offset, max_events=max_events_fetch, page_size=page_size)
        fetched_meta.append({"offset": offset, "event_count": len(batch)})
        for event in batch:
            event_id = str(event.get("id") or "")
            if event_id and event_id in seen_event_ids:
                continue
            if event_id:
                seen_event_ids.add(event_id)
            combined.append(event)
    return combined, fetched_meta


def parse_args():
    parser = argparse.ArgumentParser(description="Build a targeted geopolitical snapshot pool before fetching price history.")
    parser.add_argument("--start-date", default="2026-01-01", help="UTC date, e.g. 2026-01-01")
    parser.add_argument("--end-date", default="2026-03-01", help="UTC date, e.g. 2026-03-01")
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--start-offsets", default="", help="Comma-separated list of offsets to scan and merge.")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--lookback-events", type=int, default=50000)
    parser.add_argument("--max-events-fetch", type=int, default=50000)
    parser.add_argument("--max-candidate-markets", type=int, default=800)
    parser.add_argument("--max-history-requests", type=int, default=800)
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--min-match-score", type=float, default=1.8)
    parser.add_argument("--dataset-output", required=True, help="JSONL file or directory for the snapshot pool.")
    parser.add_argument("--summary-output", default=None, help="Optional JSON summary path.")
    parser.add_argument("--use-liquidity-filter", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    start_ts = _parse_date_to_ts(args.start_date)
    end_ts = _parse_date_to_ts(args.end_date) + (24 * 3600) - 1

    offset_list = _parse_offset_list(args.start_offsets)
    if offset_list:
        offsets = [max(0, value) for value in offset_list]
        events, fetched_meta = _fetch_events_for_offsets(
            offsets=offsets,
            max_events_fetch=args.max_events_fetch,
            page_size=args.page_size,
        )
        start_offset = offsets[0]
    elif args.start_offset is None:
        total_closed = find_total_closed_events()
        start_offset = max(0, total_closed - args.lookback_events)
        max_events = min(args.max_events_fetch, total_closed - start_offset)
        print(f"Fetching closed events from offset {start_offset} (max {max_events})...")
        events = fetch_closed_events(start_offset=start_offset, max_events=max_events, page_size=args.page_size)
        fetched_meta = [{"offset": start_offset, "event_count": len(events)}]
    else:
        start_offset = max(0, args.start_offset)
        print(f"Fetching closed events from offset {start_offset} (max {args.max_events_fetch})...")
        events = fetch_closed_events(start_offset=start_offset, max_events=args.max_events_fetch, page_size=args.page_size)
        fetched_meta = [{"offset": start_offset, "event_count": len(events)}]
    print(f"Fetched events: {len(events)}")

    geo_events, filter_summary = _filter_events_to_geopolitical(events, min_match_score=args.min_match_score)
    print(f"Geopolitical events kept: {filter_summary['events_with_geopolitical_markets']}")
    print(f"Geopolitical markets kept: {filter_summary['markets_kept']}")
    print(f"Action families: {filter_summary['action_family_counts']}")

    candidates, rejects, reasons, diagnostics, dataset_rows = build_candidates(
        events=geo_events,
        start_ts=start_ts,
        end_ts=end_ts,
        entry_hours_before_close=args.entry_hours_before_close,
        history_window_days=args.history_window_days,
        max_markets=args.max_candidate_markets,
        fidelity=args.history_fidelity,
        use_liquidity_filter=args.use_liquidity_filter,
        max_history_requests=args.max_history_requests,
    )

    dataset_path = resolve_dataset_output(args.dataset_output, args.start_date, args.end_date)
    write_jsonl(dataset_rows, dataset_path)
    print(f"Snapshot pool written: {dataset_path}")
    print(f"Dataset rows: {len(dataset_rows)} | Final candidates: {len(candidates)}")

    summary = {
        "period": {"start_date": args.start_date, "end_date": args.end_date},
        "parameters": {
            "start_offset": start_offset,
            "start_offsets": offset_list,
            "page_size": args.page_size,
            "max_events_fetch": args.max_events_fetch,
            "max_candidate_markets": args.max_candidate_markets,
            "max_history_requests": args.max_history_requests,
            "entry_hours_before_close": args.entry_hours_before_close,
            "history_window_days": args.history_window_days,
            "history_fidelity": args.history_fidelity,
            "min_match_score": args.min_match_score,
            "use_liquidity_filter": args.use_liquidity_filter,
        },
        "filter_summary": filter_summary,
        "fetch_summary": fetched_meta,
        "dataset_row_count": len(dataset_rows),
        "final_candidate_count": len(candidates),
        "rejects": rejects,
        "drop_reasons": reasons,
        "diagnostics": diagnostics,
        "top_questions": [row.get("question") for row in dataset_rows[:15]],
        "dataset_output": str(dataset_path),
    }

    if args.summary_output:
        summary_path = Path(args.summary_output)
    else:
        summary_path = dataset_path.with_suffix(".summary.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=True)
    print(f"Summary written: {summary_path}")


if __name__ == "__main__":
    main()
