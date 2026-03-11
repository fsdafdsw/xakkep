import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from backtest import (
    _parse_date_to_ts,
    build_candidates,
    fetch_closed_events,
    find_total_closed_events,
)
from geopolitical_context import build_geopolitical_context, normalize_text
from research_dataset import resolve_dataset_output, write_jsonl


_DEFAULT_RELEASE_CATALYST_TYPES = {
    "release",
    "hostage_release",
    "appeal",
    "hearing",
    "court_ruling",
}
_DEFAULT_RELEASE_DISCOVERY_KEYWORDS = (
    "jimmy lai",
    "julian assange",
    "osman kavala",
    "hostage",
    "hostages",
    "release",
    "released",
    "appeal",
    "appeals court",
    "hearing",
    "court",
    "tribunal",
    "extradite",
    "extradited",
    "extradition",
    "parole",
    "amnesty",
    "clemency",
    "prisoner",
    "prisoners",
    "detained",
    "detention",
    "custody",
    "bail",
    "prisoner swap",
    "hostage swap",
    "hostage deal",
    "hostage exchange",
    "national security law",
)
_DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS = (
    "stock exchange",
    "market open",
    "open for trading",
    "open for stock trading",
    "trading by",
    "earnings",
    "quarterly earnings",
    "revenue",
    "eps",
)


def _parse_offset_list(text):
    values = []
    for item in str(text or "").split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values


def _parse_csv_set(text):
    values = set()
    for item in str(text or "").split(","):
        item = item.strip().lower()
        if not item or item == "any":
            continue
        values.add(item)
    return values


def _parse_csv_list(text):
    values = []
    for item in str(text or "").split(","):
        item = item.strip().lower()
        if not item or item == "any":
            continue
        values.append(item)
    return values


def _to_unix(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        return None


def _date_label(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")


def _resolve_manifest_output(path_or_dir, start_date, end_date):
    if not path_or_dir or str(path_or_dir).strip().lower() == "auto":
        return Path("reports") / "research" / f"matched_release_markets_{start_date}_{end_date}.jsonl"

    path = Path(path_or_dir)
    if path.suffix.lower() == ".jsonl":
        return path
    return path / f"matched_release_markets_{start_date}_{end_date}.jsonl"


def _build_offset_list(args):
    offset_list = _parse_offset_list(args.start_offsets)
    if offset_list:
        return [max(0, value) for value in offset_list]

    if args.offset_range_start is not None and args.offset_range_end is not None:
        step = max(1, args.offset_range_step)
        start = max(0, min(args.offset_range_start, args.offset_range_end))
        end = max(args.offset_range_start, args.offset_range_end)
        return list(range(start, end + 1, step))

    return []


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


def _infer_release_event_window(events):
    timestamps = []
    for event in events:
        event_end = _to_unix(event.get("endDate"))
        if event_end is not None:
            timestamps.append(event_end)
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue
        for market in markets:
            market_end = _to_unix(market.get("endDate") or event.get("endDate"))
            if market_end is not None:
                timestamps.append(market_end)
    if not timestamps:
        return None, None
    return min(timestamps), max(timestamps)


def _build_release_manifest_row(event, market):
    context = market.get("geopolitical_context") or {}
    release_context = market.get("release_context") or {}
    return {
        "event_id": str(event.get("id") or ""),
        "event_slug": event.get("slug"),
        "event_title": event.get("title") or event.get("question"),
        "event_end_date": event.get("endDate"),
        "event_category": event.get("category"),
        "resolution_source": event.get("resolutionSource"),
        "market_id": str(market.get("id") or ""),
        "market_slug": market.get("slug"),
        "question": market.get("question"),
        "market_end_date": market.get("endDate") or event.get("endDate"),
        "outcomes": market.get("outcomes"),
        "outcome_prices": market.get("outcomePrices"),
        "clob_token_ids": market.get("clobTokenIds"),
        "discovery_hits": market.get("_release_discovery_hits") or [],
        "release_score": release_context.get("release_score"),
        "action_family": context.get("action_family"),
        "catalyst_type": context.get("catalyst_type"),
        "catalyst_family": context.get("catalyst_family"),
        "catalyst_strength": context.get("catalyst_strength"),
        "catalyst_has_official_source": context.get("catalyst_has_official_source"),
        "question_geo_keywords": context.get("question_geo_keywords") or [],
        "release_context_keywords": context.get("release_context_keywords") or [],
        "release_figure_keywords": context.get("release_figure_keywords") or [],
        "institution_keywords": context.get("institution_keywords") or [],
        "quote_market": context.get("quote_market"),
        "match_score": context.get("match_score"),
    }


def _keyword_hits(text, keywords):
    normalized = normalize_text(text)
    hits = []
    for keyword in keywords:
        if keyword in normalized:
            hits.append(keyword)
    return hits


def _coarse_keyword_prefilter_events(events, discovery_keywords, exclusion_keywords):
    if not discovery_keywords:
        return events, {
            "enabled": False,
            "events_kept": len(events),
            "markets_kept": sum(len(event.get("markets") or []) for event in events if isinstance(event.get("markets"), list)),
            "top_keywords": {},
            "top_exclusions": {},
        }

    filtered_events = []
    keyword_counter = Counter()
    exclusion_counter = Counter()
    kept_markets = 0

    for event in events:
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue

        event_title = event.get("title") or event.get("question") or ""
        event_description = event.get("description") or ""
        event_category = event.get("category") or ""
        resolution_source = event.get("resolutionSource") or ""

        kept = []
        for market in markets:
            market_text = normalize_text(
                market.get("question"),
                event_title,
                event_description,
                event_category,
                resolution_source,
            )
            exclusion_hits = _keyword_hits(market_text, exclusion_keywords)
            if exclusion_hits:
                for hit in exclusion_hits:
                    exclusion_counter[hit] += 1
                continue
            hits = _keyword_hits(market_text, discovery_keywords)
            if not hits:
                continue
            enriched = dict(market)
            enriched["_release_discovery_hits"] = hits
            kept.append(enriched)
            kept_markets += 1
            for hit in hits:
                keyword_counter[hit] += 1

        if kept:
            cloned = dict(event)
            cloned["markets"] = kept
            filtered_events.append(cloned)

    return filtered_events, {
        "enabled": True,
        "events_kept": len(filtered_events),
        "markets_kept": kept_markets,
        "top_keywords": dict(keyword_counter.most_common(20)),
        "top_exclusions": dict(exclusion_counter.most_common(20)),
    }


def _release_score(context, catalyst):
    score = float(context.get("match_score") or 0.0)
    release_context = context.get("release_context_keywords") or []
    release_figures = context.get("release_figure_keywords") or []
    question_geo = context.get("question_geo_keywords") or []

    if release_context:
        score += min(0.45, len(release_context) * 0.10)
    if release_figures:
        score += min(0.60, len(release_figures) * 0.30)
    if question_geo:
        score += min(0.30, len(question_geo) * 0.08)
    if catalyst.get("has_official_source"):
        score += 0.10
    if catalyst.get("catalyst_type") in {"appeal", "hearing", "court_ruling", "hostage_release"}:
        score += 0.15
    return score


def _is_targeted_release_candidate(context, catalyst, min_match_score, allowed_catalyst_types):
    if context.get("quote_market"):
        return False, 0.0

    catalyst_type = str(catalyst.get("catalyst_type") or "").lower()
    catalyst_family = str(catalyst.get("catalyst_family") or "").lower()
    action_family = str(context.get("action_family") or "").lower()

    if allowed_catalyst_types and catalyst_type not in allowed_catalyst_types:
        return False, 0.0

    release_like = action_family == "release" or catalyst_family == "release"
    if not release_like:
        return False, 0.0

    release_figures = context.get("release_figure_keywords") or []
    release_context = context.get("release_context_keywords") or []
    question_geo = context.get("question_geo_keywords") or []
    institution_keywords = context.get("institution_keywords") or []

    eligible = False
    if context.get("is_geopolitical") and action_family == "release":
        eligible = True
    elif release_figures:
        eligible = True
    elif question_geo and release_context:
        eligible = True
    elif institution_keywords and release_context and catalyst_type in _DEFAULT_RELEASE_CATALYST_TYPES:
        eligible = True
    elif catalyst_type == "hostage_release" and question_geo:
        eligible = True

    score = _release_score(context, catalyst)
    return eligible and score >= min_match_score, score


def _filter_events_to_release(events, min_match_score, allowed_catalyst_types):
    filtered_events = []
    manifest_rows = []
    kept_markets = 0
    scanned_markets = 0
    action_counter = Counter()
    catalyst_counter = Counter()
    figure_counter = Counter()
    geo_counter = Counter()

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
            catalyst = {
                "catalyst_type": context.get("catalyst_type"),
                "catalyst_family": context.get("catalyst_family"),
                "has_official_source": context.get("catalyst_has_official_source"),
            }
            keep, release_score = _is_targeted_release_candidate(
                context=context,
                catalyst=catalyst,
                min_match_score=min_match_score,
                allowed_catalyst_types=allowed_catalyst_types,
            )
            if not keep:
                continue

            enriched = dict(market)
            enriched["geopolitical_context"] = context
            enriched["release_context"] = {
                "release_score": release_score,
                "catalyst_type": catalyst.get("catalyst_type"),
                "catalyst_family": catalyst.get("catalyst_family"),
            }
            kept.append(enriched)
            manifest_rows.append(_build_release_manifest_row(event, enriched))
            kept_markets += 1
            action_counter[str(context.get("action_family") or "generic_release")] += 1
            catalyst_counter[str(catalyst.get("catalyst_type") or "generic")] += 1
            for figure in context.get("release_figure_keywords") or []:
                figure_counter[figure] += 1
            for keyword in context.get("question_geo_keywords") or []:
                geo_counter[keyword] += 1

        if kept:
            cloned = dict(event)
            cloned["markets"] = kept
            filtered_events.append(cloned)

    return filtered_events, manifest_rows, {
        "events_with_release_markets": len(filtered_events),
        "markets_scanned": scanned_markets,
        "markets_kept": kept_markets,
        "action_family_counts": dict(sorted(action_counter.items())),
        "catalyst_type_counts": dict(sorted(catalyst_counter.items())),
        "top_release_figures": dict(figure_counter.most_common(15)),
        "top_question_geo_keywords": dict(geo_counter.most_common(15)),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Build a targeted legal/release snapshot pool before fetching price history.")
    parser.add_argument("--start-date", default="2026-01-01", help="UTC date, e.g. 2026-01-01")
    parser.add_argument("--end-date", default="2026-03-01", help="UTC date, e.g. 2026-03-01")
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--start-offsets", default="", help="Comma-separated list of offsets to scan and merge.")
    parser.add_argument("--offset-range-start", type=int, default=None, help="Optional first offset for automatic sweep.")
    parser.add_argument("--offset-range-end", type=int, default=None, help="Optional last offset for automatic sweep.")
    parser.add_argument("--offset-range-step", type=int, default=20000, help="Step for automatic offset sweep.")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--lookback-events", type=int, default=50000)
    parser.add_argument("--max-events-fetch", type=int, default=50000)
    parser.add_argument("--max-candidate-markets", type=int, default=500)
    parser.add_argument("--max-history-requests", type=int, default=500)
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--min-match-score", type=float, default=1.8)
    parser.add_argument(
        "--allowed-catalyst-types",
        default="release,hostage_release,appeal,hearing,court_ruling",
        help="Comma-separated catalyst types to keep. Use 'any' to disable.",
    )
    parser.add_argument(
        "--discovery-keywords",
        default=",".join(_DEFAULT_RELEASE_DISCOVERY_KEYWORDS),
        help="Comma-separated coarse keywords used before full release matching. Use 'any' to disable.",
    )
    parser.add_argument(
        "--discovery-exclusion-keywords",
        default=",".join(_DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS),
        help="Comma-separated coarse exclusion keywords applied before full release matching. Use 'any' to disable.",
    )
    parser.add_argument(
        "--align-window-to-discovered-events",
        action="store_true",
        help="Use the min/max endDate of discovered release events instead of the requested date window.",
    )
    parser.add_argument("--dataset-output", required=True, help="JSONL file or directory for the snapshot pool.")
    parser.add_argument("--manifest-output", default="auto", help="JSONL file or directory for matched raw release markets.")
    parser.add_argument("--summary-output", default=None, help="Optional JSON summary path.")
    parser.add_argument("--use-liquidity-filter", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    start_ts = _parse_date_to_ts(args.start_date)
    end_ts = _parse_date_to_ts(args.end_date) + (24 * 3600) - 1
    allowed_catalyst_types = _parse_csv_set(args.allowed_catalyst_types)
    discovery_keywords = _parse_csv_list(args.discovery_keywords)
    discovery_exclusion_keywords = _parse_csv_list(args.discovery_exclusion_keywords)

    offset_list = _build_offset_list(args)
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

    coarse_events, coarse_summary = _coarse_keyword_prefilter_events(
        events,
        discovery_keywords,
        discovery_exclusion_keywords,
    )
    print(f"Coarse keyword-prefilter events kept: {coarse_summary['events_kept']}")
    print(f"Coarse keyword-prefilter markets kept: {coarse_summary['markets_kept']}")

    release_events, manifest_rows, filter_summary = _filter_events_to_release(
        events=coarse_events,
        min_match_score=args.min_match_score,
        allowed_catalyst_types=allowed_catalyst_types,
    )
    print(f"Release events kept: {filter_summary['events_with_release_markets']}")
    print(f"Release markets kept: {filter_summary['markets_kept']}")
    print(f"Catalyst types: {filter_summary['catalyst_type_counts']}")

    applied_start_ts = start_ts
    applied_end_ts = end_ts
    inferred_start_ts, inferred_end_ts = _infer_release_event_window(release_events)
    if args.align_window_to_discovered_events and inferred_start_ts is not None and inferred_end_ts is not None:
        applied_start_ts = inferred_start_ts
        applied_end_ts = inferred_end_ts
        print(
            "Applied event-anchored window: "
            f"{_date_label(applied_start_ts)} .. {_date_label(applied_end_ts)}"
        )

    candidates, rejects, reasons, diagnostics, dataset_rows = build_candidates(
        events=release_events,
        start_ts=applied_start_ts,
        end_ts=applied_end_ts,
        entry_hours_before_close=args.entry_hours_before_close,
        history_window_days=args.history_window_days,
        max_markets=args.max_candidate_markets,
        fidelity=args.history_fidelity,
        use_liquidity_filter=args.use_liquidity_filter,
        max_history_requests=args.max_history_requests,
    )

    output_start_date = _date_label(applied_start_ts)
    output_end_date = _date_label(applied_end_ts)
    dataset_path = resolve_dataset_output(args.dataset_output, output_start_date, output_end_date)
    write_jsonl(dataset_rows, dataset_path)
    print(f"Release snapshot pool written: {dataset_path}")
    print(f"Dataset rows: {len(dataset_rows)} | Final candidates: {len(candidates)}")

    manifest_path = _resolve_manifest_output(args.manifest_output, output_start_date, output_end_date)
    write_jsonl(manifest_rows, manifest_path)
    print(f"Release matched-manifest written: {manifest_path}")
    print(f"Matched release markets: {len(manifest_rows)}")

    summary = {
        "period": {"start_date": args.start_date, "end_date": args.end_date},
        "applied_period": {
            "start_date": _date_label(applied_start_ts),
            "end_date": _date_label(applied_end_ts),
            "aligned_to_discovered_events": bool(args.align_window_to_discovered_events and inferred_start_ts is not None and inferred_end_ts is not None),
        },
        "parameters": {
            "start_offset": start_offset,
            "start_offsets": offset_list,
            "offset_range_start": args.offset_range_start,
            "offset_range_end": args.offset_range_end,
            "offset_range_step": args.offset_range_step,
            "page_size": args.page_size,
            "max_events_fetch": args.max_events_fetch,
            "max_candidate_markets": args.max_candidate_markets,
            "max_history_requests": args.max_history_requests,
            "entry_hours_before_close": args.entry_hours_before_close,
            "history_window_days": args.history_window_days,
            "history_fidelity": args.history_fidelity,
            "min_match_score": args.min_match_score,
            "allowed_catalyst_types": sorted(allowed_catalyst_types),
            "discovery_keywords": discovery_keywords,
            "discovery_exclusion_keywords": discovery_exclusion_keywords,
            "align_window_to_discovered_events": args.align_window_to_discovered_events,
            "use_liquidity_filter": args.use_liquidity_filter,
        },
        "coarse_prefilter": coarse_summary,
        "filter_summary": filter_summary,
        "fetch_summary": fetched_meta,
        "manifest_row_count": len(manifest_rows),
        "manifest_output": str(manifest_path),
        "dataset_row_count": len(dataset_rows),
        "final_candidate_count": len(candidates),
        "rejects": rejects,
        "drop_reasons": reasons,
        "diagnostics": diagnostics,
        "top_questions": [row.get("question") for row in dataset_rows[:15]],
        "top_manifest_questions": [row.get("question") for row in manifest_rows[:15]],
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
