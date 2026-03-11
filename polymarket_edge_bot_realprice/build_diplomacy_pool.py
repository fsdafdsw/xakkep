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


_DEFAULT_DIPLOMACY_CATALYST_TYPES = {
    "ceasefire",
    "negotiation",
    "call_or_meeting",
    "summit",
}
_DEFAULT_DIPLOMACY_DISCOVERY_KEYWORDS = (
    "ceasefire",
    "truce",
    "peace talks",
    "peace deal",
    "peace accord",
    "peace agreement",
    "humanitarian pause",
    "pause in fighting",
    "halt fighting",
    "stop fighting",
    "end hostilities",
    "hostilities end",
    "armistice",
    "de-escalation",
    "de-escalate",
    "negotiation",
    "negotiations",
    "talk to",
    "talks with",
    "talk with",
    "meet with",
    "meeting with",
    "call with",
    "phone call",
    "summit",
    "speak with",
)
_DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS = (
    "earnings call",
    "conference call",
    "investor call",
    "quarterly earnings",
    "earnings",
    "podcast",
    "joe rogan",
    "episode",
    "said during",
    "say during",
    "wef address",
)
_STRONG_DIPLOMACY_DISCOVERY_KEYWORDS = {
    "ceasefire",
    "truce",
    "peace talks",
    "peace deal",
    "peace accord",
    "peace agreement",
    "humanitarian pause",
    "pause in fighting",
    "halt fighting",
    "stop fighting",
    "end hostilities",
    "hostilities end",
    "armistice",
    "de-escalation",
    "de-escalate",
    "negotiation",
    "negotiations",
    "summit",
}
_MEETING_DISCOVERY_KEYWORDS = {
    "talk to",
    "talks with",
    "talk with",
    "meet with",
    "meeting with",
    "call with",
    "phone call",
    "speak with",
}


def _parse_offset_list(text):
    values = []
    for item in str(text or "").split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values


def _parse_csv_list(text):
    values = []
    for item in str(text or "").split(","):
        item = item.strip().lower()
        if not item or item == "any":
            continue
        values.append(item)
    return values


def _parse_csv_set(text):
    return set(_parse_csv_list(text))


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
        return Path("reports") / "research" / f"matched_diplomacy_markets_{start_date}_{end_date}.jsonl"

    path = Path(path_or_dir)
    if path.suffix.lower() == ".jsonl":
        return path
    return path / f"matched_diplomacy_markets_{start_date}_{end_date}.jsonl"


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


def _infer_event_window(events):
    timestamps = []
    for event in events:
        event_end = _to_unix(event.get("endDate"))
        if event_end is not None:
            timestamps.append(event_end)
        for market in event.get("markets") or []:
            market_end = _to_unix(market.get("endDate") or event.get("endDate"))
            if market_end is not None:
                timestamps.append(market_end)
    if not timestamps:
        return None, None
    return min(timestamps), max(timestamps)


def _filter_events_by_end_date_floor(events, manifest_rows, min_end_ts):
    if min_end_ts is None:
        return events, manifest_rows

    kept_events = []
    kept_manifest = []
    for event in events:
        kept_markets = []
        for market in event.get("markets") or []:
            market_end_ts = _to_unix(market.get("endDate") or event.get("endDate"))
            if market_end_ts is None or market_end_ts < min_end_ts:
                continue
            kept_markets.append(market)
        if kept_markets:
            cloned = dict(event)
            cloned["markets"] = kept_markets
            kept_events.append(cloned)

    for row in manifest_rows:
        row_end_ts = _to_unix(row.get("market_end_date") or row.get("event_end_date"))
        if row_end_ts is not None and row_end_ts >= min_end_ts:
            kept_manifest.append(row)
    return kept_events, kept_manifest


def _keyword_hits(text, keywords):
    normalized = normalize_text(text)
    hits = []
    for keyword in keywords:
        if keyword in normalized:
            hits.append(keyword)
    return hits


def _coarse_diplomacy_market_score(question_text, context_text, discovery_keywords):
    question_hits = _keyword_hits(question_text, discovery_keywords)
    context_hits = [hit for hit in _keyword_hits(context_text, discovery_keywords) if hit not in question_hits]

    strong_hits = [hit for hit in question_hits + context_hits if hit in _STRONG_DIPLOMACY_DISCOVERY_KEYWORDS]
    meeting_hits = [hit for hit in question_hits + context_hits if hit in _MEETING_DISCOVERY_KEYWORDS]

    score = 0.0
    score += min(2.2, len(question_hits) * 0.85)
    score += min(0.8, len(context_hits) * 0.20)
    if strong_hits:
        score += 0.65
    if meeting_hits:
        score += 0.35
    if strong_hits and meeting_hits:
        score += 0.45
    if any(
        hit in {
            "ceasefire",
            "truce",
            "peace talks",
            "peace deal",
            "peace accord",
            "peace agreement",
            "humanitarian pause",
            "pause in fighting",
            "halt fighting",
            "stop fighting",
            "end hostilities",
            "hostilities end",
            "armistice",
            "de-escalation",
            "de-escalate",
        }
        for hit in strong_hits
    ):
        score += 0.35
    return score, question_hits, context_hits


def _coarse_keyword_prefilter_events(events, discovery_keywords, exclusion_keywords, min_coarse_score):
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
    coarse_score_distribution = []
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
            question_text = normalize_text(market.get("question"))
            context_text = normalize_text(event_title, event_description, event_category, resolution_source)
            market_text = normalize_text(question_text, context_text)
            exclusion_hits = _keyword_hits(market_text, exclusion_keywords)
            if exclusion_hits:
                for hit in exclusion_hits:
                    exclusion_counter[hit] += 1
                continue

            coarse_score, question_hits, context_hits = _coarse_diplomacy_market_score(
                question_text,
                context_text,
                discovery_keywords,
            )
            hits = question_hits + [hit for hit in context_hits if hit not in question_hits]
            if not hits or coarse_score < min_coarse_score:
                continue

            enriched = dict(market)
            enriched["_diplomacy_discovery_hits"] = hits
            enriched["_diplomacy_question_hits"] = question_hits
            enriched["_diplomacy_context_hits"] = context_hits
            enriched["_diplomacy_discovery_score"] = coarse_score
            kept.append(enriched)
            kept_markets += 1
            coarse_score_distribution.append(coarse_score)
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
        "coarse_score": {
            "count": len(coarse_score_distribution),
            "max": max(coarse_score_distribution) if coarse_score_distribution else None,
            "mean": (sum(coarse_score_distribution) / len(coarse_score_distribution)) if coarse_score_distribution else None,
        },
    }


def _diplomacy_score(context):
    score = float(context.get("match_score") or 0.0)
    geo = context.get("question_geo_keywords") or []
    action = context.get("action_keywords") or []
    institutions = context.get("institution_keywords") or []
    if geo:
        score += min(0.35, len(geo) * 0.08)
    if action:
        score += min(0.35, len(action) * 0.08)
    if institutions:
        score += min(0.12, len(institutions) * 0.03)
    if context.get("catalyst_has_official_source"):
        score += 0.10
    if str(context.get("catalyst_type") or "") == "ceasefire":
        score += 0.20
    return score


def _is_targeted_diplomacy_candidate(context, min_match_score, allowed_catalyst_types):
    if context.get("quote_market"):
        return False, 0.0

    action_family = str(context.get("action_family") or "").lower()
    catalyst_family = str(context.get("catalyst_family") or "").lower()
    catalyst_type = str(context.get("catalyst_type") or "").lower()

    if allowed_catalyst_types and catalyst_type not in allowed_catalyst_types:
        return False, 0.0

    diplomacy_like = action_family == "diplomacy" or catalyst_family == "diplomacy"
    if not diplomacy_like:
        return False, 0.0

    score = _diplomacy_score(context)
    return score >= min_match_score, score


def _build_manifest_row(event, market):
    context = market.get("geopolitical_context") or {}
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
        "discovery_hits": market.get("_diplomacy_discovery_hits") or [],
        "question_discovery_hits": market.get("_diplomacy_question_hits") or [],
        "context_discovery_hits": market.get("_diplomacy_context_hits") or [],
        "discovery_score": market.get("_diplomacy_discovery_score"),
        "action_family": context.get("action_family"),
        "catalyst_type": context.get("catalyst_type"),
        "catalyst_family": context.get("catalyst_family"),
        "catalyst_strength": context.get("catalyst_strength"),
        "catalyst_has_official_source": context.get("catalyst_has_official_source"),
        "question_geo_keywords": context.get("question_geo_keywords") or [],
        "action_keywords": context.get("action_keywords") or [],
        "institution_keywords": context.get("institution_keywords") or [],
        "quote_market": context.get("quote_market"),
        "match_score": context.get("match_score"),
        "diplomacy_score": market.get("_diplomacy_score"),
    }


def _filter_events_to_diplomacy(events, min_match_score, allowed_catalyst_types):
    filtered_events = []
    manifest_rows = []
    kept_markets = 0
    scanned_markets = 0
    action_counter = Counter()
    catalyst_counter = Counter()
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
            ok, score = _is_targeted_diplomacy_candidate(context, min_match_score, allowed_catalyst_types)
            if not ok:
                continue

            enriched = dict(market)
            enriched["geopolitical_context"] = context
            enriched["_diplomacy_score"] = score
            kept.append(enriched)
            kept_markets += 1
            action_counter[str(context.get("action_family") or "generic")] += 1
            catalyst_counter[str(context.get("catalyst_type") or "unknown")] += 1
            for keyword in context.get("question_geo_keywords") or []:
                geo_counter[keyword] += 1
            manifest_rows.append(_build_manifest_row(event, enriched))

        if kept:
            cloned = dict(event)
            cloned["markets"] = kept
            filtered_events.append(cloned)

    return filtered_events, manifest_rows, {
        "events_kept": len(filtered_events),
        "markets_scanned": scanned_markets,
        "markets_kept": kept_markets,
        "action_family_counts": dict(sorted(action_counter.items())),
        "catalyst_type_counts": dict(sorted(catalyst_counter.items())),
        "top_question_geo_keywords": dict(geo_counter.most_common(15)),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Build a targeted diplomacy snapshot pool before fetching price history.")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-03-01")
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--start-offsets", default="")
    parser.add_argument("--offset-range-start", type=int, default=None)
    parser.add_argument("--offset-range-end", type=int, default=None)
    parser.add_argument("--offset-range-step", type=int, default=20000)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--lookback-events", type=int, default=50000)
    parser.add_argument("--max-events-fetch", type=int, default=50000)
    parser.add_argument("--max-candidate-markets", type=int, default=800)
    parser.add_argument("--max-history-requests", type=int, default=800)
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--min-match-score", type=float, default=1.55)
    parser.add_argument("--catalyst-types", default="ceasefire")
    parser.add_argument("--discovery-keywords", default=",".join(_DEFAULT_DIPLOMACY_DISCOVERY_KEYWORDS))
    parser.add_argument("--discovery-exclusion-keywords", default=",".join(_DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS))
    parser.add_argument("--coarse-min-score", type=float, default=1.15)
    parser.add_argument("--dataset-output", required=True)
    parser.add_argument("--manifest-output", default="auto")
    parser.add_argument("--summary-output", default=None)
    parser.add_argument("--use-liquidity-filter", action="store_true")
    parser.add_argument("--repricing-research-mode", action="store_true")
    parser.add_argument("--align-window-to-discovered-events", action="store_true")
    parser.add_argument("--align-window-padding-days", type=int, default=14)
    parser.add_argument("--min-diplomacy-end-date", default="")
    return parser.parse_args()


def main():
    args = parse_args()
    start_ts = _parse_date_to_ts(args.start_date)
    end_ts = _parse_date_to_ts(args.end_date) + (24 * 3600) - 1
    allowed_catalyst_types = _parse_csv_set(args.catalyst_types)
    discovery_keywords = _parse_csv_list(args.discovery_keywords)
    discovery_exclusion_keywords = _parse_csv_list(args.discovery_exclusion_keywords)
    offsets = _build_offset_list(args)

    if offsets:
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
        args.coarse_min_score,
    )
    print(f"Coarse keyword-prefilter events kept: {coarse_summary['events_kept']}")
    print(f"Coarse keyword-prefilter markets kept: {coarse_summary['markets_kept']}")

    diplomacy_events, manifest_rows, filter_summary = _filter_events_to_diplomacy(
        coarse_events,
        min_match_score=args.min_match_score,
        allowed_catalyst_types=allowed_catalyst_types,
    )
    print(f"Diplomacy events kept: {filter_summary['events_kept']}")
    print(f"Diplomacy markets kept: {filter_summary['markets_kept']}")
    print(f"Catalyst types: {filter_summary['catalyst_type_counts']}")

    min_end_ts = _to_unix(f"{args.min_diplomacy_end_date}T00:00:00+00:00") if args.min_diplomacy_end_date else None
    diplomacy_events, manifest_rows = _filter_events_by_end_date_floor(diplomacy_events, manifest_rows, min_end_ts)

    if args.align_window_to_discovered_events:
        min_event_ts, max_event_ts = _infer_event_window(diplomacy_events)
        if min_event_ts is not None and max_event_ts is not None:
            padding = args.align_window_padding_days * 24 * 3600
            start_ts = max(0, min_event_ts - padding)
            end_ts = max_event_ts + padding
            args.start_date = _date_label(start_ts)
            args.end_date = _date_label(end_ts)
            print(f"Aligned diplomacy research window to {args.start_date} .. {args.end_date}")

    candidates, rejects, reasons, diagnostics, dataset_rows = build_candidates(
        events=diplomacy_events,
        start_ts=start_ts,
        end_ts=end_ts,
        entry_hours_before_close=args.entry_hours_before_close,
        history_window_days=args.history_window_days,
        max_markets=args.max_candidate_markets,
        fidelity=args.history_fidelity,
        use_liquidity_filter=args.use_liquidity_filter,
        max_history_requests=args.max_history_requests,
        skip_base_filters=args.repricing_research_mode,
        skip_score_filters=args.repricing_research_mode,
    )

    dataset_path = resolve_dataset_output(args.dataset_output, args.start_date, args.end_date)
    write_jsonl(dataset_rows, dataset_path)
    print(f"Snapshot pool written: {dataset_path}")
    print(f"Dataset rows: {len(dataset_rows)} | Final candidates: {len(candidates)}")

    manifest_path = _resolve_manifest_output(args.manifest_output, args.start_date, args.end_date)
    write_jsonl(manifest_rows, manifest_path)
    print(f"Manifest written: {manifest_path}")

    summary = {
        "period": {"start_date": args.start_date, "end_date": args.end_date},
        "parameters": {
            "start_offset": start_offset,
            "start_offsets": offsets,
            "page_size": args.page_size,
            "max_events_fetch": args.max_events_fetch,
            "max_candidate_markets": args.max_candidate_markets,
            "max_history_requests": args.max_history_requests,
            "entry_hours_before_close": args.entry_hours_before_close,
            "history_window_days": args.history_window_days,
            "history_fidelity": args.history_fidelity,
            "min_match_score": args.min_match_score,
            "catalyst_types": sorted(allowed_catalyst_types),
            "discovery_keywords": discovery_keywords,
            "discovery_exclusion_keywords": discovery_exclusion_keywords,
            "coarse_min_score": args.coarse_min_score,
            "repricing_research_mode": args.repricing_research_mode,
            "align_window_to_discovered_events": args.align_window_to_discovered_events,
            "align_window_padding_days": args.align_window_padding_days,
            "use_liquidity_filter": args.use_liquidity_filter,
        },
        "fetch_summary": fetched_meta,
        "coarse_prefilter": coarse_summary,
        "filter_summary": filter_summary,
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
