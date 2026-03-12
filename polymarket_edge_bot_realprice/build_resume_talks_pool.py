import argparse
import json
from collections import Counter
from pathlib import Path

from backtest import (
    GAMMA_EVENTS_API,
    _parse_date_to_ts,
    _request_json,
    build_candidates,
    fetch_closed_events,
    find_total_closed_events,
)
from build_diplomacy_pool import (
    _build_offset_list,
    _date_label,
    _fetch_events_for_offsets,
    _filter_events_by_end_date_floor,
    _infer_event_window,
    _resolve_manifest_output,
    _to_unix,
)
from geopolitical_context import build_geopolitical_context, normalize_text
from meeting_subtype import infer_meeting_subtype
from research_dataset import resolve_dataset_output, write_jsonl


_DEFAULT_RESUME_DISCOVERY_KEYWORDS = (
    "resume talks",
    "talks resume",
    "resume negotiations",
    "negotiations resume",
    "restart talks",
    "restart negotiations",
    "restart dialogue",
    "return to talks",
    "return to negotiations",
    "back to talks",
    "renew talks",
    "renew negotiations",
    "reopen talks",
    "reopen negotiations",
    "resume peace talks",
    "resume nuclear talks",
    "resume dialogue",
    "return to dialogue",
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
_STRONG_RESUME_PATTERNS = (
    "resume talks",
    "talks resume",
    "resume negotiations",
    "negotiations resume",
    "restart talks",
    "restart negotiations",
    "return to talks",
    "return to negotiations",
    "reopen talks",
    "reopen negotiations",
)


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


def _keyword_hits(text, keywords):
    normalized = normalize_text(text)
    return [keyword for keyword in keywords if keyword in normalized]


def _coarse_resume_market_score(question_text, context_text, discovery_keywords):
    question_hits = _keyword_hits(question_text, discovery_keywords)
    context_hits = [hit for hit in _keyword_hits(context_text, discovery_keywords) if hit not in question_hits]

    score = 0.0
    score += min(2.4, len(question_hits) * 1.00)
    score += min(0.5, len(context_hits) * 0.18)
    if any(hit in _STRONG_RESUME_PATTERNS for hit in question_hits):
        score += 0.8
    if any("negotiat" in hit or "dialogue" in hit for hit in question_hits + context_hits):
        score += 0.3
    return score, question_hits, context_hits


def _coarse_keyword_prefilter_events(events, discovery_keywords, exclusion_keywords, min_coarse_score):
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

            coarse_score, question_hits, context_hits = _coarse_resume_market_score(
                question_text,
                context_text,
                discovery_keywords,
            )
            hits = question_hits + [hit for hit in context_hits if hit not in question_hits]
            if not hits or coarse_score < min_coarse_score:
                continue

            enriched = dict(market)
            enriched["_resume_discovery_hits"] = hits
            enriched["_resume_question_hits"] = question_hits
            enriched["_resume_context_hits"] = context_hits
            enriched["_resume_discovery_score"] = coarse_score
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


def _resume_talks_score(context, question):
    text = normalize_text(question)
    score = float(context.get("match_score") or 0.0)
    score += min(0.30, len(context.get("question_geo_keywords") or []) * 0.06)
    score += min(0.12, len(context.get("institution_keywords") or []) * 0.03)
    if context.get("catalyst_has_official_source"):
        score += 0.10
    if "resume" in text:
        score += 0.22
    if "negotiat" in text or "dialogue" in text:
        score += 0.15
    if any(pattern in text for pattern in _STRONG_RESUME_PATTERNS):
        score += 0.24
    return score


def _build_manifest_row(event, market, meeting_subtype, resume_score):
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
        "discovery_hits": market.get("_resume_discovery_hits") or [],
        "question_discovery_hits": market.get("_resume_question_hits") or [],
        "context_discovery_hits": market.get("_resume_context_hits") or [],
        "discovery_score": market.get("_resume_discovery_score"),
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
        "meeting_subtype": meeting_subtype,
        "resume_talks_score": resume_score,
    }


def _filter_events_to_targets(events, event_slugs, market_slugs):
    if not event_slugs and not market_slugs:
        return events

    filtered_events = []
    for event in events:
        event_slug = str(event.get("slug") or "").strip().lower()
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue

        if event_slugs and event_slug in event_slugs:
            filtered_events.append(event)
            continue

        kept = []
        for market in markets:
            market_slug = str(market.get("slug") or "").strip().lower()
            if market_slugs and market_slug in market_slugs:
                kept.append(market)

        if kept:
            cloned = dict(event)
            cloned["markets"] = kept
            filtered_events.append(cloned)

    return filtered_events


def _fetch_events_by_slugs(event_slugs, page_size):
    events = []
    fetched_meta = []
    for slug in sorted(event_slugs):
        url = (
            f"{GAMMA_EVENTS_API}?"
            f"closed=true&slug={slug}&limit={max(1, int(page_size))}"
        )
        batch = _request_json(url)
        if not isinstance(batch, list):
            batch = []
        events.extend(batch)
        fetched_meta.append({"slug": slug, "event_count": len(batch)})
    return events, fetched_meta


def _filter_events_to_resume_talks(events, min_match_score):
    filtered_events = []
    manifest_rows = []
    kept_markets = 0
    scanned_markets = 0
    geo_counter = Counter()

    for event in events:
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue

        kept = []
        for market in markets:
            scanned_markets += 1
            question = market.get("question")
            context = build_geopolitical_context(
                question,
                event.get("title") or event.get("question"),
                event.get("description"),
                event.get("category"),
                event.get("resolutionSource"),
            )
            if context.get("quote_market"):
                continue
            if str(context.get("action_family") or "").lower() != "diplomacy":
                continue
            if str(context.get("catalyst_type") or "").lower() != "call_or_meeting":
                continue

            meeting_subtype = infer_meeting_subtype(question, catalyst_type=context.get("catalyst_type"))
            if meeting_subtype != "resume_talks":
                continue

            score = _resume_talks_score(context, question)
            if score < min_match_score:
                continue

            enriched = dict(market)
            enriched["geopolitical_context"] = context
            enriched["_resume_talks_score"] = score
            enriched["_meeting_subtype"] = meeting_subtype
            kept.append(enriched)
            kept_markets += 1
            for keyword in context.get("question_geo_keywords") or []:
                geo_counter[keyword] += 1
            manifest_rows.append(_build_manifest_row(event, enriched, meeting_subtype, score))

        if kept:
            cloned = dict(event)
            cloned["markets"] = kept
            filtered_events.append(cloned)

    return filtered_events, manifest_rows, {
        "events_kept": len(filtered_events),
        "markets_scanned": scanned_markets,
        "markets_kept": kept_markets,
        "meeting_subtype_counts": {"resume_talks": kept_markets},
        "top_question_geo_keywords": dict(geo_counter.most_common(15)),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Build a targeted resume-talks snapshot pool before fetching price history.")
    parser.add_argument("--start-date", default="2025-06-01")
    parser.add_argument("--end-date", default="2026-03-01")
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--start-offsets", default="")
    parser.add_argument("--offset-range-start", type=int, default=None)
    parser.add_argument("--offset-range-end", type=int, default=None)
    parser.add_argument("--offset-range-step", type=int, default=20000)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--lookback-events", type=int, default=50000)
    parser.add_argument("--max-events-fetch", type=int, default=50000)
    parser.add_argument("--max-candidate-markets", type=int, default=400)
    parser.add_argument("--max-history-requests", type=int, default=400)
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--min-match-score", type=float, default=1.55)
    parser.add_argument("--discovery-keywords", default=",".join(_DEFAULT_RESUME_DISCOVERY_KEYWORDS))
    parser.add_argument("--discovery-exclusion-keywords", default=",".join(_DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS))
    parser.add_argument("--coarse-min-score", type=float, default=1.10)
    parser.add_argument("--target-event-slugs", default="")
    parser.add_argument("--target-market-slugs", default="")
    parser.add_argument("--dataset-output", required=True)
    parser.add_argument("--manifest-output", default="auto")
    parser.add_argument("--summary-output", default=None)
    parser.add_argument("--use-liquidity-filter", action="store_true")
    parser.add_argument("--repricing-research-mode", action="store_true")
    parser.add_argument("--align-window-to-discovered-events", action="store_true")
    parser.add_argument("--align-window-padding-days", type=int, default=14)
    parser.add_argument("--min-resume-end-date", default="2025-06-01")
    return parser.parse_args()


def main():
    args = parse_args()
    start_ts = _parse_date_to_ts(args.start_date)
    end_ts = _parse_date_to_ts(args.end_date) + (24 * 3600) - 1
    discovery_keywords = _parse_csv_list(args.discovery_keywords)
    discovery_exclusion_keywords = _parse_csv_list(args.discovery_exclusion_keywords)
    target_event_slugs = _parse_csv_set(args.target_event_slugs)
    target_market_slugs = _parse_csv_set(args.target_market_slugs)
    offsets = _build_offset_list(args)

    if target_event_slugs:
        events, fetched_meta = _fetch_events_by_slugs(target_event_slugs, page_size=args.page_size)
        start_offset = 0
    elif offsets:
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
    events = _filter_events_to_targets(events, target_event_slugs, target_market_slugs)
    print(f"Target-filtered events: {len(events)}")
    coarse_events, coarse_summary = _coarse_keyword_prefilter_events(
        events,
        discovery_keywords,
        discovery_exclusion_keywords,
        args.coarse_min_score,
    )
    print(f"Coarse keyword-prefilter events kept: {coarse_summary['events_kept']}")
    print(f"Coarse keyword-prefilter markets kept: {coarse_summary['markets_kept']}")

    resume_events, manifest_rows, filter_summary = _filter_events_to_resume_talks(
        coarse_events,
        min_match_score=args.min_match_score,
    )
    print(f"Resume-talks events kept: {filter_summary['events_kept']}")
    print(f"Resume-talks markets kept: {filter_summary['markets_kept']}")

    min_end_ts = _to_unix(f"{args.min_resume_end_date}T00:00:00+00:00") if args.min_resume_end_date else None
    resume_events, manifest_rows = _filter_events_by_end_date_floor(resume_events, manifest_rows, min_end_ts)

    if args.align_window_to_discovered_events:
        min_event_ts, max_event_ts = _infer_event_window(resume_events)
        if min_event_ts is not None and max_event_ts is not None:
            padding = args.align_window_padding_days * 24 * 3600
            start_ts = max(0, min_event_ts - padding)
            end_ts = max_event_ts + padding
            args.start_date = _date_label(start_ts)
            args.end_date = _date_label(end_ts)
            print(f"Aligned resume-talks research window to {args.start_date} .. {args.end_date}")

    candidates, rejects, reasons, diagnostics, dataset_rows = build_candidates(
        events=resume_events,
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
            "discovery_keywords": discovery_keywords,
            "discovery_exclusion_keywords": discovery_exclusion_keywords,
            "coarse_min_score": args.coarse_min_score,
            "target_event_slugs": sorted(target_event_slugs),
            "target_market_slugs": sorted(target_market_slugs),
            "repricing_research_mode": args.repricing_research_mode,
            "align_window_to_discovered_events": args.align_window_to_discovered_events,
            "align_window_padding_days": args.align_window_padding_days,
            "use_liquidity_filter": args.use_liquidity_filter,
            "min_resume_end_date": args.min_resume_end_date,
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
