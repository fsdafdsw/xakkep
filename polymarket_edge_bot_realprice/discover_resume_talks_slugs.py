import argparse
import json
from collections import Counter
from pathlib import Path

from build_diplomacy_pool import _build_offset_list, _fetch_events_for_offsets
from build_resume_talks_pool import (
    _DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS,
    _DEFAULT_RESUME_DISCOVERY_KEYWORDS,
    _coarse_keyword_prefilter_events,
    _filter_events_to_resume_talks,
    _parse_csv_list,
)
from research_dataset import write_jsonl


def parse_args():
    parser = argparse.ArgumentParser(description="Discovery-only sweep for resume-talks event slugs.")
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--start-offsets", default="")
    parser.add_argument("--offset-range-start", type=int, default=None)
    parser.add_argument("--offset-range-end", type=int, default=None)
    parser.add_argument("--offset-range-step", type=int, default=20000)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--max-events-fetch", type=int, default=2000)
    parser.add_argument("--min-match-score", type=float, default=1.55)
    parser.add_argument("--coarse-min-score", type=float, default=1.10)
    parser.add_argument("--discovery-keywords", default=",".join(_DEFAULT_RESUME_DISCOVERY_KEYWORDS))
    parser.add_argument("--discovery-exclusion-keywords", default=",".join(_DEFAULT_DISCOVERY_EXCLUSION_KEYWORDS))
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--summary-output", required=True)
    return parser.parse_args()


def _dedupe_manifest_rows(rows):
    deduped = {}
    for row in rows:
        key = (
            str(row.get("event_slug") or "").strip().lower(),
            str(row.get("market_slug") or "").strip().lower(),
            str(row.get("question") or "").strip().lower(),
        )
        prev = deduped.get(key)
        if prev is None or float(row.get("resume_talks_score") or 0.0) > float(prev.get("resume_talks_score") or 0.0):
            deduped[key] = row
    rows = list(deduped.values())
    rows.sort(
        key=lambda row: (
            -(float(row.get("resume_talks_score") or 0.0)),
            -(float(row.get("match_score") or 0.0)),
            -(float(row.get("discovery_score") or 0.0)),
        )
    )
    return rows


def _write_summary(args, offsets, fetched_meta, manifest_rows, coarse_totals, filter_totals, summary_output):
    summary = {
        "parameters": {
            "start_offset": args.start_offset,
            "start_offsets": offsets,
            "page_size": args.page_size,
            "max_events_fetch": args.max_events_fetch,
            "min_match_score": args.min_match_score,
            "coarse_min_score": args.coarse_min_score,
            "discovery_keywords": _parse_csv_list(args.discovery_keywords),
            "discovery_exclusion_keywords": _parse_csv_list(args.discovery_exclusion_keywords),
        },
        "processed_offsets": [item.get("offset") for item in fetched_meta],
        "fetch_summary": fetched_meta,
        "coarse_prefilter": {
            "events_kept": coarse_totals["events_kept"],
            "markets_kept": coarse_totals["markets_kept"],
            "top_keywords": dict(coarse_totals["keywords"].most_common(20)),
            "top_exclusions": dict(coarse_totals["exclusions"].most_common(20)),
        },
        "filter_summary": {
            "events_kept": filter_totals["events_kept"],
            "markets_kept": filter_totals["markets_kept"],
            "top_question_geo_keywords": dict(filter_totals["geo_keywords"].most_common(15)),
        },
        "manifest_row_count": len(manifest_rows),
        "event_slug_count": len({str(row.get("event_slug") or "").strip().lower() for row in manifest_rows if row.get("event_slug")}),
        "top_event_slugs": [
            {
                "event_slug": row.get("event_slug"),
                "question": row.get("question"),
                "resume_talks_score": row.get("resume_talks_score"),
                "match_score": row.get("match_score"),
            }
            for row in manifest_rows[:20]
        ],
        "manifest_output": args.manifest_output,
    }

    summary_path = Path(summary_output)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=True)
    return summary


def main():
    args = parse_args()
    offsets = _build_offset_list(args)
    discovery_keywords = _parse_csv_list(args.discovery_keywords)
    exclusion_keywords = _parse_csv_list(args.discovery_exclusion_keywords)

    fetched_meta = []
    manifest_rows = []
    coarse_totals = {
        "events_kept": 0,
        "markets_kept": 0,
        "keywords": Counter(),
        "exclusions": Counter(),
    }
    filter_totals = {
        "events_kept": 0,
        "markets_kept": 0,
        "geo_keywords": Counter(),
    }

    for offset in offsets:
        print(f"Fetching closed events from offset {offset} (max {args.max_events_fetch})...")
        events, batch_fetch_meta = _fetch_events_for_offsets(
            offsets=[offset],
            max_events_fetch=args.max_events_fetch,
            page_size=args.page_size,
        )
        fetched_meta.extend(batch_fetch_meta)

        coarse_events, coarse_summary = _coarse_keyword_prefilter_events(
            events,
            discovery_keywords,
            exclusion_keywords,
            args.coarse_min_score,
        )
        _, batch_manifest_rows, filter_summary = _filter_events_to_resume_talks(
            coarse_events,
            min_match_score=args.min_match_score,
        )

        manifest_rows.extend(batch_manifest_rows)
        manifest_rows = _dedupe_manifest_rows(manifest_rows)

        coarse_totals["events_kept"] += int(coarse_summary.get("events_kept") or 0)
        coarse_totals["markets_kept"] += int(coarse_summary.get("markets_kept") or 0)
        coarse_totals["keywords"].update(coarse_summary.get("top_keywords") or {})
        coarse_totals["exclusions"].update(coarse_summary.get("top_exclusions") or {})

        filter_totals["events_kept"] += int(filter_summary.get("events_kept") or 0)
        filter_totals["markets_kept"] += int(filter_summary.get("markets_kept") or 0)
        filter_totals["geo_keywords"].update(filter_summary.get("top_question_geo_keywords") or {})

        write_jsonl(manifest_rows, args.manifest_output)
        summary = _write_summary(
            args,
            offsets,
            fetched_meta,
            manifest_rows,
            coarse_totals,
            filter_totals,
            args.summary_output,
        )
        print(
            f"Checkpoint offset={offset}: "
            f"manifest_rows={len(manifest_rows)} "
            f"unique_event_slugs={summary['event_slug_count']}"
        )

    print(f"Processed offsets: {len(fetched_meta)}")
    print(f"Resume talks manifest rows: {len(manifest_rows)}")
    print(f"Unique event slugs: {summary['event_slug_count']}")
    print(f"Summary written: {args.summary_output}")


if __name__ == "__main__":
    main()
