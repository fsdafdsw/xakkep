import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


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


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _join_keys(row):
    question = str(row.get("question") or "").strip().lower()
    event_slug = str(row.get("event_slug") or "").strip().lower()
    market_slug = str(row.get("market_slug") or "").strip().lower()
    token_id = str(row.get("token_id") or "").strip()
    market_id = str(row.get("market_id") or "").strip()
    keys = set()
    if event_slug and market_slug:
        keys.add(("event_market", event_slug, market_slug))
    if event_slug and question:
        keys.add(("event_question", event_slug, question))
    if market_slug and question:
        keys.add(("market_question", market_slug, question))
    if token_id:
        keys.add(("token", token_id))
    if market_id:
        keys.add(("market_id", market_id))
    return keys


def _index_rows(rows):
    index = defaultdict(list)
    for row in rows:
        for key in _join_keys(row):
            index[key].append(row)
    return index


def _first_match(index, row):
    for key in _join_keys(row):
        matches = index.get(key) or []
        if matches:
            return matches[0]
    return None


def _history_quality(repricing_row):
    if not repricing_row:
        return "none"
    history_source = str(repricing_row.get("history_source") or "")
    if history_source in {"api_only", "api_plus_settlement"}:
        return "real_forward"
    if history_source == "settlement_only":
        return "settlement_fallback"
    return "unknown"


def _research_action(snapshot_row, repricing_row):
    if not snapshot_row:
        return "rebuild_snapshot_window"
    if not repricing_row:
        entry_price = _safe_float(snapshot_row.get("entry_price") or snapshot_row.get("market_implied"))
        repricing_potential = _safe_float(snapshot_row.get("repricing_potential"), default=0.0) or 0.0
        repricing_verdict = str(snapshot_row.get("repricing_verdict") or "")
        if entry_price is not None and entry_price >= 0.85:
            return "skip_late_snapshot"
        if repricing_verdict == "ignore" and repricing_potential < 0.60:
            return "skip_weak_snapshot"
        return "run_repricing_backtest"
    history_quality = _history_quality(repricing_row)
    if history_quality == "real_forward":
        return "ready_for_selector_tuning"
    if history_quality == "settlement_fallback":
        return "needs_alt_history_source"
    return "inspect_manually"


def _priority_score(manifest_row, snapshot_row, repricing_row):
    score = 0.0
    score += _safe_float(manifest_row.get("discovery_score"), 0.0) or 0.0
    score += _safe_float(manifest_row.get("diplomacy_score"), 0.0) or 0.0
    score += _safe_float(manifest_row.get("match_score"), 0.0) or 0.0

    if manifest_row.get("catalyst_has_official_source"):
        score += 0.20
    if snapshot_row:
        score += 0.25
        score += min(0.20, (_safe_float(snapshot_row.get("repricing_potential"), 0.0) or 0.0) * 0.20)
    if repricing_row:
        verdict = str(repricing_row.get("repricing_verdict") or "")
        if verdict == "watch_high_upside":
            score += 0.30
        elif verdict == "watch":
            score += 0.20
        elif verdict == "buy_now":
            score += 0.35
        history_quality = _history_quality(repricing_row)
        if history_quality == "real_forward":
            score += 0.35
        elif history_quality == "settlement_fallback":
            score -= 0.10
        score += min(0.50, max(0.0, _safe_float(repricing_row.get("best_runup_pct"), 0.0) or 0.0) * 0.10)
    return round(score, 6)


def _build_manifest_rows(manifest_rows, snapshot_index, repricing_index, catalyst_type):
    output_rows = []
    for row in manifest_rows:
        if str(row.get("catalyst_type") or "") != catalyst_type:
            continue
        snapshot_row = _first_match(snapshot_index, row)
        repricing_row = _first_match(repricing_index, row)
        history_quality = _history_quality(repricing_row)
        action = _research_action(snapshot_row, repricing_row)
        output_rows.append(
            {
                "event_id": row.get("event_id"),
                "event_slug": row.get("event_slug"),
                "event_title": row.get("event_title"),
                "event_end_date": row.get("event_end_date"),
                "market_id": row.get("market_id"),
                "market_slug": row.get("market_slug"),
                "market_end_date": row.get("market_end_date"),
                "question": row.get("question"),
                "catalyst_type": row.get("catalyst_type"),
                "catalyst_strength": row.get("catalyst_strength"),
                "catalyst_has_official_source": row.get("catalyst_has_official_source"),
                "discovery_score": row.get("discovery_score"),
                "diplomacy_score": row.get("diplomacy_score"),
                "match_score": row.get("match_score"),
                "discovery_hits": row.get("discovery_hits") or [],
                "question_geo_keywords": row.get("question_geo_keywords") or [],
                "snapshot_present": snapshot_row is not None,
                "snapshot_entry_ts": snapshot_row.get("entry_ts") if snapshot_row else None,
                "snapshot_repricing_potential": snapshot_row.get("repricing_potential") if snapshot_row else None,
                "snapshot_repricing_verdict": snapshot_row.get("repricing_verdict") if snapshot_row else None,
                "repricing_present": repricing_row is not None,
                "repricing_verdict": repricing_row.get("repricing_verdict") if repricing_row else None,
                "history_source": repricing_row.get("history_source") if repricing_row else None,
                "history_quality": history_quality,
                "best_runup_pct": repricing_row.get("best_runup_pct") if repricing_row else None,
                "repricing_score": repricing_row.get("repricing_score") if repricing_row else None,
                "repricing_watch_score": repricing_row.get("repricing_watch_score") if repricing_row else None,
                "attention_gap": repricing_row.get("repricing_attention_gap") if repricing_row else None,
                "already_priced_penalty": repricing_row.get("repricing_already_priced_penalty") if repricing_row else None,
                "research_action": action,
                "priority_score": _priority_score(row, snapshot_row, repricing_row),
            }
        )
    output_rows.sort(
        key=lambda item: (
            -(item.get("priority_score") or 0.0),
            -(item.get("best_runup_pct") or 0.0),
            -(item.get("match_score") or 0.0),
            -(item.get("discovery_score") or 0.0),
        )
    )
    return output_rows


def _group_top_events(rows, limit):
    grouped = defaultdict(list)
    for row in rows:
        key = row.get("event_slug") or row.get("event_id") or row.get("event_title") or row.get("question")
        grouped[key].append(row)

    leaders = []
    for key, items in grouped.items():
        top = sorted(items, key=lambda item: (-(item.get("priority_score") or 0.0), -(item.get("best_runup_pct") or 0.0)))[0]
        leaders.append(
            {
                "event_key": key,
                "event_title": top.get("event_title"),
                "event_end_date": top.get("event_end_date"),
                "market_count": len(items),
                "best_priority_score": top.get("priority_score"),
                "best_question": top.get("question"),
                "best_history_quality": top.get("history_quality"),
                "best_research_action": top.get("research_action"),
                "best_runup_pct": top.get("best_runup_pct"),
            }
        )

    leaders.sort(
        key=lambda item: (
            -(item.get("best_priority_score") or 0.0),
            -(item.get("best_runup_pct") or 0.0),
            -(item.get("market_count") or 0),
        )
    )
    return leaders[:limit]


def _summary(rows, top_limit):
    history_counter = Counter(row.get("history_quality") or "none" for row in rows)
    action_counter = Counter(row.get("research_action") or "unknown" for row in rows)
    verdict_counter = Counter(row.get("repricing_verdict") or "none" for row in rows if row.get("repricing_present"))
    top_rows = rows[:top_limit]
    return {
        "row_count": len(rows),
        "snapshot_present_count": sum(1 for row in rows if row.get("snapshot_present")),
        "repricing_present_count": sum(1 for row in rows if row.get("repricing_present")),
        "history_quality_counts": dict(sorted(history_counter.items())),
        "research_action_counts": dict(sorted(action_counter.items())),
        "repricing_verdict_counts": dict(sorted(verdict_counter.items())),
        "top_events": _group_top_events(rows, top_limit),
        "top_markets": top_rows,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Build a ceasefire-first research manifest from diplomacy discovery outputs.")
    parser.add_argument("--manifest-input", required=True)
    parser.add_argument("--snapshot-input", default="")
    parser.add_argument("--repricing-input", default="")
    parser.add_argument("--catalyst-type", default="ceasefire")
    parser.add_argument("--output-jsonl", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--top-limit", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()
    manifest_rows = _load_jsonl(args.manifest_input)
    snapshot_rows = _load_jsonl(args.snapshot_input) if args.snapshot_input else []
    repricing_rows = _load_jsonl(args.repricing_input) if args.repricing_input else []

    snapshot_index = _index_rows(snapshot_rows)
    repricing_index = _index_rows(repricing_rows)

    manifest = _build_manifest_rows(
        manifest_rows,
        snapshot_index=snapshot_index,
        repricing_index=repricing_index,
        catalyst_type=args.catalyst_type,
    )
    summary = {
        "inputs": {
            "manifest_input": args.manifest_input,
            "snapshot_input": args.snapshot_input,
            "repricing_input": args.repricing_input,
            "catalyst_type": args.catalyst_type,
        },
        "summary": _summary(manifest, args.top_limit),
    }

    output_jsonl = Path(args.output_jsonl)
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with output_jsonl.open("w", encoding="utf-8") as fh:
        for row in manifest:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    with output_json.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=True)

    print(f"Manifest rows written: {output_jsonl}")
    print(f"Summary written: {output_json}")
    print(
        "Counts: "
        f"rows={summary['summary']['row_count']} "
        f"snapshot_present={summary['summary']['snapshot_present_count']} "
        f"repricing_present={summary['summary']['repricing_present_count']}"
    )
    print(f"History quality: {summary['summary']['history_quality_counts']}")
    print(f"Research actions: {summary['summary']['research_action_counts']}")


if __name__ == "__main__":
    main()
