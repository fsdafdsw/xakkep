import argparse
import json
from pathlib import Path

from config import REPRICING_LANE_PRIOR_WEIGHT, REPRICING_WATCH_PRIOR_WEIGHT
from repricing_selector import repricing_lane_info


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


def _load_jsonl(path):
    rows = []
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _tp25(row):
    window = ((row.get("window_metrics") or {}).get("3d") or {}).get("labels") or {}
    return bool(window.get("repriced_25pct"))


def _normalize_row(row):
    action_family = str(row.get("domain_action_family") or "unknown")
    catalyst_type = row.get("catalyst_type")
    meeting_subtype = row.get("meeting_subtype")
    lane_key = row.get("repricing_lane_key")
    lane_label = row.get("repricing_lane_label")
    lane_prior = _safe_float(row.get("repricing_lane_prior"))
    if lane_key is None or lane_label is None or lane_prior is None:
        lane = repricing_lane_info(action_family, catalyst_type, meeting_subtype)
        lane_key = lane["lane_key"]
        lane_label = lane["lane_label"]
        lane_prior = lane["lane_prior"]

    current_score = _safe_float(row.get("repricing_score"), 0.0) or 0.0
    current_watch_score = _safe_float(row.get("repricing_watch_score"), 0.0) or 0.0
    prior_delta = lane_prior - 0.50

    normalized = dict(row)
    normalized["lane_key"] = lane_key
    normalized["lane_label"] = lane_label
    normalized["lane_prior"] = lane_prior
    normalized["entry_ts"] = _safe_int(row.get("entry_ts"), 0) or 0
    normalized["best_runup_pct"] = _safe_float(row.get("best_runup_pct"), 0.0) or 0.0
    normalized["tp25_3d"] = _tp25(row)
    normalized["current_rank_score"] = current_score
    normalized["baseline_rank_score"] = current_score - (prior_delta * REPRICING_LANE_PRIOR_WEIGHT)
    normalized["current_watch_rank_score"] = current_watch_score
    normalized["baseline_watch_rank_score"] = current_watch_score - (prior_delta * REPRICING_WATCH_PRIOR_WEIGHT)
    return normalized


def _selection_summary(rows):
    if not rows:
        return {
            "count": 0,
            "mean_best_runup_pct": None,
            "tp25_rate": None,
            "mean_current_rank_score": None,
            "mean_baseline_rank_score": None,
            "top_cases": [],
        }

    ordered = sorted(
        rows,
        key=lambda row: (
            row.get("best_runup_pct") if row.get("best_runup_pct") is not None else float("-inf"),
            row.get("current_rank_score") if row.get("current_rank_score") is not None else float("-inf"),
        ),
        reverse=True,
    )
    return {
        "count": len(rows),
        "mean_best_runup_pct": sum(row["best_runup_pct"] for row in rows) / len(rows),
        "tp25_rate": sum(1 for row in rows if row.get("tp25_3d")) / len(rows),
        "mean_current_rank_score": sum(row["current_rank_score"] for row in rows) / len(rows),
        "mean_baseline_rank_score": sum(row["baseline_rank_score"] for row in rows) / len(rows),
        "top_cases": [
            {
                "question": row.get("question"),
                "lane_key": row.get("lane_key"),
                "best_runup_pct": row.get("best_runup_pct"),
                "repricing_verdict": row.get("repricing_verdict"),
                "current_rank_score": row.get("current_rank_score"),
                "baseline_rank_score": row.get("baseline_rank_score"),
            }
            for row in ordered[:3]
        ],
    }


def _top_k_comparison(rows, top_k):
    if not rows:
        return {
            "top_k": top_k,
            "current": _selection_summary([]),
            "baseline": _selection_summary([]),
            "delta_mean_best_runup_pct": None,
            "delta_tp25_rate": None,
        }

    current = sorted(
        rows,
        key=lambda row: (
            row["current_rank_score"],
            row["current_watch_rank_score"],
            row["best_runup_pct"],
        ),
        reverse=True,
    )[: min(top_k, len(rows))]
    baseline = sorted(
        rows,
        key=lambda row: (
            row["baseline_rank_score"],
            row["baseline_watch_rank_score"],
            row["best_runup_pct"],
        ),
        reverse=True,
    )[: min(top_k, len(rows))]
    current_summary = _selection_summary(current)
    baseline_summary = _selection_summary(baseline)
    current_mean = current_summary["mean_best_runup_pct"]
    baseline_mean = baseline_summary["mean_best_runup_pct"]
    current_tp25 = current_summary["tp25_rate"]
    baseline_tp25 = baseline_summary["tp25_rate"]
    return {
        "top_k": top_k,
        "current": current_summary,
        "baseline": baseline_summary,
        "delta_mean_best_runup_pct": None if current_mean is None or baseline_mean is None else current_mean - baseline_mean,
        "delta_tp25_rate": None if current_tp25 is None or baseline_tp25 is None else current_tp25 - baseline_tp25,
    }


def _group_report(rows, top_ks):
    rows = sorted(rows, key=lambda row: row["entry_ts"])
    return {
        "count": len(rows),
        "first_entry_ts": rows[0]["entry_ts"] if rows else None,
        "last_entry_ts": rows[-1]["entry_ts"] if rows else None,
        "comparisons": [_top_k_comparison(rows, top_k) for top_k in top_ks],
    }


def main():
    parser = argparse.ArgumentParser(description="Compare lane-aware repricing ranking vs baseline ranking on holdout data.")
    parser.add_argument("--inputs", required=True, help="Comma-separated list of repricing rows JSONL files.")
    parser.add_argument("--output", required=True, help="Path to output JSON summary.")
    parser.add_argument("--holdout-ratio", type=float, default=0.4)
    parser.add_argument("--top-ks", default="1,3,5")
    parser.add_argument("--min-real-forward-only", action="store_true")
    parser.add_argument("--min-lane-count", type=int, default=2)
    args = parser.parse_args()

    top_ks = [int(part.strip()) for part in str(args.top_ks).split(",") if part.strip()]
    deduped = {}
    for raw_path in [part.strip() for part in str(args.inputs).split(",") if part.strip()]:
        for row in _load_jsonl(raw_path):
            snapshot_id = row.get("snapshot_id") or f"{row.get('event_slug')}:{row.get('entry_ts')}"
            candidate = _normalize_row(row)
            existing = deduped.get(snapshot_id)
            if existing is None:
                deduped[snapshot_id] = candidate
                continue
            if str(candidate.get("history_source") or "").startswith("api") and not str(existing.get("history_source") or "").startswith("api"):
                deduped[snapshot_id] = candidate
                continue
            if candidate["entry_ts"] > existing["entry_ts"]:
                deduped[snapshot_id] = candidate

    rows = list(deduped.values())
    rows.sort(key=lambda row: row["entry_ts"])
    if args.min_real_forward_only:
        rows = [row for row in rows if str(row.get("history_source") or "") in {"api_only", "api_plus_settlement"}]

    holdout_start = max(1, int(len(rows) * (1.0 - args.holdout_ratio))) if rows else 0
    train_rows = rows[:holdout_start]
    test_rows = rows[holdout_start:]

    by_lane = {}
    lane_groups = {}
    for row in test_rows:
        lane_groups.setdefault(str(row.get("lane_key") or "unknown"), []).append(row)
    for lane_key, items in sorted(lane_groups.items()):
        if len(items) < args.min_lane_count:
            continue
        by_lane[lane_key] = {
            "lane_label": items[0].get("lane_label"),
            "lane_prior": items[0].get("lane_prior"),
            **_group_report(items, top_ks),
        }

    summary = {
        "row_count": len(rows),
        "train_count": len(train_rows),
        "holdout_count": len(test_rows),
        "holdout_ratio": args.holdout_ratio,
        "top_ks": top_ks,
        "overall": _group_report(test_rows, top_ks),
        "by_lane": by_lane,
    }
    _write_json(args.output, summary)

    print(f"Rows total: {len(rows)}")
    print(f"Train rows: {len(train_rows)}")
    print(f"Holdout rows: {len(test_rows)}")
    for comparison in summary["overall"]["comparisons"]:
        print(
            f"overall top{comparison['top_k']}: "
            f"current_mean={comparison['current']['mean_best_runup_pct']} "
            f"baseline_mean={comparison['baseline']['mean_best_runup_pct']} "
            f"delta={comparison['delta_mean_best_runup_pct']}"
        )
    if by_lane:
        print("Lane holdout comparisons:")
        for lane_key, lane_summary in by_lane.items():
            first = lane_summary["comparisons"][0]
            print(
                f"- {lane_key}: count={lane_summary['count']} "
                f"top{first['top_k']}_delta={first['delta_mean_best_runup_pct']}"
            )


if __name__ == "__main__":
    main()
