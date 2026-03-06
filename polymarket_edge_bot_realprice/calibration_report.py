import argparse
import json
import math
from collections import defaultdict
from pathlib import Path

from config import REPORTS_DIR


_PROBABILITY_FIELDS = ("market_implied", "fair", "fair_lcb")
_FAMILY_FIELD = "market_type"


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value, low=1e-6, high=1 - 1e-6):
    return max(low, min(high, value))


def load_jsonl(path):
    rows = []
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def brier_score(rows, probability_field):
    values = []
    for row in rows:
        p = _safe_float(row.get(probability_field))
        y = _safe_float(row.get("resolved_outcome"))
        if p is None or y is None:
            continue
        values.append((p - y) ** 2)
    return (sum(values) / len(values)) if values else None


def log_loss(rows, probability_field):
    values = []
    for row in rows:
        p = _safe_float(row.get(probability_field))
        y = _safe_float(row.get("resolved_outcome"))
        if p is None or y is None:
            continue
        p = _clamp(p)
        values.append(-(y * math.log(p) + ((1 - y) * math.log(1 - p))))
    return (sum(values) / len(values)) if values else None


def calibration_bins(rows, probability_field, bin_count=10):
    bins = []
    abs_errors = []
    for idx in range(bin_count):
        low = idx / bin_count
        high = (idx + 1) / bin_count
        bucket = []
        for row in rows:
            p = _safe_float(row.get(probability_field))
            y = _safe_float(row.get("resolved_outcome"))
            if p is None or y is None:
                continue
            if idx == bin_count - 1:
                in_bin = low <= p <= high
            else:
                in_bin = low <= p < high
            if in_bin:
                bucket.append((p, y))

        if not bucket:
            bins.append(
                {
                    "range": [round(low, 3), round(high, 3)],
                    "count": 0,
                    "mean_probability": None,
                    "mean_outcome": None,
                    "calibration_gap": None,
                }
            )
            continue

        mean_probability = sum(p for p, _ in bucket) / len(bucket)
        mean_outcome = sum(y for _, y in bucket) / len(bucket)
        gap = mean_probability - mean_outcome
        abs_errors.append(abs(gap) * (len(bucket) / max(1, len(rows))))
        bins.append(
            {
                "range": [round(low, 3), round(high, 3)],
                "count": len(bucket),
                "mean_probability": mean_probability,
                "mean_outcome": mean_outcome,
                "calibration_gap": gap,
            }
        )

    return bins, (sum(abs_errors) if abs_errors else None)


def probability_summary(rows, probability_field):
    score = brier_score(rows, probability_field)
    loss = log_loss(rows, probability_field)
    bins, ece = calibration_bins(rows, probability_field)
    usable = [
        (
            _safe_float(row.get(probability_field)),
            _safe_float(row.get("resolved_outcome")),
        )
        for row in rows
        if _safe_float(row.get(probability_field)) is not None and _safe_float(row.get("resolved_outcome")) is not None
    ]
    mean_probability = (sum(p for p, _ in usable) / len(usable)) if usable else None
    base_rate = (sum(y for _, y in usable) / len(usable)) if usable else None
    return {
        "count": len(usable),
        "mean_probability": mean_probability,
        "base_rate": base_rate,
        "brier_score": score,
        "log_loss": loss,
        "ece": ece,
        "bins": bins,
    }


def build_report(rows):
    report = {
        "row_count": len(rows),
        "families": {},
        "overall": {},
    }

    for field in _PROBABILITY_FIELDS:
        report["overall"][field] = probability_summary(rows, field)

    grouped = defaultdict(list)
    for row in rows:
        family = row.get(_FAMILY_FIELD) or "unknown"
        grouped[family].append(row)

    for family, family_rows in sorted(grouped.items()):
        report["families"][family] = {
            "row_count": len(family_rows),
            "decision_status_counts": dict(sorted(_count_values(family_rows, "decision_status").items())),
            "reject_reason_counts": dict(sorted(_count_values(family_rows, "reject_reason").items())),
            "probabilities": {},
        }
        for field in _PROBABILITY_FIELDS:
            report["families"][family]["probabilities"][field] = probability_summary(family_rows, field)

    return report


def _count_values(rows, field):
    counts = defaultdict(int)
    for row in rows:
        value = row.get(field)
        if value is None:
            value = "null"
        counts[str(value)] += 1
    return counts


def default_output_path(dataset_path):
    dataset_name = Path(dataset_path).stem
    return REPORTS_DIR / "research" / f"calibration_{dataset_name}.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Build calibration report from research snapshot JSONL.")
    parser.add_argument("--dataset", required=True, help="Path to research JSONL dataset.")
    parser.add_argument("--output", default=None, help="Where to write JSON report.")
    return parser.parse_args()


def main():
    args = parse_args()
    rows = load_jsonl(args.dataset)
    report = build_report(rows)
    output_path = Path(args.output) if args.output else default_output_path(args.dataset)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=True)

    print(f"Rows loaded: {len(rows)}")
    print(f"Calibration report written: {output_path}")
    for field in _PROBABILITY_FIELDS:
        summary = report["overall"][field]
        print(
            f"{field}: count={summary['count']} "
            f"brier={summary['brier_score']} "
            f"log_loss={summary['log_loss']} "
            f"ece={summary['ece']}"
        )


if __name__ == "__main__":
    main()
