import argparse
import json
from pathlib import Path

from calibration_report import load_jsonl
from config import REPORTS_DIR
from meta_model import brier_score, fit_meta_model, log_loss, score_meta_rows


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


def _count(rows, field):
    counts = {}
    for row in rows:
        key = str(row.get(field))
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _sort_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            _safe_int(row.get("entry_ts"), 0) or 0,
            str(row.get("snapshot_id") or ""),
        ),
    )


def _baseline_rows(rows, base_rate):
    updated = []
    for row in rows:
        item = dict(row)
        item["baseline_prob"] = base_rate
        updated.append(item)
    return updated


def _classification_metrics(rows, probability_field, label_field, threshold):
    predicted = 0
    positives = 0
    true_positive = 0
    false_positive = 0
    false_negative = 0

    for row in rows:
        probability = _safe_float(row.get(probability_field))
        label = _safe_float(row.get(label_field))
        if probability is None or label is None:
            continue
        positives += int(label > 0.5)
        flag = probability >= threshold
        if flag:
            predicted += 1
            if label > 0.5:
                true_positive += 1
            else:
                false_positive += 1
        elif label > 0.5:
            false_negative += 1

    precision = (true_positive / predicted) if predicted else None
    recall = (true_positive / positives) if positives else None
    return {
        "threshold": threshold,
        "predicted_count": predicted,
        "positive_count": positives,
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "precision": precision,
        "recall": recall,
    }


def _threshold_score(metrics, base_rate, min_predicted):
    predicted = metrics["predicted_count"]
    precision = metrics["precision"]
    if precision is None or predicted < min_predicted:
        return float("-inf")
    uplift = precision - base_rate
    return uplift * predicted


def choose_threshold(train_rows, label_field, thresholds, base_rate, min_predicted):
    best = None
    best_score = float("-inf")
    for threshold in thresholds:
        metrics = _classification_metrics(train_rows, "meta_trade_prob", label_field, threshold)
        score = _threshold_score(metrics, base_rate=base_rate, min_predicted=min_predicted)
        metrics["selection_score"] = score
        if score > best_score:
            best_score = score
            best = metrics
    return best


def _probability_metrics(rows, probability_field, label_field):
    return {
        "count": len(rows),
        "brier_score": brier_score(rows, probability_field, label_field),
        "log_loss": log_loss(rows, probability_field, label_field),
    }


def evaluate_split(train_rows, test_rows, label_field, thresholds, min_predicted, bin_count, smoothing, min_bucket_rows):
    artifact = fit_meta_model(
        train_rows,
        label_field=label_field,
        bin_count=bin_count,
        smoothing=smoothing,
        min_bucket_rows=min_bucket_rows,
    )

    train_scored = score_meta_rows(train_rows, artifact)
    test_scored = score_meta_rows(test_rows, artifact)
    base_rate = artifact["base_rate"]

    train_baseline = _baseline_rows(train_rows, base_rate)
    test_baseline = _baseline_rows(test_rows, base_rate)

    chosen_threshold = choose_threshold(
        train_scored,
        label_field=label_field,
        thresholds=thresholds,
        base_rate=base_rate,
        min_predicted=min_predicted,
    )

    threshold = chosen_threshold["threshold"] if chosen_threshold else thresholds[0]
    train_class = _classification_metrics(train_scored, "meta_trade_prob", label_field, threshold)
    test_class = _classification_metrics(test_scored, "meta_trade_prob", label_field, threshold)

    return {
        "artifact_summary": {
            "row_count": artifact["row_count"],
            "base_rate": artifact["base_rate"],
            "bin_count": artifact["bin_count"],
            "smoothing": artifact["smoothing"],
            "min_bucket_rows": artifact["min_bucket_rows"],
        },
        "train": {
            "baseline": _probability_metrics(train_baseline, "baseline_prob", label_field),
            "meta_model": _probability_metrics(train_scored, "meta_trade_prob", label_field),
            "classification": train_class,
        },
        "test": {
            "baseline": _probability_metrics(test_baseline, "baseline_prob", label_field),
            "meta_model": _probability_metrics(test_scored, "meta_trade_prob", label_field),
            "classification": test_class,
        },
        "chosen_threshold": chosen_threshold,
    }


def build_holdout(rows, train_fraction):
    split_idx = max(1, min(len(rows) - 1, int(len(rows) * train_fraction)))
    return rows[:split_idx], rows[split_idx:]


def build_walkforward_splits(rows, folds, min_train_rows):
    total = len(rows)
    remaining = total - min_train_rows
    if remaining <= 0:
        return []
    test_size = max(1, remaining // folds)
    splits = []
    train_end = min_train_rows
    while train_end < total:
        test_end = min(total, train_end + test_size)
        splits.append((rows[:train_end], rows[train_end:test_end]))
        train_end = test_end
    return splits


def _weighted_average(items, path):
    weighted = []
    total_weight = 0
    for item in items:
        cursor = item
        for key in path:
            cursor = cursor.get(key) if isinstance(cursor, dict) else None
        value = _safe_float(cursor)
        weight = _safe_float(item.get("weight"), 0.0) or 0.0
        if value is None or weight <= 0:
            continue
        weighted.append(value * weight)
        total_weight += weight
    return (sum(weighted) / total_weight) if total_weight else None


def summarize_walkforward(folds_payload):
    weighted_rows = []
    for idx, fold in enumerate(folds_payload, start=1):
        weighted_rows.append(
            {
                "fold": idx,
                "weight": fold["test"]["meta_model"]["count"],
                "baseline_brier": fold["test"]["baseline"]["brier_score"],
                "meta_brier": fold["test"]["meta_model"]["brier_score"],
                "baseline_log_loss": fold["test"]["baseline"]["log_loss"],
                "meta_log_loss": fold["test"]["meta_model"]["log_loss"],
                "precision": fold["test"]["classification"]["precision"],
                "predicted_count": fold["test"]["classification"]["predicted_count"],
            }
        )

    predicted_total = sum(int(item.get("predicted_count") or 0) for item in weighted_rows)
    return {
        "fold_count": len(folds_payload),
        "weighted_test_brier_baseline": _weighted_average(weighted_rows, ("baseline_brier",)),
        "weighted_test_brier_meta": _weighted_average(weighted_rows, ("meta_brier",)),
        "weighted_test_log_loss_baseline": _weighted_average(weighted_rows, ("baseline_log_loss",)),
        "weighted_test_log_loss_meta": _weighted_average(weighted_rows, ("meta_log_loss",)),
        "weighted_test_precision": _weighted_average(weighted_rows, ("precision",)),
        "total_predicted_positive": predicted_total,
    }


def _label_counts(rows, label_field):
    counts = {}
    for row in rows:
        label = row.get(label_field)
        key = str(int(float(label))) if label not in (None, "") else "None"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _evaluate_rows(rows, *, label_field, thresholds, train_fraction, walkforward_folds, min_train_rows, min_predicted, bin_count, smoothing, min_bucket_rows):
    train_rows, test_rows = build_holdout(rows, train_fraction)
    holdout = evaluate_split(
        train_rows,
        test_rows,
        label_field=label_field,
        thresholds=thresholds,
        min_predicted=min_predicted,
        bin_count=bin_count,
        smoothing=smoothing,
        min_bucket_rows=min_bucket_rows,
    )

    wf_splits = build_walkforward_splits(rows, folds=walkforward_folds, min_train_rows=min_train_rows)
    walkforward_folds_payload = []
    for train_split, test_split in wf_splits:
        walkforward_folds_payload.append(
            evaluate_split(
                train_split,
                test_split,
                label_field=label_field,
                thresholds=thresholds,
                min_predicted=min_predicted,
                bin_count=bin_count,
                smoothing=smoothing,
                min_bucket_rows=min_bucket_rows,
            )
        )

    return {
        "row_count": len(rows),
        "label_counts": _label_counts(rows, label_field),
        "market_type_counts": _count(rows, "market_type"),
        "reject_reason_counts": _count(rows, "reject_reason"),
        "holdout": holdout,
        "walkforward": {
            "folds": walkforward_folds_payload,
            "summary": summarize_walkforward(walkforward_folds_payload),
        },
    }


def _family_reports(rows, *, label_field, thresholds, train_fraction, walkforward_folds, min_train_rows, min_predicted, bin_count, smoothing, min_bucket_rows):
    reports = {}
    minimum_rows = max(min_train_rows + max(4, walkforward_folds), 16)

    family_names = sorted({str(row.get("market_type") or "unknown") for row in rows})
    for family in family_names:
        family_rows = [row for row in rows if str(row.get("market_type") or "unknown") == family]
        label_counts = _label_counts(family_rows, label_field)
        unique_labels = {key for key, value in label_counts.items() if value > 0 and key != "None"}
        if len(family_rows) < minimum_rows:
            reports[family] = {
                "row_count": len(family_rows),
                "label_counts": label_counts,
                "skipped": f"insufficient_rows<{minimum_rows}",
            }
            continue
        if len(unique_labels) < 2:
            reports[family] = {
                "row_count": len(family_rows),
                "label_counts": label_counts,
                "skipped": "single_label_family",
            }
            continue
        reports[family] = _evaluate_rows(
            family_rows,
            label_field=label_field,
            thresholds=thresholds,
            train_fraction=train_fraction,
            walkforward_folds=walkforward_folds,
            min_train_rows=min_train_rows,
            min_predicted=min_predicted,
            bin_count=bin_count,
            smoothing=smoothing,
            min_bucket_rows=min_bucket_rows,
        )

    return reports


def default_output_path(dataset_path):
    dataset_name = Path(dataset_path).stem
    return REPORTS_DIR / "research" / f"meta_model_eval_{dataset_name}.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate meta model with chronological holdout and walk-forward.")
    parser.add_argument("--dataset", required=True, help="Path to meta dataset JSONL.")
    parser.add_argument("--output", default=None, help="Where to write evaluation JSON.")
    parser.add_argument("--label-field", default="label_trade_positive")
    parser.add_argument("--train-fraction", type=float, default=0.7)
    parser.add_argument("--walkforward-folds", type=int, default=3)
    parser.add_argument("--min-train-rows", type=int, default=24)
    parser.add_argument("--thresholds", default="0.50,0.55,0.58,0.60,0.65,0.70")
    parser.add_argument("--min-predicted", type=int, default=3)
    parser.add_argument("--bin-count", type=int, default=5)
    parser.add_argument("--smoothing", type=float, default=8.0)
    parser.add_argument("--min-bucket-rows", type=int, default=4)
    return parser.parse_args()


def main():
    args = parse_args()
    rows = _sort_rows(load_jsonl(args.dataset))
    thresholds = [float(item.strip()) for item in args.thresholds.split(",") if item.strip()]
    report = _evaluate_rows(
        rows,
        label_field=args.label_field,
        thresholds=thresholds,
        train_fraction=args.train_fraction,
        walkforward_folds=args.walkforward_folds,
        min_train_rows=args.min_train_rows,
        min_predicted=args.min_predicted,
        bin_count=args.bin_count,
        smoothing=args.smoothing,
        min_bucket_rows=args.min_bucket_rows,
    )
    report["label_field"] = args.label_field
    report["families"] = _family_reports(
        rows,
        label_field=args.label_field,
        thresholds=thresholds,
        train_fraction=args.train_fraction,
        walkforward_folds=args.walkforward_folds,
        min_train_rows=args.min_train_rows,
        min_predicted=args.min_predicted,
        bin_count=args.bin_count,
        smoothing=args.smoothing,
        min_bucket_rows=args.min_bucket_rows,
    )

    output_path = Path(args.output) if args.output else default_output_path(args.dataset)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=True)

    holdout = report["holdout"]
    holdout_test = holdout["test"]
    wf_summary = report["walkforward"]["summary"]
    print(f"Rows loaded: {len(rows)}")
    print(f"Evaluation report written: {output_path}")
    print(
        f"Holdout test brier: baseline={holdout_test['baseline']['brier_score']} "
        f"meta={holdout_test['meta_model']['brier_score']}"
    )
    print(
        f"Holdout test log_loss: baseline={holdout_test['baseline']['log_loss']} "
        f"meta={holdout_test['meta_model']['log_loss']}"
    )
    print(
        f"Holdout threshold={holdout['chosen_threshold']['threshold'] if holdout['chosen_threshold'] else None} "
        f"precision={holdout_test['classification']['precision']} "
        f"predicted={holdout_test['classification']['predicted_count']}"
    )
    print(
        f"Walkforward weighted brier: baseline={wf_summary['weighted_test_brier_baseline']} "
        f"meta={wf_summary['weighted_test_brier_meta']}"
    )
    print(
        f"Walkforward weighted log_loss: baseline={wf_summary['weighted_test_log_loss_baseline']} "
        f"meta={wf_summary['weighted_test_log_loss_meta']}"
    )
    print(
        f"Walkforward weighted precision={wf_summary['weighted_test_precision']} "
        f"predicted_total={wf_summary['total_predicted_positive']}"
    )


if __name__ == "__main__":
    main()
