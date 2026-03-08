import argparse
from pathlib import Path

from calibration_report import load_jsonl
from config import REPORTS_DIR
from meta_model import (
    brier_score,
    fit_meta_model,
    log_loss,
    save_meta_model,
    score_meta_rows,
)


def default_output_path(dataset_path, label_field):
    dataset_name = Path(dataset_path).stem
    return REPORTS_DIR / "artifacts" / f"meta_model_{label_field}_{dataset_name}.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Fit additive scorecard meta model from meta dataset JSONL.")
    parser.add_argument("--dataset", required=True, help="Path to meta dataset JSONL.")
    parser.add_argument("--output", default=None, help="Where to write meta model artifact JSON.")
    parser.add_argument("--label-field", default="label_trade_positive")
    parser.add_argument("--bin-count", type=int, default=5)
    parser.add_argument("--smoothing", type=float, default=8.0)
    parser.add_argument("--min-bucket-rows", type=int, default=4)
    return parser.parse_args()


def main():
    args = parse_args()
    rows = load_jsonl(args.dataset)
    artifact = fit_meta_model(
        rows,
        label_field=args.label_field,
        bin_count=args.bin_count,
        smoothing=args.smoothing,
        min_bucket_rows=args.min_bucket_rows,
    )
    output_path = Path(args.output) if args.output else default_output_path(args.dataset, args.label_field)
    save_meta_model(artifact, output_path)

    base_rate = artifact["base_rate"]
    baseline_rows = []
    for row in rows:
        updated = dict(row)
        updated["baseline_prob"] = base_rate
        baseline_rows.append(updated)
    scored_rows = score_meta_rows(rows, artifact)

    print(f"Rows loaded: {len(rows)}")
    print(f"Meta model artifact written: {output_path}")
    print(
        f"baseline: brier={brier_score(baseline_rows, 'baseline_prob', args.label_field)} "
        f"log_loss={log_loss(baseline_rows, 'baseline_prob', args.label_field)}"
    )
    print(
        f"meta_model: brier={brier_score(scored_rows, 'meta_trade_prob', args.label_field)} "
        f"log_loss={log_loss(scored_rows, 'meta_trade_prob', args.label_field)}"
    )


if __name__ == "__main__":
    main()
