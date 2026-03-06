import argparse
from pathlib import Path

from calibration import fit_family_calibrators, save_calibrators
from calibration_report import load_jsonl, probability_summary
from config import REPORTS_DIR


def _apply_rows(rows, field, artifact):
    from calibration import apply_family_calibration

    calibrated = []
    for row in rows:
        updated = dict(row)
        family = row.get("market_type") or "unknown"
        updated[f"{field}_calibrated"] = apply_family_calibration(row.get(field), family, artifact)
        calibrated.append(updated)
    return calibrated


def default_output_path(dataset_path, probability_field):
    dataset_name = Path(dataset_path).stem
    return REPORTS_DIR / "artifacts" / f"calibration_{probability_field}_{dataset_name}.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Fit family-level calibrators from research JSONL.")
    parser.add_argument("--dataset", required=True, help="Path to research JSONL dataset.")
    parser.add_argument("--field", default="fair", choices=("market_implied", "fair", "fair_lcb"))
    parser.add_argument("--output", default=None, help="Where to write calibrator artifact JSON.")
    parser.add_argument("--min-rows", type=int, default=30)
    return parser.parse_args()


def main():
    args = parse_args()
    rows = load_jsonl(args.dataset)
    artifact = fit_family_calibrators(
        rows,
        probability_field=args.field,
        family_field="market_type",
        min_rows=args.min_rows,
    )
    output_path = Path(args.output) if args.output else default_output_path(args.dataset, args.field)
    save_calibrators(artifact, output_path)

    raw_summary = probability_summary(rows, args.field)
    calibrated_rows = _apply_rows(rows, args.field, artifact)
    calibrated_summary = probability_summary(calibrated_rows, f"{args.field}_calibrated")

    print(f"Rows loaded: {len(rows)}")
    print(f"Calibrator artifact written: {output_path}")
    print(
        f"raw {args.field}: "
        f"brier={raw_summary['brier_score']} "
        f"log_loss={raw_summary['log_loss']} "
        f"ece={raw_summary['ece']}"
    )
    print(
        f"calibrated {args.field}: "
        f"brier={calibrated_summary['brier_score']} "
        f"log_loss={calibrated_summary['log_loss']} "
        f"ece={calibrated_summary['ece']}"
    )


if __name__ == "__main__":
    main()
