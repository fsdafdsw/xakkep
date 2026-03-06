import argparse
import itertools
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
BACKTEST_SCRIPT = SCRIPT_DIR / "backtest.py"
REPORTS_DIR = SCRIPT_DIR / "reports" / "walkforward"


def _parse_float_grid(text):
    values = []
    for part in text.split(","):
        part = part.strip()
        if part:
            values.append(float(part))
    return values


def _parse_int_grid(text):
    values = []
    for part in text.split(","):
        part = part.strip()
        if part:
            values.append(int(part))
    return values


def _run_backtest(label, period_args, common_args, env_overrides):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS_DIR / f"{label}.json"
    env = os.environ.copy()
    env.update({key: str(value) for key, value in env_overrides.items()})

    cmd = [
        sys.executable,
        str(BACKTEST_SCRIPT),
        "--start-date",
        period_args["start_date"],
        "--end-date",
        period_args["end_date"],
        "--entry-hours-before-close",
        str(common_args.entry_hours_before_close),
        "--history-window-days",
        str(common_args.history_window_days),
        "--history-fidelity",
        str(common_args.history_fidelity),
        "--page-size",
        str(common_args.page_size),
        "--max-events-fetch",
        str(common_args.max_events_fetch),
        "--max-candidate-markets",
        str(common_args.max_candidate_markets),
        "--max-history-requests",
        str(common_args.max_history_requests),
        "--initial-bankroll",
        str(common_args.initial_bankroll),
        "--json-output",
        str(json_path),
    ]

    if common_args.start_offset is not None:
        cmd.extend(["--start-offset", str(common_args.start_offset)])
    if common_args.use_liquidity_filter:
        cmd.append("--use-liquidity-filter")

    result = subprocess.run(
        cmd,
        cwd=str(SCRIPT_DIR),
        env=env,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"backtest failed for {label}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    with json_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    payload["stdout_tail"] = result.stdout[-2000:]
    return payload


def _train_sort_key(item):
    summary = item["train"]["summary"]
    diagnostics = item["train"].get("diagnostics", {})
    stage_counts = diagnostics.get("stage_counts", {})
    return (
        summary["roi"],
        summary["realized_pnl"],
        summary["total_trades"],
        summary.get("candidate_count", 0),
        stage_counts.get("after_lcb_edge", 0),
        stage_counts.get("after_robustness", 0),
        -summary["max_drawdown"],
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Walk-forward optimizer for the Polymarket edge bot.")
    parser.add_argument("--train-start", required=True)
    parser.add_argument("--train-end", required=True)
    parser.add_argument("--test-start", required=True)
    parser.add_argument("--test-end", required=True)
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--max-events-fetch", type=int, default=2000)
    parser.add_argument("--max-candidate-markets", type=int, default=120)
    parser.add_argument("--max-history-requests", type=int, default=220)
    parser.add_argument("--initial-bankroll", type=float, default=10.0)
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--use-liquidity-filter", action="store_true")
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--min-train-trades", type=int, default=0)
    parser.add_argument("--confidence-grid", default="0.85,0.90")
    parser.add_argument("--gross-edge-grid", default="0.015,0.02")
    parser.add_argument("--edge-threshold-grid", default="0.015,0.02")
    parser.add_argument("--watch-threshold-grid", default="0.01")
    parser.add_argument("--adjustment-scale-grid", default="0.04,0.06")
    parser.add_argument("--signals-per-event-grid", default="1")
    parser.add_argument("--meta-confidence-grid", default="0.60,0.63")
    parser.add_argument("--graph-consistency-grid", default="0.50,0.52")
    parser.add_argument("--robustness-grid", default="0.58,0.60")
    parser.add_argument("--lcb-edge-grid", default="0.0")
    parser.add_argument("--watch-lcb-floor-grid", default="-0.020,-0.015")
    parser.add_argument("--uncertainty-base-grid", default="0.006,0.010")
    parser.add_argument("--uncertainty-confidence-grid", default="0.030,0.050")
    parser.add_argument("--uncertainty-external-grid", default="0.010,0.020")
    parser.add_argument("--uncertainty-anomaly-grid", default="0.015,0.020")
    parser.add_argument("--max-combos", type=int, default=24)
    return parser.parse_args()


def main():
    args = parse_args()
    confidence_grid = _parse_float_grid(args.confidence_grid)
    gross_edge_grid = _parse_float_grid(args.gross_edge_grid)
    edge_threshold_grid = _parse_float_grid(args.edge_threshold_grid)
    watch_threshold_grid = _parse_float_grid(args.watch_threshold_grid)
    adjustment_scale_grid = _parse_float_grid(args.adjustment_scale_grid)
    signals_per_event_grid = _parse_int_grid(args.signals_per_event_grid)
    meta_confidence_grid = _parse_float_grid(args.meta_confidence_grid)
    graph_consistency_grid = _parse_float_grid(args.graph_consistency_grid)
    robustness_grid = _parse_float_grid(args.robustness_grid)
    lcb_edge_grid = _parse_float_grid(args.lcb_edge_grid)
    watch_lcb_floor_grid = _parse_float_grid(args.watch_lcb_floor_grid)
    uncertainty_base_grid = _parse_float_grid(args.uncertainty_base_grid)
    uncertainty_confidence_grid = _parse_float_grid(args.uncertainty_confidence_grid)
    uncertainty_external_grid = _parse_float_grid(args.uncertainty_external_grid)
    uncertainty_anomaly_grid = _parse_float_grid(args.uncertainty_anomaly_grid)

    combos = list(
        itertools.product(
            confidence_grid,
            gross_edge_grid,
            edge_threshold_grid,
            watch_threshold_grid,
            adjustment_scale_grid,
            signals_per_event_grid,
            meta_confidence_grid,
            graph_consistency_grid,
            robustness_grid,
            lcb_edge_grid,
            watch_lcb_floor_grid,
            uncertainty_base_grid,
            uncertainty_confidence_grid,
            uncertainty_external_grid,
            uncertainty_anomaly_grid,
        )
    )
    if args.max_combos and len(combos) > args.max_combos:
        combos = combos[: args.max_combos]

    train_period = {"start_date": args.train_start, "end_date": args.train_end}
    test_period = {"start_date": args.test_start, "end_date": args.test_end}

    train_runs = []
    print(f"Train grid size: {len(combos)}")
    for idx, combo in enumerate(combos, start=1):
        (
            min_confidence,
            min_gross_edge,
            edge_threshold,
            watch_threshold,
            adjustment_scale,
            max_signals_per_event,
            min_meta_confidence,
            min_graph_consistency,
            min_robustness_score,
            min_lcb_edge,
            watch_lcb_floor,
            uncertainty_base,
            uncertainty_confidence_weight,
            uncertainty_external_weight,
            uncertainty_anomaly_weight,
        ) = combo
        env_overrides = {
            "MIN_CONFIDENCE": min_confidence,
            "MIN_GROSS_EDGE": min_gross_edge,
            "EDGE_THRESHOLD": edge_threshold,
            "WATCH_THRESHOLD": watch_threshold,
            "MODEL_ADJUSTMENT_SCALE": adjustment_scale,
            "MAX_SIGNALS_PER_EVENT": max_signals_per_event,
            "MIN_META_CONFIDENCE": min_meta_confidence,
            "MIN_GRAPH_CONSISTENCY": min_graph_consistency,
            "MIN_ROBUSTNESS_SCORE": min_robustness_score,
            "MIN_LCB_EDGE": min_lcb_edge,
            "WATCH_LCB_FLOOR": watch_lcb_floor,
            "UNCERTAINTY_BASE": uncertainty_base,
            "UNCERTAINTY_CONFIDENCE_WEIGHT": uncertainty_confidence_weight,
            "UNCERTAINTY_EXTERNAL_CONF_WEIGHT": uncertainty_external_weight,
            "UNCERTAINTY_ANOMALY_WEIGHT": uncertainty_anomaly_weight,
        }
        label = (
            f"train_{idx:02d}_"
            f"c{min_confidence}_g{min_gross_edge}_e{edge_threshold}_"
            f"w{watch_threshold}_a{adjustment_scale}_s{max_signals_per_event}_"
            f"mc{min_meta_confidence}_gc{min_graph_consistency}_rb{min_robustness_score}_"
            f"lc{min_lcb_edge}_wl{watch_lcb_floor}_ub{uncertainty_base}_"
            f"uc{uncertainty_confidence_weight}_ue{uncertainty_external_weight}_"
            f"ua{uncertainty_anomaly_weight}"
        ).replace(".", "_")
        print(f"[train {idx}/{len(combos)}] {env_overrides}")
        payload = _run_backtest(label, train_period, args, env_overrides)
        trades = payload["summary"]["total_trades"]
        if trades < args.min_train_trades:
            continue
        train_runs.append({"env": env_overrides, "train": payload})

    train_runs.sort(key=_train_sort_key, reverse=True)
    shortlisted = train_runs[: args.top_n]

    evaluated = []
    for idx, item in enumerate(shortlisted, start=1):
        env_overrides = item["env"]
        label = f"test_{idx:02d}".replace(".", "_")
        print(f"[test {idx}/{len(shortlisted)}] {env_overrides}")
        test_payload = _run_backtest(label, test_period, args, env_overrides)
        evaluated.append(
            {
                "env": env_overrides,
                "train": item["train"],
                "test": test_payload,
            }
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    latest_path = REPORTS_DIR / "latest_walkforward.json"
    run_path = REPORTS_DIR / f"walkforward_{timestamp}.json"
    payload = {
        "generated_at_utc": timestamp,
        "train_period": train_period,
        "test_period": test_period,
        "grid": {
            "MIN_CONFIDENCE": confidence_grid,
            "MIN_GROSS_EDGE": gross_edge_grid,
            "EDGE_THRESHOLD": edge_threshold_grid,
            "WATCH_THRESHOLD": watch_threshold_grid,
            "MODEL_ADJUSTMENT_SCALE": adjustment_scale_grid,
            "MAX_SIGNALS_PER_EVENT": signals_per_event_grid,
            "MIN_META_CONFIDENCE": meta_confidence_grid,
            "MIN_GRAPH_CONSISTENCY": graph_consistency_grid,
            "MIN_ROBUSTNESS_SCORE": robustness_grid,
            "MIN_LCB_EDGE": lcb_edge_grid,
            "WATCH_LCB_FLOOR": watch_lcb_floor_grid,
            "UNCERTAINTY_BASE": uncertainty_base_grid,
            "UNCERTAINTY_CONFIDENCE_WEIGHT": uncertainty_confidence_grid,
            "UNCERTAINTY_EXTERNAL_CONF_WEIGHT": uncertainty_external_grid,
            "UNCERTAINTY_ANOMALY_WEIGHT": uncertainty_anomaly_grid,
        },
        "common_args": {
            "start_offset": args.start_offset,
            "page_size": args.page_size,
            "max_events_fetch": args.max_events_fetch,
            "max_candidate_markets": args.max_candidate_markets,
            "max_history_requests": args.max_history_requests,
            "initial_bankroll": args.initial_bankroll,
            "entry_hours_before_close": args.entry_hours_before_close,
            "history_window_days": args.history_window_days,
            "history_fidelity": args.history_fidelity,
            "use_liquidity_filter": args.use_liquidity_filter,
            "top_n": args.top_n,
            "min_train_trades": args.min_train_trades,
            "max_combos": args.max_combos,
        },
        "train_candidates_evaluated": len(train_runs),
        "shortlisted": evaluated,
    }
    with latest_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=True)
    with run_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=True)

    print("\n=== Walk-forward Summary ===")
    print(f"Train candidates with enough trades: {len(train_runs)}")
    print(f"Shortlisted configs evaluated on test: {len(evaluated)}")
    if not evaluated:
        print("No configs met the minimum train-trade threshold.")
        return

    for idx, item in enumerate(evaluated, start=1):
        train_summary = item["train"]["summary"]
        test_summary = item["test"]["summary"]
        print(
            f"{idx}. env={item['env']} | "
            f"train_roi={train_summary['roi']:.2%} train_trades={train_summary['total_trades']} | "
            f"test_roi={test_summary['roi']:.2%} test_trades={test_summary['total_trades']}"
        )


if __name__ == "__main__":
    main()
