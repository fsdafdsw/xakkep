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
    return (
        summary["roi"],
        summary["realized_pnl"],
        summary["total_trades"],
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
    parser.add_argument("--min-train-trades", type=int, default=1)
    parser.add_argument("--confidence-grid", default="0.85,0.90")
    parser.add_argument("--gross-edge-grid", default="0.015,0.02")
    parser.add_argument("--edge-threshold-grid", default="0.015,0.02")
    parser.add_argument("--watch-threshold-grid", default="0.01")
    parser.add_argument("--adjustment-scale-grid", default="0.04,0.06")
    parser.add_argument("--signals-per-event-grid", default="1")
    return parser.parse_args()


def main():
    args = parse_args()
    confidence_grid = _parse_float_grid(args.confidence_grid)
    gross_edge_grid = _parse_float_grid(args.gross_edge_grid)
    edge_threshold_grid = _parse_float_grid(args.edge_threshold_grid)
    watch_threshold_grid = _parse_float_grid(args.watch_threshold_grid)
    adjustment_scale_grid = _parse_float_grid(args.adjustment_scale_grid)
    signals_per_event_grid = _parse_int_grid(args.signals_per_event_grid)

    combos = list(
        itertools.product(
            confidence_grid,
            gross_edge_grid,
            edge_threshold_grid,
            watch_threshold_grid,
            adjustment_scale_grid,
            signals_per_event_grid,
        )
    )

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
        ) = combo
        env_overrides = {
            "MIN_CONFIDENCE": min_confidence,
            "MIN_GROSS_EDGE": min_gross_edge,
            "EDGE_THRESHOLD": edge_threshold,
            "WATCH_THRESHOLD": watch_threshold,
            "MODEL_ADJUSTMENT_SCALE": adjustment_scale,
            "MAX_SIGNALS_PER_EVENT": max_signals_per_event,
        }
        label = (
            f"train_{idx:02d}_"
            f"c{min_confidence}_g{min_gross_edge}_e{edge_threshold}_"
            f"w{watch_threshold}_a{adjustment_scale}_s{max_signals_per_event}"
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
