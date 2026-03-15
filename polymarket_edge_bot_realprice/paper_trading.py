import json
from datetime import datetime, timezone
from pathlib import Path

from config import (
    BANKROLL_USD,
    ESTIMATED_SLIPPAGE_BPS,
    PAPER_INITIAL_BANKROLL_USD,
    PAPER_MAX_BET_USD,
    PAPER_MAX_OPEN_POSITIONS,
    PAPER_MIN_TRADE_USD,
    PAPER_REENTRY_COOLDOWN_MINUTES,
    PAPER_REPORT_MAX_OPEN_POSITIONS,
    PAPER_STATE_DIR,
    TAKER_FEE_BPS,
)
from exit_policy import live_exit_plan
from utils import safe_float, safe_int


def _utc_now():
    return datetime.now(timezone.utc)


def _dt_from_ts(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _iso_utc(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _market_key_from_market(market):
    slug = market.get("event_slug") or market.get("slug") or market.get("id") or "unknown"
    token = market.get("selected_token_id") or market.get("token_yes") or ""
    return f"{slug}|{token}"


def _market_key_from_candidate(candidate):
    key = candidate.get("market_key")
    if key:
        return str(key)
    slug = candidate.get("event_slug") or candidate.get("link") or candidate.get("event_key") or candidate.get("question")
    token = candidate.get("selected_token_id") or ""
    return f"{slug}|{token}"


def _mark_price(market, default=None):
    for field in ("best_bid", "ref_price", "selected_price", "best_ask"):
        price = safe_float(market.get(field))
        if price is not None and price >= 0:
            return price
    return default


def _fee_rate():
    return (TAKER_FEE_BPS + ESTIMATED_SLIPPAGE_BPS) / 10000.0


def _default_state():
    return {
        "version": 1,
        "created_at_utc": _iso_utc(_utc_now()),
        "initial_bankroll_usd": PAPER_INITIAL_BANKROLL_USD,
        "cash_usd": PAPER_INITIAL_BANKROLL_USD,
        "realized_pnl_usd": 0.0,
        "positions": [],
        "recently_closed": {},
        "run_count": 0,
        "last_run_at_utc": None,
    }


def _paper_paths(state_dir=None):
    root = Path(state_dir or PAPER_STATE_DIR)
    return {
        "root": root,
        "state": root / "portfolio.json",
        "ledger": root / "ledger.jsonl",
        "runs": root / "runs.jsonl",
        "latest": root / "latest_summary.json",
    }


def load_state(state_dir=None):
    paths = _paper_paths(state_dir)
    path = paths["state"]
    if not path.exists():
        return _default_state()
    try:
        with path.open("r", encoding="utf-8") as fh:
            state = json.load(fh)
    except Exception:
        return _default_state()

    default = _default_state()
    default.update(state if isinstance(state, dict) else {})
    default["positions"] = list(default.get("positions") or [])
    default["recently_closed"] = dict(default.get("recently_closed") or {})
    return default


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=True)


def _append_jsonl(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")


def save_state(state, *, summary=None, state_dir=None):
    paths = _paper_paths(state_dir)
    _write_json(paths["state"], state)
    if summary is not None:
        _write_json(paths["latest"], summary)
        _append_jsonl(paths["runs"], summary)


def _sale_proceeds(shares, price):
    gross = shares * price
    return max(0.0, gross * (1.0 - _fee_rate()))


def _scaled_stake(candidate_stake, equity):
    if candidate_stake is None or candidate_stake <= 0:
        return 0.0
    reference_bankroll = BANKROLL_USD if BANKROLL_USD > 0 else 1000.0
    scaled = equity * (candidate_stake / reference_bankroll)
    return min(PAPER_MAX_BET_USD, max(0.0, scaled))


def _position_line(position, mark_price=None, unrealized_pnl=None):
    lane = position.get("lane_label") or position.get("lane_key") or "paper lane"
    parts = [position.get("question") or "Unknown market"]
    parts.append(f"BUY {position.get('selected_outcome') or 'outcome'} @ {position.get('entry_price', 0.0):.3f}")
    if mark_price is not None:
        parts.append(f"mark {mark_price:.3f}")
    if unrealized_pnl is not None:
        sign = "+" if unrealized_pnl >= 0 else ""
        parts.append(f"PnL {sign}${unrealized_pnl:.2f}")
    parts.append(lane)
    return " | ".join(parts)


def _open_position(state, candidate, now_dt, ledger_rows):
    if len(state["positions"]) >= PAPER_MAX_OPEN_POSITIONS:
        return None

    market_key = _market_key_from_candidate(candidate)
    recent_close_ts = safe_int(state.get("recently_closed", {}).get(market_key))
    now_ts = int(now_dt.timestamp())
    cooldown_seconds = int(PAPER_REENTRY_COOLDOWN_MINUTES * 60)
    if recent_close_ts and now_ts - recent_close_ts < cooldown_seconds:
        return None

    if any(position.get("market_key") == market_key for position in state["positions"]):
        return None

    entry_price = safe_float(candidate.get("entry"))
    if entry_price is None or entry_price <= 0:
        return None

    equity = safe_float(state.get("cash_usd"), default=0.0)
    for position in state["positions"]:
        mark = safe_float(position.get("last_mark_price"), default=position.get("entry_price"))
        equity += _sale_proceeds(position.get("shares", 0.0), mark)

    desired_stake = _scaled_stake(safe_float(candidate.get("stake_usd")), equity)
    if desired_stake < PAPER_MIN_TRADE_USD:
        desired_stake = PAPER_MIN_TRADE_USD

    shares = desired_stake / entry_price
    additional_cost_per_share = safe_float(candidate.get("cost_per_share"), default=0.0)
    total_outlay = desired_stake + (shares * additional_cost_per_share)
    cash = safe_float(state.get("cash_usd"), default=0.0)
    if total_outlay > cash or total_outlay <= 0:
        return None

    exit_plan = live_exit_plan(
        candidate.get("domain_action_family"),
        repricing_verdict=candidate.get("repricing_verdict"),
        entry_price=entry_price,
        catalyst_type=candidate.get("catalyst_type"),
    )
    time_stop_days = safe_float(exit_plan.get("time_stop_days"), default=7.0)
    time_stop_ts = int(now_ts + (time_stop_days * 24 * 3600))

    position = {
        "position_id": f"{market_key}:{now_ts}",
        "market_key": market_key,
        "market_id": candidate.get("market_id"),
        "event_slug": candidate.get("event_slug"),
        "link": candidate.get("link"),
        "question": candidate.get("question"),
        "selected_outcome": candidate.get("selected_outcome"),
        "entry_price": entry_price,
        "shares": shares,
        "stake_usd": desired_stake,
        "total_outlay_usd": total_outlay,
        "opened_ts": now_ts,
        "opened_at_utc": _iso_utc(now_dt),
        "action_family": candidate.get("domain_action_family"),
        "catalyst_type": candidate.get("catalyst_type"),
        "lane_key": candidate.get("repricing_lane_key"),
        "lane_label": candidate.get("repricing_lane_label"),
        "take_profit_price": safe_float(exit_plan.get("take_profit_price")),
        "stop_loss_price": safe_float(exit_plan.get("stop_loss_price")),
        "stop_activation_hours": safe_float(exit_plan.get("stop_activation_hours"), default=12.0),
        "time_stop_days": time_stop_days,
        "time_stop_ts": time_stop_ts,
        "trailing_arm_pct": safe_float(exit_plan.get("trailing_arm_pct"), default=0.20),
        "trailing_drawdown_pct": safe_float(exit_plan.get("trailing_drawdown_pct"), default=0.18),
        "max_mark_price": entry_price,
        "last_mark_price": entry_price,
    }

    state["cash_usd"] = cash - total_outlay
    state["positions"].append(position)
    ledger_rows.append(
        {
            "ts_utc": _iso_utc(now_dt),
            "kind": "open",
            "market_key": market_key,
            "question": position["question"],
            "selected_outcome": position["selected_outcome"],
            "entry_price": entry_price,
            "stake_usd": desired_stake,
            "total_outlay_usd": total_outlay,
            "lane_key": position.get("lane_key"),
            "catalyst_type": position.get("catalyst_type"),
        }
    )
    return position


def _close_position(state, position, mark_price, reason, now_dt, ledger_rows):
    proceeds = _sale_proceeds(position.get("shares", 0.0), mark_price)
    pnl = proceeds - safe_float(position.get("total_outlay_usd"), default=0.0)
    state["cash_usd"] = safe_float(state.get("cash_usd"), default=0.0) + proceeds
    state["realized_pnl_usd"] = safe_float(state.get("realized_pnl_usd"), default=0.0) + pnl
    state.setdefault("recently_closed", {})[position["market_key"]] = int(now_dt.timestamp())
    ledger_rows.append(
        {
            "ts_utc": _iso_utc(now_dt),
            "kind": "close",
            "market_key": position["market_key"],
            "question": position.get("question"),
            "selected_outcome": position.get("selected_outcome"),
            "exit_price": mark_price,
            "pnl_usd": pnl,
            "reason": reason,
            "lane_key": position.get("lane_key"),
            "catalyst_type": position.get("catalyst_type"),
        }
    )
    return {
        "question": position.get("question"),
        "selected_outcome": position.get("selected_outcome"),
        "exit_price": mark_price,
        "pnl_usd": pnl,
        "reason": reason,
        "lane_label": position.get("lane_label") or position.get("lane_key"),
        "link": position.get("link"),
    }


def _update_positions(state, market_index, now_dt, ledger_rows):
    remaining = []
    closed_rows = []
    now_ts = int(now_dt.timestamp())

    for position in state["positions"]:
        market = market_index.get(position["market_key"])
        current_mark = _mark_price(market or {}, default=safe_float(position.get("last_mark_price"), default=position.get("entry_price")))
        if current_mark is None:
            current_mark = safe_float(position.get("entry_price"), default=0.0)

        position["last_mark_price"] = current_mark
        position["max_mark_price"] = max(
            safe_float(position.get("max_mark_price"), default=current_mark),
            current_mark,
        )

        take_profit = safe_float(position.get("take_profit_price"))
        stop_loss = safe_float(position.get("stop_loss_price"))
        stop_activation_ts = int(
            safe_float(position.get("opened_ts"), default=now_ts)
            + (safe_float(position.get("stop_activation_hours"), default=12.0) * 3600)
        )
        time_stop_ts = safe_int(position.get("time_stop_ts"), default=now_ts)
        trailing_arm_pct = safe_float(position.get("trailing_arm_pct"), default=0.20)
        trailing_drawdown_pct = safe_float(position.get("trailing_drawdown_pct"), default=0.18)
        trailing_active = position["max_mark_price"] >= (
            safe_float(position.get("entry_price"), default=current_mark) * (1.0 + trailing_arm_pct)
        )
        trailing_stop = position["max_mark_price"] * (1.0 - trailing_drawdown_pct)

        close_reason = None
        if take_profit is not None and current_mark >= take_profit:
            close_reason = "take_profit"
        elif trailing_active and current_mark <= trailing_stop:
            close_reason = "trailing_stop"
        elif stop_loss is not None and now_ts >= stop_activation_ts and current_mark <= stop_loss:
            close_reason = "stop_loss"
        elif now_ts >= time_stop_ts:
            close_reason = "time_stop"

        if close_reason:
            closed_rows.append(_close_position(state, position, current_mark, close_reason, now_dt, ledger_rows))
        else:
            remaining.append(position)

    state["positions"] = remaining
    return closed_rows


def _state_snapshot(state):
    cash = safe_float(state.get("cash_usd"), default=0.0)
    fee_rate = _fee_rate()
    open_positions = []
    unrealized = 0.0

    for position in state["positions"]:
        mark = safe_float(position.get("last_mark_price"), default=position.get("entry_price"))
        proceeds = _sale_proceeds(position.get("shares", 0.0), mark)
        pnl = proceeds - safe_float(position.get("total_outlay_usd"), default=0.0)
        unrealized += pnl
        open_positions.append(
            {
                "question": position.get("question"),
                "selected_outcome": position.get("selected_outcome"),
                "entry_price": safe_float(position.get("entry_price"), default=0.0),
                "mark_price": mark,
                "unrealized_pnl_usd": pnl,
                "lane_label": position.get("lane_label") or position.get("lane_key"),
                "link": position.get("link"),
                "opened_at_utc": position.get("opened_at_utc"),
                "max_mark_price": safe_float(position.get("max_mark_price"), default=mark),
                "stake_usd": safe_float(position.get("stake_usd"), default=0.0),
            }
        )

    equity = cash + sum(
        _sale_proceeds(position.get("shares", 0.0), safe_float(position.get("last_mark_price"), default=position.get("entry_price")))
        for position in state["positions"]
    )
    return {
        "cash_usd": cash,
        "equity_usd": equity,
        "realized_pnl_usd": safe_float(state.get("realized_pnl_usd"), default=0.0),
        "unrealized_pnl_usd": unrealized,
        "open_positions": sorted(open_positions, key=lambda row: row["unrealized_pnl_usd"], reverse=True),
        "fee_rate": fee_rate,
    }


def _format_report(summary):
    start_bank = summary["initial_bankroll_usd"]
    equity = summary["equity_usd"]
    change = equity - start_bank
    change_pct = (change / start_bank) * 100.0 if start_bank > 0 else 0.0
    sign = "+" if change >= 0 else ""

    realized = summary["realized_pnl_usd"]
    unrealized = summary["unrealized_pnl_usd"]
    realized_sign = "+" if realized >= 0 else "-"
    unrealized_sign = "+" if unrealized >= 0 else "-"

    lines = [
        f"Polymarket paper bot - {summary['generated_at_utc']}",
        "",
        f"Bank: ${start_bank:.2f} -> ${equity:.2f} ({sign}{change_pct:.2f}%)",
        f"Cash: ${summary['cash_usd']:.2f} | Open positions: {summary['open_position_count']} | Realized: {realized_sign}${abs(realized):.2f} | Unrealized: {unrealized_sign}${abs(unrealized):.2f}",
        "",
        "Opened this run",
    ]

    if summary["opened"]:
        for idx, row in enumerate(summary["opened"], start=1):
            lines.append(
                f"{idx}. {row['question']} | BUY {row['selected_outcome']} @ {row['entry_price']:.3f} | Stake ${row['stake_usd']:.2f} | {row['lane_label']}"
            )
    else:
        lines.append("none")

    lines.extend(["", "Closed this run"])
    if summary["closed"]:
        for idx, row in enumerate(summary["closed"], start=1):
            pnl = row["pnl_usd"]
            sign = "+" if pnl >= 0 else "-"
            lines.append(
                f"{idx}. {row['question']} | EXIT @ {row['exit_price']:.3f} | PnL {sign}${abs(pnl):.2f} | {row['reason']}"
            )
    else:
        lines.append("none")

    lines.extend(["", "Open positions"])
    open_rows = summary["open_positions"][:PAPER_REPORT_MAX_OPEN_POSITIONS]
    if open_rows:
        for idx, row in enumerate(open_rows, start=1):
            sign = "+" if row["unrealized_pnl_usd"] >= 0 else "-"
            lines.append(
                f"{idx}. {row['question']} | BUY {row['selected_outcome']} @ {row['entry_price']:.3f} | Mark {row['mark_price']:.3f} | PnL {sign}${abs(row['unrealized_pnl_usd']):.2f}"
            )
    else:
        lines.append("none")

    return "\n".join(lines)


def run_paper_cycle(markets, buy_candidates, *, state_dir=None, generated_at_utc=None):
    now_dt = _utc_now()
    state = load_state(state_dir=state_dir)
    state["run_count"] = safe_int(state.get("run_count"), default=0) + 1
    state["last_run_at_utc"] = _iso_utc(now_dt)

    market_index = {_market_key_from_market(market): market for market in markets}
    ledger_rows = []
    closed = _update_positions(state, market_index, now_dt, ledger_rows)

    opened_positions = []
    for candidate in buy_candidates:
        position = _open_position(state, candidate, now_dt, ledger_rows)
        if position:
            opened_positions.append(
                {
                    "question": position.get("question"),
                    "selected_outcome": position.get("selected_outcome"),
                    "entry_price": safe_float(position.get("entry_price"), default=0.0),
                    "stake_usd": safe_float(position.get("stake_usd"), default=0.0),
                    "lane_label": position.get("lane_label") or position.get("lane_key"),
                    "link": position.get("link"),
                }
            )

    snapshot = _state_snapshot(state)
    summary = {
        "generated_at_utc": generated_at_utc or _iso_utc(now_dt),
        "initial_bankroll_usd": safe_float(state.get("initial_bankroll_usd"), default=PAPER_INITIAL_BANKROLL_USD),
        "cash_usd": snapshot["cash_usd"],
        "equity_usd": snapshot["equity_usd"],
        "realized_pnl_usd": snapshot["realized_pnl_usd"],
        "unrealized_pnl_usd": snapshot["unrealized_pnl_usd"],
        "open_position_count": len(snapshot["open_positions"]),
        "opened": opened_positions,
        "closed": closed,
        "open_positions": snapshot["open_positions"],
        "run_count": state["run_count"],
    }

    save_state(state, summary=summary, state_dir=state_dir)
    for row in ledger_rows:
        _append_jsonl(_paper_paths(state_dir)["ledger"], row)

    return {
        "state": state,
        "summary": summary,
        "report_text": _format_report(summary),
    }
