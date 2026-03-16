import json
from datetime import datetime, timezone
from pathlib import Path

from config import (
    BANKROLL_USD,
    ESTIMATED_SLIPPAGE_BPS,
    PAPER_DAILY_STOP_LOSS_USD,
    PAPER_INITIAL_BANKROLL_USD,
    PAPER_MAX_BET_USD,
    PAPER_MAX_OPEN_POSITIONS,
    PAPER_MIN_TRADE_USD,
    PAPER_REPORT_MAX_IDEAS,
    PAPER_REENTRY_COOLDOWN_MINUTES,
    PAPER_RADAR_SCOUT_ENABLED,
    PAPER_RADAR_SCOUT_MAX_ENTRY_PRICE,
    PAPER_RADAR_SCOUT_MIN_ATTENTION_GAP,
    PAPER_RADAR_SCOUT_MIN_CONFIDENCE,
    PAPER_RADAR_SCOUT_MIN_SCORE,
    PAPER_REPORT_MAX_OPEN_POSITIONS,
    PAPER_SCOUT_ENABLED,
    PAPER_SCOUT_LANES,
    PAPER_SCOUT_MAX_ENTRY_PRICE,
    PAPER_SCOUT_MAX_PER_RUN,
    PAPER_SCOUT_MIN_CONFIDENCE,
    PAPER_SCOUT_MIN_ATTENTION_GAP,
    PAPER_SCOUT_MIN_LANE_PRIOR,
    PAPER_SCOUT_MIN_WATCH_SCORE,
    PAPER_SCOUT_STAKE_USD,
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
        "daily_anchor_date": _utc_now().strftime("%Y-%m-%d"),
        "daily_anchor_equity_usd": PAPER_INITIAL_BANKROLL_USD,
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


def _scout_stake(equity):
    return min(PAPER_SCOUT_STAKE_USD, max(0.0, equity * 0.04))


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


def _preview_verdict_label(candidate):
    verdict = str(candidate.get("repricing_verdict") or "").strip()
    if not verdict:
        return "idea"
    return verdict.replace("_", " ")


def _preview_action_label(trade_type):
    return {
        "core": "Could buy now",
        "scout": "Likely scout",
        "monitor": "Watch only",
    }.get(trade_type, "Watch only")


def _format_preview_line(idx, row):
    return (
        f"{idx}. {row['question']} | {_preview_action_label(row.get('trade_type'))} | "
        f"{row['verdict']} | BUY {row['selected_outcome']} @ {row['entry_price']:.3f} | {row['lane_label']}"
    )


def _preview_priority(row):
    trade_type = str(row.get("trade_type") or "")
    verdict = str(row.get("verdict") or "")
    return (
        -(1 if trade_type == "core" else 0),
        -(1 if trade_type == "scout" else 0),
        -(1 if verdict == "watch high upside" else 0),
        -(row.get("watch_score") or 0.0),
        -(row.get("repricing_score") or 0.0),
        -(row.get("lane_prior") or 0.0),
        -(row.get("confidence") or 0.0),
    )


def _build_idea_preview(
    buy_candidates,
    eligible_scout_candidates,
    best_watchlist,
    radar_candidates,
    *,
    excluded_links=None,
):
    rows = []
    seen = set()
    blocked = {link for link in (excluded_links or set()) if link}

    for trade_type, bucket, candidates in (
        ("core", "buy_now", buy_candidates or []),
        ("scout", "watchlist", eligible_scout_candidates or []),
        ("monitor", "watchlist", best_watchlist or []),
        ("monitor", "radar", radar_candidates or []),
    ):
        for candidate in candidates:
            link = candidate.get("link")
            dedupe_key = link or candidate.get("market_key") or candidate.get("question")
            if not dedupe_key or dedupe_key in seen or link in blocked:
                continue
            rows.append(
                {
                    "bucket": bucket,
                    "trade_type": trade_type,
                    "question": candidate.get("question") or "Unknown market",
                    "selected_outcome": candidate.get("selected_outcome") or "outcome",
                    "entry_price": safe_float(candidate.get("entry"), default=0.0),
                    "verdict": _preview_verdict_label(candidate),
                    "lane_label": candidate.get("repricing_lane_label") or candidate.get("repricing_lane_key") or "repricing lane",
                    "link": link,
                    "watch_score": safe_float(candidate.get("repricing_watch_score"), default=0.0),
                    "repricing_score": safe_float(candidate.get("repricing_score"), default=0.0),
                    "lane_prior": safe_float(candidate.get("repricing_lane_prior"), default=0.0),
                    "confidence": safe_float(candidate.get("confidence"), default=0.0),
                }
            )
            seen.add(dedupe_key)

    rows.sort(key=_preview_priority)
    return rows[:PAPER_REPORT_MAX_IDEAS]


def _open_position(state, candidate, now_dt, ledger_rows, *, trade_mode="core"):
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

    if trade_mode == "scout":
        desired_stake = _scout_stake(equity)
    else:
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
        "trade_mode": trade_mode,
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
            "trade_mode": trade_mode,
            "link": position.get("link"),
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
            "trade_mode": position.get("trade_mode"),
            "link": position.get("link"),
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
        "trade_mode": position.get("trade_mode"),
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
                "trade_mode": position.get("trade_mode"),
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
        f"Daily guard: {'STOPPED' if summary.get('daily_stop_hit') else 'ACTIVE'} | Drawdown today ${summary.get('daily_drawdown_usd', 0.0):.2f} / ${PAPER_DAILY_STOP_LOSS_USD:.2f}",
        f"Signal pool: {summary.get('buy_now_count', 0)} buy now | {summary.get('watchlist_count', 0)} watchlist | {summary.get('radar_count', 0)} radar",
        "",
        "Opened this run",
    ]

    if summary["opened"]:
        for idx, row in enumerate(summary["opened"], start=1):
            lines.append(
                f"{idx}. {row['question']} | BUY {row['selected_outcome']} @ {row['entry_price']:.3f} | Stake ${row['stake_usd']:.2f} | {row['trade_mode']} | {row['lane_label']}"
            )
            if row.get("link"):
                lines.append(f"   Link: {row['link']}")
    else:
        lines.append("none")

    lines.extend(["", "Closed this run"])
    if summary["closed"]:
        for idx, row in enumerate(summary["closed"], start=1):
            pnl = row["pnl_usd"]
            sign = "+" if pnl >= 0 else "-"
            lines.append(
                f"{idx}. {row['question']} | EXIT @ {row['exit_price']:.3f} | PnL {sign}${abs(pnl):.2f} | {row['reason']} | {row.get('trade_mode') or 'core'}"
            )
            if row.get("link"):
                lines.append(f"   Link: {row['link']}")
    else:
        lines.append("none")

    lines.extend(["", "Open positions"])
    open_rows = summary["open_positions"][:PAPER_REPORT_MAX_OPEN_POSITIONS]
    if open_rows:
        for idx, row in enumerate(open_rows, start=1):
            sign = "+" if row["unrealized_pnl_usd"] >= 0 else "-"
            lines.append(
                f"{idx}. {row['question']} | BUY {row['selected_outcome']} @ {row['entry_price']:.3f} | Mark {row['mark_price']:.3f} | PnL {sign}${abs(row['unrealized_pnl_usd']):.2f} | {row.get('trade_mode') or 'core'}"
            )
            if row.get("link"):
                lines.append(f"   Link: {row['link']}")
    else:
        lines.append("none")

    lines.extend(["", "Next trade"])
    preview_rows = summary.get("idea_preview") or []
    if preview_rows:
        top_row = preview_rows[0]
        lines.append(_format_preview_line(1, top_row))
        if top_row.get("link"):
            lines.append(f"   Link: {top_row['link']}")

        backup_rows = preview_rows[1:3]
        lines.extend(["", "Backups"])
        if backup_rows:
            for idx, row in enumerate(backup_rows, start=1):
                lines.append(_format_preview_line(idx, row))
                if row.get("link"):
                    lines.append(f"   Link: {row['link']}")
        else:
            lines.append("none")
    else:
        lines.append("none")

    return "\n".join(lines)


def _refresh_daily_anchor(state, snapshot, now_dt):
    today = now_dt.strftime("%Y-%m-%d")
    if state.get("daily_anchor_date") != today:
        state["daily_anchor_date"] = today
        state["daily_anchor_equity_usd"] = snapshot["equity_usd"]


def _daily_stop_hit(state, snapshot):
    anchor = safe_float(state.get("daily_anchor_equity_usd"), default=snapshot["equity_usd"])
    drawdown = anchor - snapshot["equity_usd"]
    return drawdown >= PAPER_DAILY_STOP_LOSS_USD, drawdown


def _eligible_scout_candidates(candidates, *, limit=True):
    if not PAPER_SCOUT_ENABLED:
        return []

    rows = []
    seen = set()
    for candidate in candidates or []:
        link = candidate.get("link")
        if link and link in seen:
            continue
        verdict = str(candidate.get("repricing_verdict") or "")
        lane_key = str(candidate.get("repricing_lane_key") or "")
        entry = safe_float(candidate.get("entry"), default=1.0)
        watch_score = safe_float(candidate.get("repricing_watch_score"), default=0.0)
        score = safe_float(candidate.get("repricing_score"), default=0.0)
        attention_gap = safe_float(candidate.get("repricing_attention_gap"), default=0.0)
        confidence = safe_float(candidate.get("confidence"), default=0.0)
        lane_prior = safe_float(candidate.get("repricing_lane_prior"), default=0.0)
        is_watch_lane = lane_key in PAPER_SCOUT_LANES
        is_radar_buy = (
            PAPER_RADAR_SCOUT_ENABLED
            and verdict == "buy_now"
            and score >= PAPER_RADAR_SCOUT_MIN_SCORE
            and confidence >= PAPER_RADAR_SCOUT_MIN_CONFIDENCE
            and attention_gap >= PAPER_RADAR_SCOUT_MIN_ATTENTION_GAP
            and entry <= PAPER_RADAR_SCOUT_MAX_ENTRY_PRICE
        )
        is_lane_high_upside = (
            is_watch_lane
            and verdict == "watch_high_upside"
            and entry <= PAPER_SCOUT_MAX_ENTRY_PRICE
        )
        is_lane_strong_watch = (
            is_watch_lane
            and verdict == "watch"
            and watch_score >= PAPER_SCOUT_MIN_WATCH_SCORE
            and attention_gap >= PAPER_SCOUT_MIN_ATTENTION_GAP
            and entry <= PAPER_SCOUT_MAX_ENTRY_PRICE
        )
        is_global_high_upside = (
            verdict == "watch_high_upside"
            and watch_score >= PAPER_SCOUT_MIN_WATCH_SCORE
            and attention_gap >= PAPER_SCOUT_MIN_ATTENTION_GAP
            and confidence >= PAPER_SCOUT_MIN_CONFIDENCE
            and lane_prior >= PAPER_SCOUT_MIN_LANE_PRIOR
            and entry <= PAPER_SCOUT_MAX_ENTRY_PRICE
        )
        if not (is_lane_high_upside or is_lane_strong_watch or is_global_high_upside or is_radar_buy):
            continue
        rows.append(candidate)
        if link:
            seen.add(link)

    rows.sort(
        key=lambda row: (
            -(1 if str(row.get("repricing_verdict") or "") == "buy_now" else 0),
            -(row.get("repricing_watch_score") or 0.0),
            -(row.get("repricing_score") or 0.0),
            -(row.get("repricing_lane_prior") or 0.0),
            -(row.get("repricing_optionality_score") or 0.0),
            -(row.get("repricing_attention_gap") or 0.0),
            -(row.get("confidence") or 0.0),
        )
    )
    if limit:
        return rows[:PAPER_SCOUT_MAX_PER_RUN]
    return rows


def run_paper_cycle(
    markets,
    buy_candidates,
    *,
    best_watchlist=None,
    scout_candidates=None,
    radar_candidates=None,
    state_dir=None,
    generated_at_utc=None,
):
    now_dt = _utc_now()
    state = load_state(state_dir=state_dir)
    state["run_count"] = safe_int(state.get("run_count"), default=0) + 1
    state["last_run_at_utc"] = _iso_utc(now_dt)

    market_index = {_market_key_from_market(market): market for market in markets}
    ledger_rows = []
    closed = _update_positions(state, market_index, now_dt, ledger_rows)
    post_close_snapshot = _state_snapshot(state)
    _refresh_daily_anchor(state, post_close_snapshot, now_dt)
    daily_stop_hit, daily_drawdown = _daily_stop_hit(state, post_close_snapshot)

    opened_positions = []
    if not daily_stop_hit:
        scout_pool = scout_candidates if scout_candidates is not None else best_watchlist
        eligible_scout_rows = _eligible_scout_candidates(scout_pool)
        for trade_mode, rows in (("core", buy_candidates), ("scout", eligible_scout_rows)):
            for candidate in rows:
                position = _open_position(state, candidate, now_dt, ledger_rows, trade_mode=trade_mode)
                if position:
                    opened_positions.append(
                        {
                            "question": position.get("question"),
                            "selected_outcome": position.get("selected_outcome"),
                            "entry_price": safe_float(position.get("entry_price"), default=0.0),
                            "stake_usd": safe_float(position.get("stake_usd"), default=0.0),
                            "lane_label": position.get("lane_label") or position.get("lane_key"),
                            "trade_mode": trade_mode,
                            "link": position.get("link"),
                        }
                    )

    snapshot = _state_snapshot(state)
    open_links = {row.get("link") for row in snapshot["open_positions"] if row.get("link")}
    preview_scout_rows = _eligible_scout_candidates(scout_pool, limit=False) if not daily_stop_hit else []
    idea_preview = _build_idea_preview(
        buy_candidates,
        preview_scout_rows,
        best_watchlist,
        radar_candidates,
        excluded_links=open_links,
    )
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
        "daily_stop_hit": daily_stop_hit,
        "daily_drawdown_usd": daily_drawdown,
        "daily_anchor_equity_usd": safe_float(state.get("daily_anchor_equity_usd"), default=snapshot["equity_usd"]),
        "buy_now_count": len(buy_candidates or []),
        "watchlist_count": len(best_watchlist or []),
        "radar_count": len(radar_candidates or []),
        "idea_preview": idea_preview,
    }

    save_state(state, summary=summary, state_dir=state_dir)
    for row in ledger_rows:
        _append_jsonl(_paper_paths(state_dir)["ledger"], row)

    return {
        "state": state,
        "summary": summary,
        "report_text": _format_report(summary),
    }
