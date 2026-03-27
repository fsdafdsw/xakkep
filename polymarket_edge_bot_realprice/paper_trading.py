import json
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from config import (
    BANKROLL_USD,
    ESTIMATED_SLIPPAGE_BPS,
    PAPER_CORE_LANES,
    PAPER_DAILY_STOP_LOSS_USD,
    PAPER_LANE_ADMISSION_LOOKBACK,
    PAPER_LANE_KILL_LOSS_STREAK,
    PAPER_LANE_KILL_MAX_MEAN_PNL_USD,
    PAPER_LANE_KILL_MIN_TRADES,
    PAPER_INITIAL_BANKROLL_USD,
    PAPER_MAX_BET_USD,
    PAPER_MAX_CONFLICT_OPEN_POSITIONS,
    PAPER_MAX_OPEN_POSITIONS,
    PAPER_MAX_OPEN_PER_THEME,
    PAPER_MIN_TRADE_USD,
    PAPER_REENTRY_COOLDOWN_MINUTES,
    PAPER_THESIS_REENTRY_COOLDOWN_MINUTES,
    PAPER_REPORT_MAX_OPEN_POSITIONS,
    PAPER_STATE_DIR,
    PAPER_STRATEGY_VERSION,
    PAPER_THEME_ADMISSION_LOOKBACK,
    PAPER_THEME_KILL_MAX_MEAN_PNL_USD,
    PAPER_THEME_KILL_MIN_TRADES,
    TAKER_FEE_BPS,
)
from exit_policy import live_exit_plan
from portfolio_admission import can_open_portfolio_trade, portfolio_theme_key, register_closed_trade
from thesis_trade_policy import can_open_thesis_trade, register_closed_thesis, thesis_identity
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
        "version": 2,
        "strategy_version": PAPER_STRATEGY_VERSION,
        "created_at_utc": _iso_utc(_utc_now()),
        "initial_bankroll_usd": PAPER_INITIAL_BANKROLL_USD,
        "cash_usd": PAPER_INITIAL_BANKROLL_USD,
        "realized_pnl_usd": 0.0,
        "positions": [],
        "recently_closed": {},
        "recently_closed_theses": {},
        "closed_trade_memory": [],
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


def _safe_archive_part(value):
    text = str(value or "legacy")
    cleaned = []
    for char in text:
        if char.isalnum() or char in ("-", "_"):
            cleaned.append(char)
        else:
            cleaned.append("_")
    collapsed = "".join(cleaned).strip("_")
    return collapsed or "legacy"


def _archive_stale_state(paths, state, *, archived_from=None):
    existing_files = [paths[name] for name in ("state", "ledger", "runs", "latest") if paths[name].exists()]
    if not existing_files:
        return None

    archived_from = str(archived_from or state.get("strategy_version") or "legacy")
    archive_dir = (
        paths["root"]
        / "archive"
        / f"{_utc_now().strftime('%Y%m%dT%H%M%SZ')}_{_safe_archive_part(archived_from)}"
    )
    archive_dir.mkdir(parents=True, exist_ok=True)
    for source in existing_files:
        shutil.move(str(source), str(archive_dir / source.name))

    return {
        "from_strategy_version": archived_from,
        "archive_dir": str(archive_dir),
    }


def load_state(state_dir=None):
    paths = _paper_paths(state_dir)
    path = paths["state"]
    if not path.exists():
        return _default_state(), None
    try:
        with path.open("r", encoding="utf-8") as fh:
            raw_state = json.load(fh)
    except Exception:
        return _default_state(), None

    default = _default_state()
    default.update(raw_state if isinstance(raw_state, dict) else {})
    default["positions"] = list(default.get("positions") or [])
    default["recently_closed"] = dict(default.get("recently_closed") or {})
    default["recently_closed_theses"] = dict(default.get("recently_closed_theses") or {})
    default["closed_trade_memory"] = list(default.get("closed_trade_memory") or [])
    raw_strategy = ""
    if isinstance(raw_state, dict):
        raw_strategy = str(raw_state.get("strategy_version") or "")
    if raw_strategy != PAPER_STRATEGY_VERSION:
        reset_info = _archive_stale_state(paths, default, archived_from=raw_strategy or "legacy") or {
            "from_strategy_version": raw_strategy or "legacy",
            "archive_dir": None,
        }
        return _default_state(), reset_info
    return default, None


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


def _passes_structure_execution_gate(candidate):
    return bool(candidate)


def _execution_policy_decision(candidate, *, trade_mode):
    lane_key = str(candidate.get("repricing_lane_key") or "")
    verdict = str(candidate.get("repricing_verdict") or "")
    entry = safe_float(candidate.get("entry"), default=1.0)
    watch_score = safe_float(candidate.get("repricing_watch_score"), default=0.0)
    attention_gap = safe_float(candidate.get("repricing_attention_gap"), default=0.0)
    confidence = safe_float(candidate.get("confidence"), default=0.0)
    fresh_catalyst = safe_float(candidate.get("repricing_fresh_catalyst_score"), default=0.0)

    if trade_mode != "core":
        return {"allowed": False, "blocked_reason": "scout_disabled"}

    if lane_key not in PAPER_CORE_LANES:
        return {"allowed": False, "blocked_reason": "lane_not_enabled"}

    if lane_key == "release_hearing":
        if verdict != "buy_now":
            return {"allowed": False, "blocked_reason": "not_buy_now"}
        if entry > 0.22:
            return {"allowed": False, "blocked_reason": "price_too_high"}
        if confidence < 0.68:
            return {"allowed": False, "blocked_reason": "confidence_too_low"}
        if fresh_catalyst < 0.52:
            return {"allowed": False, "blocked_reason": "catalyst_too_weak"}
        if watch_score < 0.68:
            return {"allowed": False, "blocked_reason": "watch_score_too_low"}
        return {"allowed": True, "blocked_reason": None}

    if lane_key == "diplomacy_talk_call":
        if verdict != "buy_now":
            return {"allowed": False, "blocked_reason": "not_buy_now"}
        if entry > 0.14:
            return {"allowed": False, "blocked_reason": "price_too_high"}
        if confidence < 0.70:
            return {"allowed": False, "blocked_reason": "confidence_too_low"}
        if attention_gap < 0.32:
            return {"allowed": False, "blocked_reason": "attention_too_low"}
        if watch_score < 0.70:
            return {"allowed": False, "blocked_reason": "watch_score_too_low"}
        return {"allowed": True, "blocked_reason": None}

    if lane_key == "diplomacy_meeting":
        if verdict != "watch_high_upside":
            return {"allowed": False, "blocked_reason": "not_high_upside"}
        if entry > 0.08:
            return {"allowed": False, "blocked_reason": "price_too_high"}
        if confidence < 0.76:
            return {"allowed": False, "blocked_reason": "confidence_too_low"}
        if fresh_catalyst < 0.60:
            return {"allowed": False, "blocked_reason": "catalyst_too_weak"}
        if attention_gap < 0.70:
            return {"allowed": False, "blocked_reason": "attention_too_low"}
        if watch_score < 0.92:
            return {"allowed": False, "blocked_reason": "watch_score_too_low"}
        return {"allowed": True, "blocked_reason": None}

    return {"allowed": False, "blocked_reason": "lane_not_enabled"}


def _filter_consistency_execution_candidates(candidates):
    rows = []
    for candidate in candidates or []:
        if _passes_structure_execution_gate(candidate):
            rows.append(candidate)
    return rows


def _merge_candidate_lists(*candidate_groups):
    rows = []
    seen = set()
    for group in candidate_groups:
        for candidate in group or []:
            key = candidate.get("link") or candidate.get("market_key") or candidate.get("question")
            if not key or key in seen:
                continue
            rows.append(candidate)
            seen.add(key)
    return rows


def _portfolio_equity(state):
    equity = safe_float(state.get("cash_usd"), default=0.0)
    for position in state.get("positions") or []:
        mark = safe_float(position.get("last_mark_price"), default=position.get("entry_price"))
        equity += _sale_proceeds(position.get("shares", 0.0), mark)
    return equity


def _candidate_open_plan(state, candidate, now_dt, *, trade_mode="core", excluded_links=None):
    if not _passes_structure_execution_gate(candidate):
        return {"allowed": False, "blocked_reason": "structure_gate"}

    policy = _execution_policy_decision(candidate, trade_mode=trade_mode)
    if not policy["allowed"]:
        return {"allowed": False, "blocked_reason": policy["blocked_reason"]}

    if len(state["positions"]) >= PAPER_MAX_OPEN_POSITIONS:
        return {"allowed": False, "blocked_reason": "max_open_positions"}

    market_key = _market_key_from_candidate(candidate)
    link = candidate.get("link")
    if excluded_links and link and link in excluded_links:
        return {"allowed": False, "blocked_reason": "already_open_link"}

    recent_close_ts = safe_int(state.get("recently_closed", {}).get(market_key))
    now_ts = int(now_dt.timestamp())
    cooldown_seconds = int(PAPER_REENTRY_COOLDOWN_MINUTES * 60)
    if recent_close_ts and now_ts - recent_close_ts < cooldown_seconds:
        return {"allowed": False, "blocked_reason": "market_cooldown"}

    if any(position.get("market_key") == market_key for position in state["positions"]):
        return {"allowed": False, "blocked_reason": "market_already_open"}

    admission_gate = can_open_portfolio_trade(
        state,
        candidate,
        max_open_per_theme=PAPER_MAX_OPEN_PER_THEME,
        max_conflict_open_positions=PAPER_MAX_CONFLICT_OPEN_POSITIONS,
        lane_recent_trades=PAPER_LANE_ADMISSION_LOOKBACK,
        lane_kill_min_trades=PAPER_LANE_KILL_MIN_TRADES,
        lane_kill_max_mean_pnl_usd=PAPER_LANE_KILL_MAX_MEAN_PNL_USD,
        lane_kill_loss_streak=PAPER_LANE_KILL_LOSS_STREAK,
        theme_recent_trades=PAPER_THEME_ADMISSION_LOOKBACK,
        theme_kill_min_trades=PAPER_THEME_KILL_MIN_TRADES,
        theme_kill_max_mean_pnl_usd=PAPER_THEME_KILL_MAX_MEAN_PNL_USD,
    )
    if not admission_gate["allowed"]:
        return {
            "allowed": False,
            "blocked_reason": admission_gate["blocked_reason"],
            "theme_key": admission_gate["theme_key"],
        }

    thesis_gate = can_open_thesis_trade(
        state,
        candidate,
        now_ts=now_ts,
        thesis_cooldown_minutes=PAPER_THESIS_REENTRY_COOLDOWN_MINUTES,
    )
    if not thesis_gate["allowed"]:
        return {
            "allowed": False,
            "blocked_reason": thesis_gate["blocked_reason"],
            "theme_key": admission_gate["theme_key"],
            "thesis_id": thesis_gate["thesis_id"],
        }

    entry_price = safe_float(candidate.get("entry"))
    if entry_price is None or entry_price <= 0:
        return {"allowed": False, "blocked_reason": "bad_entry_price"}

    equity = _portfolio_equity(state)
    desired_stake = _scaled_stake(safe_float(candidate.get("stake_usd")), equity)
    if desired_stake < PAPER_MIN_TRADE_USD:
        desired_stake = PAPER_MIN_TRADE_USD

    shares = desired_stake / entry_price
    additional_cost_per_share = safe_float(candidate.get("cost_per_share"), default=0.0)
    total_outlay = desired_stake + (shares * additional_cost_per_share)
    cash = safe_float(state.get("cash_usd"), default=0.0)
    if total_outlay > cash or total_outlay <= 0:
        return {"allowed": False, "blocked_reason": "insufficient_cash"}

    return {
        "allowed": True,
        "blocked_reason": None,
        "market_key": market_key,
        "now_ts": now_ts,
        "theme_key": admission_gate["theme_key"],
        "thesis_id": thesis_gate["thesis_id"],
        "entry_price": entry_price,
        "desired_stake": desired_stake,
        "shares": shares,
        "total_outlay": total_outlay,
        "cash": cash,
    }


def _filter_executable_candidates(state, candidates, now_dt, *, trade_mode="core", excluded_links=None):
    rows = []
    blocked_reasons = Counter()
    for candidate in candidates or []:
        plan = _candidate_open_plan(
            state,
            candidate,
            now_dt,
            trade_mode=trade_mode,
            excluded_links=excluded_links,
        )
        if not plan["allowed"]:
            blocked_reasons[plan.get("blocked_reason") or "unknown"] += 1
            continue
        rows.append(candidate)
    return rows, blocked_reasons


def _build_idea_preview(
    executable_buy_candidates,
):
    rows = []
    seen = set()
    for candidate in executable_buy_candidates or []:
        link = candidate.get("link")
        dedupe_key = link or candidate.get("market_key") or candidate.get("question")
        if not dedupe_key or dedupe_key in seen:
            continue
        rows.append(
            {
                "bucket": "buy_now",
                "trade_type": "core",
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
    return rows[:1]


def _open_position(state, candidate, now_dt, ledger_rows, *, trade_mode="core"):
    open_plan = _candidate_open_plan(state, candidate, now_dt, trade_mode=trade_mode)
    if not open_plan["allowed"]:
        return None
    market_key = open_plan["market_key"]
    now_ts = open_plan["now_ts"]
    entry_price = open_plan["entry_price"]
    desired_stake = open_plan["desired_stake"]
    shares = open_plan["shares"]
    total_outlay = open_plan["total_outlay"]
    cash = open_plan["cash"]

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
        "thesis_id": open_plan["thesis_id"],
        "thesis_type": candidate.get("thesis_type"),
        "thesis_cluster_size": candidate.get("thesis_cluster_size"),
        "primary_entity_key": candidate.get("primary_entity_key"),
        "theme_key": open_plan["theme_key"],
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
            "thesis_id": position.get("thesis_id"),
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
    thesis_id = register_closed_thesis(state, position, closed_ts=int(now_dt.timestamp()))
    closed_trade = register_closed_trade(state, position, pnl_usd=pnl, closed_ts=int(now_dt.timestamp()))
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
            "thesis_id": thesis_id,
            "theme_key": closed_trade.get("theme_key"),
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
        "thesis_id": thesis_id,
        "theme_key": closed_trade.get("theme_key"),
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
                "thesis_id": position.get("thesis_id") or thesis_identity(position),
                "theme_key": position.get("theme_key") or portfolio_theme_key(position),
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


def _blocked_reason_label(reason):
    labels = {
        "daily_stop": "Daily stop is active, so new entries are paused.",
        "no_buy_candidates": "No core candidates reached paper execution.",
        "lane_not_enabled": "No candidate landed in the active core lanes.",
        "not_buy_now": "Candidates existed, but none were strong enough for buy_now.",
        "not_high_upside": "Candidates existed, but none reached the high-upside meeting threshold.",
        "price_too_high": "Best candidate is too expensive at the current price.",
        "confidence_too_low": "Confidence is below the core threshold.",
        "catalyst_too_weak": "Fresh catalyst signal is too weak for entry.",
        "watch_score_too_low": "Repricing watch score is too weak for entry.",
        "attention_too_low": "Attention gap is too weak for entry.",
        "theme_cap": "Theme cap blocked a repeat trade in the same story.",
        "lane_cap_conflict": "Conflict lane cap blocked a new position.",
        "lane_expectancy_kill": "Recent lane expectancy is negative, so the lane is paused.",
        "lane_loss_streak": "Recent lane loss streak paused new entries.",
        "theme_expectancy_kill": "Recent theme expectancy is negative, so the theme is paused.",
        "thesis_position_open": "A position in the same thesis is already open.",
        "thesis_cooldown": "The thesis is still in cooldown after a recent close.",
        "market_cooldown": "This market is still in cooldown after a recent close.",
        "market_already_open": "This market is already open in the portfolio.",
        "max_open_positions": "Portfolio already reached the max open positions.",
        "insufficient_cash": "Cash is too low for a new position.",
        "bad_entry_price": "Entry price is invalid, so the trade was skipped.",
        "structure_gate": "Structural gate blocked the candidate.",
        "scout_disabled": "Scout mode is disabled in the current paper setup.",
    }
    return labels.get(reason or "", "No executable trade passed the current rules.")


def _summarize_no_trade_reason(*, daily_stop_hit, buy_candidates, blocked_reasons, opened_positions):
    if daily_stop_hit:
        return _blocked_reason_label("daily_stop")
    if opened_positions:
        return "Opened a core trade this run."
    if not (buy_candidates or []):
        return _blocked_reason_label("no_buy_candidates")
    if blocked_reasons:
        reason = sorted(blocked_reasons.items(), key=lambda item: (-item[1], item[0]))[0][0]
        return _blocked_reason_label(reason)
    return _blocked_reason_label(None)


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
        f"Health: {'STOPPED' if summary.get('daily_stop_hit') else 'ACTIVE'} | Runs {summary.get('run_count', 0)} | Open positions {summary['open_position_count']}",
        f"Strategy: {summary.get('strategy_version') or PAPER_STRATEGY_VERSION}",
        f"Bank: ${start_bank:.2f} -> ${equity:.2f} ({sign}{change_pct:.2f}%)",
        f"Cash: ${summary['cash_usd']:.2f} | Open positions: {summary['open_position_count']} | Realized: {realized_sign}${abs(realized):.2f} | Unrealized: {unrealized_sign}${abs(unrealized):.2f}",
        f"Daily guard: {'STOPPED' if summary.get('daily_stop_hit') else 'ACTIVE'} | Drawdown today ${summary.get('daily_drawdown_usd', 0.0):.2f} / ${PAPER_DAILY_STOP_LOSS_USD:.2f}",
        f"Executable core pool: {summary.get('buy_now_count', 0)}",
    ]

    if summary.get("strategy_reset"):
        reset_from = summary.get("strategy_reset_from") or "legacy"
        lines.append(f"State reset: yes | previous strategy {reset_from}")

    lines.extend(["", "Opened this run"])

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

    lines.extend(["", "Next executable trade"])
    preview_rows = summary.get("idea_preview") or []
    if preview_rows:
        top_row = preview_rows[0]
        lines.append(_format_preview_line(1, top_row))
        if top_row.get("link"):
            lines.append(f"   Link: {top_row['link']}")
    else:
        lines.append("none")

    lines.extend(["", "Why no trade"])
    lines.append(summary.get("no_trade_reason") or "none")

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
    return []


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
    state, reset_info = load_state(state_dir=state_dir)
    state["run_count"] = safe_int(state.get("run_count"), default=0) + 1
    state["last_run_at_utc"] = _iso_utc(now_dt)

    market_index = {_market_key_from_market(market): market for market in markets}
    ledger_rows = []
    closed = _update_positions(state, market_index, now_dt, ledger_rows)
    post_close_snapshot = _state_snapshot(state)
    _refresh_daily_anchor(state, post_close_snapshot, now_dt)
    daily_stop_hit, daily_drawdown = _daily_stop_hit(state, post_close_snapshot)
    open_links = {row.get("link") for row in post_close_snapshot["open_positions"] if row.get("link")}

    paper_buy_candidates = _merge_candidate_lists(
        _filter_consistency_execution_candidates(buy_candidates),
        _filter_consistency_execution_candidates(best_watchlist),
    )

    opened_positions = []
    executable_buy_candidates = []
    blocked_buy_reasons = Counter()
    if not daily_stop_hit:
        executable_buy_candidates, blocked_buy_reasons = _filter_executable_candidates(
            state,
            paper_buy_candidates,
            now_dt,
            trade_mode="core",
            excluded_links=open_links,
        )
        for candidate in executable_buy_candidates[:1]:
            position = _open_position(state, candidate, now_dt, ledger_rows, trade_mode="core")
            if position:
                opened_positions.append(
                    {
                        "question": position.get("question"),
                        "selected_outcome": position.get("selected_outcome"),
                        "entry_price": safe_float(position.get("entry_price"), default=0.0),
                        "stake_usd": safe_float(position.get("stake_usd"), default=0.0),
                        "lane_label": position.get("lane_label") or position.get("lane_key"),
                        "trade_mode": "core",
                        "link": position.get("link"),
                    }
                )

    snapshot = _state_snapshot(state)
    remaining_buy_candidates = executable_buy_candidates[1:] if opened_positions else executable_buy_candidates
    idea_preview = _build_idea_preview(remaining_buy_candidates if not daily_stop_hit else [])
    no_trade_reason = _summarize_no_trade_reason(
        daily_stop_hit=daily_stop_hit,
        buy_candidates=paper_buy_candidates,
        blocked_reasons=blocked_buy_reasons,
        opened_positions=opened_positions,
    )
    summary = {
        "generated_at_utc": generated_at_utc or _iso_utc(now_dt),
        "strategy_version": state.get("strategy_version") or PAPER_STRATEGY_VERSION,
        "strategy_reset": bool(reset_info),
        "strategy_reset_from": (reset_info or {}).get("from_strategy_version"),
        "strategy_reset_archive_dir": (reset_info or {}).get("archive_dir"),
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
        "buy_now_count": len(executable_buy_candidates),
        "watchlist_count": 0,
        "radar_count": 0,
        "idea_preview": idea_preview,
        "no_trade_reason": no_trade_reason,
        "blocked_reason_counts": dict(blocked_buy_reasons),
    }

    save_state(state, summary=summary, state_dir=state_dir)
    for row in ledger_rows:
        _append_jsonl(_paper_paths(state_dir)["ledger"], row)

    return {
        "state": state,
        "summary": summary,
        "report_text": _format_report(summary),
    }
