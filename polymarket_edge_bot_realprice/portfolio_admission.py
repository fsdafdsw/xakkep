import re

from utils import safe_float


_SUBJECT_RE = re.compile(
    r"^(?:will\s+)?(?P<subject>.+?)\s+"
    r"(?:strike|invade|meet|talk|call|release|rule|sign|leave|resume)\b",
    re.IGNORECASE,
)


def _normalize_key(value):
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def portfolio_theme_key(item):
    action_family = str(item.get("domain_action_family") or item.get("action_family") or "unknown").strip().lower()
    primary_entity = _normalize_key(item.get("primary_entity_key"))
    if primary_entity:
        return f"{action_family}:{primary_entity}"

    question = str(item.get("question") or "").strip()
    match = _SUBJECT_RE.match(question)
    if match:
        subject = _normalize_key(match.group("subject"))
        if subject:
            return f"{action_family}:{subject}"

    event_slug = _normalize_key(item.get("event_slug"))
    if event_slug:
        return f"{action_family}:{event_slug}"

    thesis_id = _normalize_key(item.get("thesis_id"))
    if thesis_id:
        return f"{action_family}:{thesis_id}"

    market_key = _normalize_key(item.get("market_key"))
    return f"{action_family}:{market_key or 'unknown'}"


def _closed_trade_memory(state):
    return list(state.get("closed_trade_memory") or [])


def _recent_rows(rows, *, field, value, limit):
    matches = [row for row in reversed(rows) if str(row.get(field) or "") == str(value or "")]
    return matches[:limit]


def _loss_streak(rows):
    streak = 0
    for row in rows:
        pnl = safe_float(row.get("pnl_usd"), default=0.0)
        if pnl < 0:
            streak += 1
            continue
        break
    return streak


def can_open_portfolio_trade(
    state,
    candidate,
    *,
    max_open_per_theme,
    max_conflict_open_positions,
    lane_recent_trades,
    lane_kill_min_trades,
    lane_kill_max_mean_pnl_usd,
    lane_kill_loss_streak,
    theme_recent_trades,
    theme_kill_min_trades,
    theme_kill_max_mean_pnl_usd,
):
    lane_key = str(candidate.get("repricing_lane_key") or candidate.get("lane_key") or "").strip()
    theme_key = portfolio_theme_key(candidate)
    positions = list(state.get("positions") or [])

    theme_open_count = sum(1 for position in positions if portfolio_theme_key(position) == theme_key)
    if theme_open_count >= max_open_per_theme:
        return {
            "allowed": False,
            "blocked_reason": "theme_cap",
            "lane_key": lane_key,
            "theme_key": theme_key,
        }

    if lane_key == "conflict_fast":
        lane_open_count = sum(1 for position in positions if str(position.get("lane_key") or "") == lane_key)
        if lane_open_count >= max_conflict_open_positions:
            return {
                "allowed": False,
                "blocked_reason": "lane_cap_conflict",
                "lane_key": lane_key,
                "theme_key": theme_key,
            }

    closed_rows = _closed_trade_memory(state)
    lane_recent = _recent_rows(closed_rows, field="lane_key", value=lane_key, limit=lane_recent_trades)
    if len(lane_recent) >= lane_kill_min_trades:
        lane_mean_pnl = sum(safe_float(row.get("pnl_usd"), default=0.0) for row in lane_recent) / max(1, len(lane_recent))
        if lane_mean_pnl <= lane_kill_max_mean_pnl_usd:
            return {
                "allowed": False,
                "blocked_reason": "lane_expectancy_kill",
                "lane_key": lane_key,
                "theme_key": theme_key,
            }
        if _loss_streak(lane_recent) >= lane_kill_loss_streak:
            return {
                "allowed": False,
                "blocked_reason": "lane_loss_streak",
                "lane_key": lane_key,
                "theme_key": theme_key,
            }

    theme_recent = _recent_rows(closed_rows, field="theme_key", value=theme_key, limit=theme_recent_trades)
    if len(theme_recent) >= theme_kill_min_trades:
        theme_mean_pnl = sum(safe_float(row.get("pnl_usd"), default=0.0) for row in theme_recent) / max(1, len(theme_recent))
        if theme_mean_pnl <= theme_kill_max_mean_pnl_usd:
            return {
                "allowed": False,
                "blocked_reason": "theme_expectancy_kill",
                "lane_key": lane_key,
                "theme_key": theme_key,
            }

    return {
        "allowed": True,
        "blocked_reason": None,
        "lane_key": lane_key,
        "theme_key": theme_key,
    }


def register_closed_trade(state, position, *, pnl_usd, closed_ts, max_rows=100):
    row = {
        "closed_ts": int(closed_ts),
        "lane_key": position.get("lane_key"),
        "theme_key": portfolio_theme_key(position),
        "market_key": position.get("market_key"),
        "pnl_usd": safe_float(pnl_usd, default=0.0),
        "trade_mode": position.get("trade_mode"),
        "question": position.get("question"),
    }
    memory = list(state.get("closed_trade_memory") or [])
    memory.append(row)
    if len(memory) > max_rows:
        memory = memory[-max_rows:]
    state["closed_trade_memory"] = memory
    return row
