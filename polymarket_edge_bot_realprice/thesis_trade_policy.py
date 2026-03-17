from utils import safe_int


def thesis_identity(item):
    thesis_id = str(item.get("thesis_id") or "").strip()
    if thesis_id:
        return thesis_id

    market_key = str(item.get("market_key") or "").strip()
    if market_key:
        return f"market:{market_key}"

    event_slug = str(item.get("event_slug") or "").strip()
    if event_slug:
        return f"event:{event_slug}"

    link = str(item.get("link") or "").strip()
    if link:
        return f"link:{link}"

    question = str(item.get("question") or "").strip()
    if question:
        return f"question:{question}"

    return "unknown"


def can_open_thesis_trade(state, candidate, *, now_ts, thesis_cooldown_minutes):
    thesis_id = thesis_identity(candidate)
    cooldown_seconds = max(0, int(thesis_cooldown_minutes * 60))

    for position in state.get("positions") or []:
        if thesis_identity(position) == thesis_id:
            return {
                "allowed": False,
                "blocked_reason": "thesis_position_open",
                "thesis_id": thesis_id,
            }

    closed_map = state.get("recently_closed_theses") or {}
    recent_close_ts = safe_int(closed_map.get(thesis_id))
    if recent_close_ts and now_ts - recent_close_ts < cooldown_seconds:
        return {
            "allowed": False,
            "blocked_reason": "thesis_cooldown",
            "thesis_id": thesis_id,
        }

    return {
        "allowed": True,
        "blocked_reason": None,
        "thesis_id": thesis_id,
    }


def register_closed_thesis(state, position, *, closed_ts):
    thesis_id = thesis_identity(position)
    state.setdefault("recently_closed_theses", {})[thesis_id] = int(closed_ts)
    return thesis_id
