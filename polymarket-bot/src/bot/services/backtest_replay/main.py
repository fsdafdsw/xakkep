import json
import logging
from datetime import UTC, datetime
from typing import Any

from bot.config import get_settings
from bot.constants import KEY_REPLAY_CURSOR, KEY_REPLAY_ROWS, KEY_REPLAY_STATE, TOPIC_MARKET_SNAPSHOT, book_key
from bot.db import get_db
from bot.event_bus import publish_event
from bot.metrics import EVENT_OUT_TOTAL
from bot.redis_client import get_redis
from bot.schemas import MarketSnapshot
from bot.services.common import run_loop


def _parse_dt(raw: str) -> datetime | None:
    value = raw.strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _load_cursor(redis: Any) -> tuple[datetime, str, str] | None:
    raw = redis.get(KEY_REPLAY_CURSOR)
    if not raw:
        return None
    payload = json.loads(raw.decode("utf-8"))
    ts = _parse_dt(str(payload.get("ts", "")))
    market_id = str(payload.get("market_id", ""))
    outcome_id = str(payload.get("outcome_id", ""))
    if ts is None:
        return None
    return ts, market_id, outcome_id


def _store_cursor(redis: Any, ts: datetime, market_id: str, outcome_id: str) -> None:
    payload = {"ts": ts.isoformat(), "market_id": market_id, "outcome_id": outcome_id}
    redis.set(KEY_REPLAY_CURSOR, json.dumps(payload))


def _snapshot_from_row(row: dict[str, Any]) -> MarketSnapshot:
    best_bid = float(row["best_bid"])
    best_ask = float(row["best_ask"])
    mid = (best_bid + best_ask) / 2.0 if best_ask >= best_bid else 0.5
    return MarketSnapshot(
        timestamp=row["ts"],
        market_id=str(row["market_id"]),
        outcome_id=str(row["outcome_id"]),
        best_bid=best_bid,
        best_ask=best_ask,
        depth_bid=float(row["depth_bid"]),
        depth_ask=float(row["depth_ask"]),
        last_price=mid,
        volume_1h=0.0,
    )


def _book_payload(snapshot: MarketSnapshot) -> dict[str, Any]:
    return {
        "timestamp": snapshot.timestamp.isoformat(),
        "market_id": snapshot.market_id,
        "outcome_id": snapshot.outcome_id,
        "best_bid": snapshot.best_bid,
        "best_ask": snapshot.best_ask,
        "depth_bid": snapshot.depth_bid,
        "depth_ask": snapshot.depth_ask,
        "last_price": snapshot.last_price,
        "volume_1h": snapshot.volume_1h,
    }


def _reset_state_if_needed(redis: Any, db: Any, should_reset: bool) -> None:
    if not should_reset:
        return
    state = redis.get(KEY_REPLAY_STATE)
    if state and state.decode("utf-8") == "reset_done":
        return
    db.reset_runtime_state()
    redis.delete(KEY_REPLAY_CURSOR)
    redis.set(KEY_REPLAY_ROWS, "0")
    for key in redis.scan_iter(match="offset:*"):
        redis.delete(key)
    redis.set(KEY_REPLAY_STATE, "reset_done")


def tick() -> None:
    settings = get_settings()
    logger = logging.getLogger("backtest_replay")
    if not settings.backtest_replay_enabled:
        logger.debug("backtest replay disabled")
        return

    redis = get_redis()
    db = get_db()
    _reset_state_if_needed(redis=redis, db=db, should_reset=settings.backtest_replay_reset_state)

    start_at = _parse_dt(settings.backtest_replay_start)
    end_at = _parse_dt(settings.backtest_replay_end)
    cursor = _load_cursor(redis)
    cursor_ts = cursor[0] if cursor else None
    cursor_market_id = cursor[1] if cursor else ""
    cursor_outcome_id = cursor[2] if cursor else ""

    speed = max(1, int(round(settings.backtest_replay_speed)))
    limit = max(1, settings.backtest_replay_batch_size * speed)
    rows = db.replay_orderbooks(
        limit=limit,
        start_at=start_at,
        end_at=end_at,
        cursor_ts=cursor_ts,
        cursor_market_id=cursor_market_id,
        cursor_outcome_id=cursor_outcome_id,
    )

    if not rows:
        if settings.backtest_replay_loop:
            redis.delete(KEY_REPLAY_CURSOR)
            redis.set(KEY_REPLAY_STATE, "looped")
            logger.info("replay loop restart")
        else:
            redis.set(KEY_REPLAY_STATE, "done")
            logger.info("replay finished")
        return

    emitted = 0
    for row in rows:
        snapshot = _snapshot_from_row(row)
        publish_event(redis, TOPIC_MARKET_SNAPSHOT, snapshot)
        EVENT_OUT_TOTAL.labels(service="backtest_replay", topic=TOPIC_MARKET_SNAPSHOT).inc()
        redis.set(book_key(snapshot.market_id, snapshot.outcome_id), json.dumps(_book_payload(snapshot)), ex=300)
        _store_cursor(redis, snapshot.timestamp, snapshot.market_id, snapshot.outcome_id)
        emitted += 1

    redis.incrby(KEY_REPLAY_ROWS, emitted)
    redis.set(KEY_REPLAY_STATE, "running")
    logger.info(
        "replay tick",
        extra={"rows": emitted, "cursor_ts": rows[-1]["ts"].isoformat(), "speed": speed},
    )


if __name__ == "__main__":
    run_loop("backtest_replay", tick)
