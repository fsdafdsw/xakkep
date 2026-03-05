import logging
import json
from datetime import UTC, datetime
from typing import Any

from bot.config import get_settings
from bot.constants import TOPIC_EDGE_SIGNAL, TOPIC_FORECAST, book_key, stream_offset_key
from bot.db import get_db
from bot.event_bus import consume_events, publish_event
from bot.metrics import EVENT_IN_TOTAL, EVENT_OUT_TOTAL
from bot.redis_client import get_redis
from bot.schemas import EdgeSignal, Side, SignalAction
from bot.services.common import run_loop


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def tick() -> None:
    settings = get_settings()
    service = "edge_engine"
    logger = logging.getLogger("edge_engine")
    redis = get_redis()
    db = get_db()
    events = consume_events(
        redis=redis,
        topic=TOPIC_FORECAST,
        offset_key=stream_offset_key(service, TOPIC_FORECAST),
        count=200,
    )
    if not events:
        return

    EVENT_IN_TOTAL.labels(service=service, topic=TOPIC_FORECAST).inc(len(events))

    emitted = 0
    for event in events:
        market_id = str(event["market_id"])
        outcome_id = str(event["outcome_id"])
        p_hat = _safe_float(event.get("p_hat"), 0.5)
        p_low = _safe_float(event.get("p_low"), p_hat)
        p_high = _safe_float(event.get("p_high"), p_hat)

        raw_book = redis.get(book_key(market_id, outcome_id))
        if not raw_book:
            continue
        book = json.loads(raw_book.decode("utf-8"))
        best_bid = _safe_float(book.get("best_bid"), 0.0)
        best_ask = _safe_float(book.get("best_ask"), 1.0)
        market_p = (best_bid + best_ask) / 2.0 if best_ask >= best_bid else _safe_float(book.get("last_price"), 0.5)

        delta = p_hat - market_p
        spread = max(best_ask - best_bid, 0.0)
        costs = 0.0025 + 0.25 * spread
        edge_net = abs(delta) - costs
        confidence = _clamp(1.0 - max(p_high - p_low, 0.0))

        side = Side.BUY if delta >= 0 else Side.SELL
        if edge_net >= settings.entry_edge_threshold and confidence >= settings.min_confidence:
            action = SignalAction.ENTER
        elif edge_net <= settings.exit_edge_threshold:
            action = SignalAction.EXIT
        else:
            action = SignalAction.HOLD

        signal = EdgeSignal(
            timestamp=datetime.now(UTC),
            market_id=market_id,
            outcome_id=outcome_id,
            action=action,
            suggested_side=side,
            edge_net=edge_net,
            confidence=confidence,
            reason=f"delta={delta:.4f},costs={costs:.4f},market_p={market_p:.4f}",
        )
        db.insert_signal(
            ts=signal.timestamp,
            market_id=signal.market_id,
            outcome_id=signal.outcome_id,
            edge_net=signal.edge_net,
            action=signal.action.value,
            confidence=signal.confidence,
            reason=signal.reason,
        )
        publish_event(redis, TOPIC_EDGE_SIGNAL, signal)
        emitted += 1

    EVENT_OUT_TOTAL.labels(service=service, topic=TOPIC_EDGE_SIGNAL).inc(emitted)
    logger.info("edge tick", extra={"timestamp": datetime.now(UTC).isoformat(), "signals": emitted})


if __name__ == "__main__":
    run_loop("edge_engine", tick)
