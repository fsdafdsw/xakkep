import logging
from collections import deque
from datetime import UTC, datetime
from typing import Any

from bot.constants import TOPIC_FEATURE_VECTOR, TOPIC_MARKET_SNAPSHOT, stream_offset_key
from bot.event_bus import consume_events, publish_event
from bot.metrics import EVENT_IN_TOTAL, EVENT_OUT_TOTAL
from bot.redis_client import get_redis
from bot.schemas import FeatureVector
from bot.services.common import run_loop

PRICE_HISTORY: dict[str, deque[tuple[float, float]]] = {}
MAX_HISTORY_POINTS = 1000


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def _update_price_history(key: str, timestamp_s: float, mid_price: float) -> None:
    history = PRICE_HISTORY.setdefault(key, deque(maxlen=MAX_HISTORY_POINTS))
    history.append((timestamp_s, mid_price))


def _metrics_from_history(key: str, now_s: float) -> tuple[float, float]:
    history = PRICE_HISTORY.get(key)
    if not history:
        return 0.0, 0.0
    points = [(ts, p) for ts, p in history if now_s - ts <= 3600]
    if len(points) < 2:
        return 0.0, 0.0

    momentum_window = [p for ts, p in points if now_s - ts <= 300]
    if len(momentum_window) < 2:
        momentum = 0.0
    else:
        baseline = max(momentum_window[0], 1e-6)
        momentum = (momentum_window[-1] - momentum_window[0]) / baseline

    returns: list[float] = []
    for idx in range(1, len(points)):
        prev = max(points[idx - 1][1], 1e-6)
        current = points[idx][1]
        returns.append((current - prev) / prev)
    if not returns:
        return momentum, 0.0
    mean = sum(returns) / len(returns)
    variance = sum((ret - mean) ** 2 for ret in returns) / len(returns)
    volatility = variance ** 0.5
    return momentum, volatility


def tick() -> None:
    logger = logging.getLogger("feature_service")
    redis = get_redis()
    service = "feature_service"
    events = consume_events(
        redis=redis,
        topic=TOPIC_MARKET_SNAPSHOT,
        offset_key=stream_offset_key(service, TOPIC_MARKET_SNAPSHOT),
        count=200,
    )
    if not events:
        return

    EVENT_IN_TOTAL.labels(service=service, topic=TOPIC_MARKET_SNAPSHOT).inc(len(events))

    generated = 0
    for event in events:
        market_id = str(event["market_id"])
        outcome_id = str(event["outcome_id"])
        best_bid = _safe_float(event.get("best_bid"))
        best_ask = _safe_float(event.get("best_ask"), 1.0)
        depth_bid = _safe_float(event.get("depth_bid"))
        depth_ask = _safe_float(event.get("depth_ask"))
        mid = (best_bid + best_ask) / 2.0 if best_ask >= best_bid else _safe_float(event.get("last_price"), 0.5)

        now = _parse_timestamp(event.get("timestamp"))
        timestamp_s = now.timestamp()
        history_key = f"{market_id}:{outcome_id}"
        _update_price_history(history_key, timestamp_s, mid)
        momentum_5m, volatility_1h = _metrics_from_history(history_key, timestamp_s)

        spread = max(best_ask - best_bid, 0.0)
        depth_total = depth_bid + depth_ask + 1e-9
        imbalance = (depth_bid - depth_ask) / depth_total
        external_key = f"external:{market_id}:{outcome_id}"
        external_score_raw = redis.get(external_key)
        external_score = _safe_float(external_score_raw.decode("utf-8") if external_score_raw else 0.0)

        vector = FeatureVector(
            timestamp=now,
            market_id=market_id,
            outcome_id=outcome_id,
            mid_price=mid,
            spread=spread,
            imbalance=imbalance,
            momentum_5m=momentum_5m,
            volatility_1h=volatility_1h,
            external_score=external_score,
        )
        publish_event(redis, TOPIC_FEATURE_VECTOR, vector)
        generated += 1

    EVENT_OUT_TOTAL.labels(service=service, topic=TOPIC_FEATURE_VECTOR).inc(generated)
    logger.info("feature tick", extra={"timestamp": datetime.now(UTC).isoformat(), "vectors": generated})


if __name__ == "__main__":
    run_loop("feature_service", tick)
