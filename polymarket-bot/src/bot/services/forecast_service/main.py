import logging
from datetime import UTC, datetime
from typing import Any

from bot.constants import TOPIC_FEATURE_VECTOR, TOPIC_FORECAST, stream_offset_key
from bot.db import get_db
from bot.event_bus import consume_events, publish_event
from bot.metrics import EVENT_IN_TOTAL, EVENT_OUT_TOTAL
from bot.redis_client import get_redis
from bot.schemas import Prediction
from bot.services.common import run_loop


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _clamp(value: float, low: float = 0.01, high: float = 0.99) -> float:
    return max(low, min(high, value))


def tick() -> None:
    service = "forecast_service"
    logger = logging.getLogger("forecast_service")
    redis = get_redis()
    db = get_db()
    events = consume_events(
        redis=redis,
        topic=TOPIC_FEATURE_VECTOR,
        offset_key=stream_offset_key(service, TOPIC_FEATURE_VECTOR),
        count=200,
    )
    if not events:
        return

    EVENT_IN_TOTAL.labels(service=service, topic=TOPIC_FEATURE_VECTOR).inc(len(events))

    emitted = 0
    for event in events:
        timestamp = datetime.now(UTC)
        market_id = str(event["market_id"])
        outcome_id = str(event["outcome_id"])
        mid = _safe_float(event.get("mid_price"), 0.5)
        spread = _safe_float(event.get("spread"))
        imbalance = _safe_float(event.get("imbalance"))
        momentum = _safe_float(event.get("momentum_5m"))
        volatility = _safe_float(event.get("volatility_1h"))
        external = _safe_float(event.get("external_score"))

        raw = mid + 0.30 * momentum + 0.08 * imbalance - 0.04 * spread + 0.05 * external
        p_hat = _clamp(raw)
        uncertainty = _clamp(0.05 + 0.60 * volatility + 0.40 * spread, low=0.02, high=0.45)
        p_low = _clamp(p_hat - uncertainty)
        p_high = _clamp(p_hat + uncertainty)

        prediction = Prediction(
            timestamp=timestamp,
            market_id=market_id,
            outcome_id=outcome_id,
            p_hat=p_hat,
            p_low=p_low,
            p_high=p_high,
            model_version="heuristic-v1",
        )
        db.insert_prediction(
            ts=prediction.timestamp,
            market_id=prediction.market_id,
            outcome_id=prediction.outcome_id,
            p_hat=prediction.p_hat,
            p_low=prediction.p_low,
            p_high=prediction.p_high,
            model_version=prediction.model_version,
        )
        publish_event(redis, TOPIC_FORECAST, prediction)
        emitted += 1

    EVENT_OUT_TOTAL.labels(service=service, topic=TOPIC_FORECAST).inc(emitted)
    logger.info("forecast tick", extra={"timestamp": datetime.now(UTC).isoformat(), "predictions": emitted})


if __name__ == "__main__":
    run_loop("forecast_service", tick)
