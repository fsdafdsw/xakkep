import logging
import json
from datetime import UTC, datetime
from typing import Any

from bot.config import get_settings
from bot.constants import KEY_TRADING_ENABLED, TOPIC_EDGE_SIGNAL, TOPIC_RISK_DECISION, book_key, stream_offset_key
from bot.db import get_db
from bot.event_bus import consume_events, publish_event
from bot.metrics import EVENT_IN_TOTAL, EVENT_OUT_TOTAL
from bot.redis_client import get_redis
from bot.schemas import OrderType, RiskDecision, Side, SignalAction
from bot.services.common import run_loop


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _trading_enabled(redis_value: bytes | None, default: bool) -> bool:
    if redis_value is None:
        return default
    raw = redis_value.decode("utf-8").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _book_prices(raw: bytes | None) -> tuple[float, float, float]:
    if not raw:
        return 0.0, 1.0, 0.5
    book = json.loads(raw.decode("utf-8"))
    best_bid = _safe_float(book.get("best_bid"), 0.0)
    best_ask = _safe_float(book.get("best_ask"), 1.0)
    mid = (best_bid + best_ask) / 2.0 if best_ask >= best_bid else _safe_float(book.get("last_price"), 0.5)
    return best_bid, best_ask, mid


def tick() -> None:
    settings = get_settings()
    service = "risk_engine"
    logger = logging.getLogger("risk_engine")
    redis = get_redis()
    db = get_db()

    if redis.get(KEY_TRADING_ENABLED) is None:
        redis.set(KEY_TRADING_ENABLED, "1" if settings.trading_enabled else "0")

    events = consume_events(
        redis=redis,
        topic=TOPIC_EDGE_SIGNAL,
        offset_key=stream_offset_key(service, TOPIC_EDGE_SIGNAL),
        count=200,
    )
    if not events:
        return

    EVENT_IN_TOTAL.labels(service=service, topic=TOPIC_EDGE_SIGNAL).inc(len(events))

    trading_enabled = _trading_enabled(redis.get(KEY_TRADING_ENABLED), settings.trading_enabled)
    open_orders = db.count_open_orders()
    daily_realized = db.daily_realized_pnl()
    emitted = 0

    for event in events:
        market_id = str(event["market_id"])
        outcome_id = str(event["outcome_id"])
        edge_net = _safe_float(event.get("edge_net"))
        confidence = _safe_float(event.get("confidence"), 0.0)
        try:
            action = SignalAction(str(event.get("action", SignalAction.HOLD.value)))
        except ValueError:
            action = SignalAction.HOLD
        try:
            suggested_side = Side(str(event.get("suggested_side", Side.BUY.value)))
        except ValueError:
            suggested_side = Side.BUY

        best_bid, best_ask, mid = _book_prices(redis.get(book_key(market_id, outcome_id)))
        spread = max(best_ask - best_bid, 0.0)
        position = db.get_position(market_id, outcome_id)
        position_qty = _safe_float(position["qty"])

        allow = True
        reason = "ok"
        side = suggested_side
        order_type = OrderType.LIMIT
        target_price = mid
        capped_size_usd = 0.0

        if not trading_enabled:
            allow = False
            reason = "trading_paused"
        elif daily_realized <= -abs(settings.max_daily_loss_usd):
            allow = False
            reason = "daily_loss_limit_reached"
        elif open_orders >= settings.max_open_orders:
            allow = False
            reason = "open_order_limit_reached"
        elif action == SignalAction.HOLD:
            allow = False
            reason = "hold_signal"
        elif action == SignalAction.ENTER:
            if edge_net < settings.entry_edge_threshold or confidence < settings.min_confidence:
                allow = False
                reason = "signal_below_threshold"
            else:
                edge_scale = min(1.0, edge_net / max(settings.entry_edge_threshold, 1e-6))
                capped_size_usd = max(5.0, settings.max_position_usd * settings.kelly_fraction * edge_scale)
                capped_size_usd = min(capped_size_usd, settings.max_position_usd)
                if side == Side.BUY:
                    target_price = min(0.99, best_bid + 0.25 * spread + 1e-6)
                else:
                    target_price = max(0.01, best_ask - 0.25 * spread - 1e-6)
                order_type = OrderType.LIMIT
        elif action == SignalAction.EXIT:
            if abs(position_qty) < 1e-9:
                allow = False
                reason = "no_position_to_exit"
            else:
                side = Side.SELL if position_qty > 0 else Side.BUY
                order_type = OrderType.MARKET
                target_price = best_bid if side == Side.SELL else best_ask
                capped_size_usd = abs(position_qty) * max(mid, 1e-6)
        else:
            allow = False
            reason = "unsupported_signal"

        decision = RiskDecision(
            timestamp=datetime.now(UTC),
            market_id=market_id,
            outcome_id=outcome_id,
            action=action,
            side=side,
            order_type=order_type,
            target_price=target_price,
            allow=allow,
            capped_size_usd=capped_size_usd,
            reason=reason,
        )
        publish_event(redis, TOPIC_RISK_DECISION, decision)
        if decision.allow:
            open_orders += 1
        emitted += 1

    EVENT_OUT_TOTAL.labels(service=service, topic=TOPIC_RISK_DECISION).inc(emitted)
    logger.info("risk tick", extra={"timestamp": datetime.now(UTC).isoformat(), "decisions": emitted})


if __name__ == "__main__":
    run_loop("risk_engine", tick)
