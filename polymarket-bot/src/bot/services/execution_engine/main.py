import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from bot.config import get_settings
from bot.constants import TOPIC_EXECUTION_EVENT, TOPIC_RISK_DECISION, book_key, stream_offset_key
from bot.db import get_db
from bot.event_bus import consume_events, publish_event
from bot.execution.live_clob import ClobLiveClient
from bot.metrics import EVENT_IN_TOTAL, EVENT_OUT_TOTAL
from bot.redis_client import get_redis
from bot.schemas import OrderEvent, OrderStatus, OrderType, Side, SignalAction
from bot.services.common import run_loop

ALLOWED_TRANSITIONS = {
    OrderStatus.NEW.value: {
        OrderStatus.OPEN.value,
        OrderStatus.FILLED.value,
        OrderStatus.CANCELED.value,
        OrderStatus.REJECTED.value,
    },
    OrderStatus.OPEN.value: {
        OrderStatus.FILLED.value,
        OrderStatus.PARTIALLY_FILLED.value,
        OrderStatus.CANCELED.value,
        OrderStatus.REJECTED.value,
    },
    OrderStatus.PARTIALLY_FILLED.value: {OrderStatus.FILLED.value, OrderStatus.CANCELED.value},
}


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _book_prices(redis: Any, market_id: str, outcome_id: str) -> tuple[float, float, float]:
    raw = redis.get(book_key(market_id, outcome_id))
    if not raw:
        return 0.0, 1.0, 0.5
    payload = json.loads(raw.decode("utf-8"))
    best_bid = _safe_float(payload.get("best_bid"), 0.0)
    best_ask = _safe_float(payload.get("best_ask"), 1.0)
    mid = (best_bid + best_ask) / 2.0 if best_ask >= best_bid else _safe_float(payload.get("last_price"), 0.5)
    return best_bid, best_ask, mid


def _apply_fill_to_position(
    db: Any,
    market_id: str,
    outcome_id: str,
    side: Side,
    quantity: float,
    fill_price: float,
    mark_price: float,
) -> None:
    position = db.get_position(market_id, outcome_id)
    current_qty = _safe_float(position["qty"])
    current_avg = _safe_float(position["avg_price"])
    current_realized = _safe_float(position["realized_pnl"])

    delta = quantity if side == Side.BUY else -quantity
    new_qty = current_qty + delta
    new_avg = current_avg
    realized = current_realized
    epsilon = 1e-12

    if abs(current_qty) < epsilon or current_qty * delta > 0:
        total_abs = abs(current_qty) + abs(delta)
        new_avg = ((abs(current_qty) * current_avg) + (abs(delta) * fill_price)) / max(total_abs, epsilon)
    else:
        closed_qty = min(abs(current_qty), abs(delta))
        if current_qty > 0:
            realized += (fill_price - current_avg) * closed_qty
        else:
            realized += (current_avg - fill_price) * closed_qty
        if abs(new_qty) < epsilon:
            new_avg = 0.0
        elif current_qty * new_qty < 0:
            new_avg = fill_price

    if new_qty > 0:
        unrealized = (mark_price - new_avg) * new_qty
    elif new_qty < 0:
        unrealized = (new_avg - mark_price) * abs(new_qty)
    else:
        unrealized = 0.0

    db.upsert_position(
        market_id=market_id,
        outcome_id=outcome_id,
        qty=new_qty,
        avg_price=new_avg,
        unrealized_pnl=unrealized,
        realized_pnl=realized,
    )


def _transition_order_status(db: Any, client_order_id: str, next_status: OrderStatus) -> bool:
    current = db.get_order_status(client_order_id)
    if current is None:
        return False
    if current == next_status.value:
        return True
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if next_status.value not in allowed:
        return False
    db.update_order_status(client_order_id, next_status.value)
    return True


def _emit_order_event(redis: Any, event: OrderEvent) -> None:
    publish_event(redis, TOPIC_EXECUTION_EVENT, event)
    EVENT_OUT_TOTAL.labels(service="execution_engine", topic=TOPIC_EXECUTION_EVENT).inc()


def _fill_order(
    db: Any,
    redis: Any,
    order: dict[str, Any],
    fill_price: float,
    fee_bps: float,
    fee_override: float | None = None,
    fill_size_override: float | None = None,
) -> bool:
    size = fill_size_override if fill_size_override is not None else _safe_float(order["size"])
    if size <= 0:
        return False
    side = Side(str(order["side"]))
    transitioned = _transition_order_status(db, str(order["client_order_id"]), OrderStatus.FILLED)
    if not transitioned:
        return False
    fee = fee_override if fee_override is not None else size * fill_price * fee_bps / 10000.0
    db.insert_fill(order_id=int(order["id"]), price=fill_price, size=size, fee=fee)
    _, _, mid = _book_prices(redis, str(order["market_id"]), str(order["outcome_id"]))
    _apply_fill_to_position(
        db=db,
        market_id=str(order["market_id"]),
        outcome_id=str(order["outcome_id"]),
        side=side,
        quantity=size,
        fill_price=fill_price,
        mark_price=mid,
    )
    _emit_order_event(
        redis,
        OrderEvent(
            timestamp=datetime.now(UTC),
            client_order_id=str(order["client_order_id"]),
            status=OrderStatus.FILLED,
            filled_size=size,
            avg_fill_price=fill_price,
            fee_paid=fee,
        ),
    )
    return True


def _cancel_order(db: Any, redis: Any, client_order_id: str) -> bool:
    transitioned = _transition_order_status(db, client_order_id, OrderStatus.CANCELED)
    if transitioned:
        _emit_order_event(
            redis,
            OrderEvent(
                timestamp=datetime.now(UTC),
                client_order_id=client_order_id,
                status=OrderStatus.CANCELED,
                filled_size=0.0,
                avg_fill_price=0.0,
                fee_paid=0.0,
            ),
        )
    return transitioned


def _new_local_order(
    db: Any,
    market_id: str,
    outcome_id: str,
    side: Side,
    order_type: OrderType,
    target_price: float,
    quantity: float,
) -> dict[str, Any]:
    client_order_id = f"{int(datetime.now(UTC).timestamp() * 1000)}-{uuid4().hex[:10]}"
    order_id = db.insert_order(
        client_order_id=client_order_id,
        exchange_order_id=None,
        market_id=market_id,
        outcome_id=outcome_id,
        side=side.value,
        order_type=order_type.value,
        price=target_price,
        size=quantity,
        status=OrderStatus.NEW.value,
    )
    return {
        "id": order_id,
        "client_order_id": client_order_id,
        "exchange_order_id": None,
        "market_id": market_id,
        "outcome_id": outcome_id,
        "side": side.value,
        "type": order_type.value,
        "price": target_price,
        "size": quantity,
        "created_at": datetime.now(UTC),
    }


def _reconcile_paper_orders(db: Any, redis: Any, maker_timeout_seconds: int, fee_bps: float) -> int:
    filled = 0
    now = datetime.now(UTC)
    open_orders = db.list_open_orders()
    for order in open_orders:
        best_bid, best_ask, _ = _book_prices(redis, str(order["market_id"]), str(order["outcome_id"]))
        price = _safe_float(order["price"])
        side = Side(str(order["side"]))
        age_seconds = (now - order["created_at"]).total_seconds()
        crossed = (side == Side.BUY and price >= best_ask) or (side == Side.SELL and price <= best_bid)

        if crossed:
            fill_price = best_ask if side == Side.BUY else best_bid
            if _fill_order(db, redis, order, fill_price=fill_price, fee_bps=fee_bps):
                filled += 1
            continue

        if age_seconds >= maker_timeout_seconds:
            fallback_price = best_ask if side == Side.BUY else best_bid
            if fallback_price <= 0:
                _cancel_order(db, redis, str(order["client_order_id"]))
                continue
            if _fill_order(db, redis, order, fill_price=fallback_price, fee_bps=fee_bps):
                filled += 1
    return filled


def _reconcile_live_orders(
    db: Any,
    redis: Any,
    live: ClobLiveClient,
    maker_timeout_seconds: int,
    fee_bps: float,
) -> int:
    now = datetime.now(UTC)
    open_orders = db.list_open_orders()
    if not open_orders:
        return 0

    ok_open, remote_open_ids = live.fetch_open_order_ids()
    if not ok_open:
        return 0
    ok_fills, recent_fills = live.fetch_recent_fills()
    filled = 0

    for order in open_orders:
        client_id = str(order["client_order_id"])
        exchange_id = str(order["exchange_order_id"] or "")
        identifiers = {client_id}
        if exchange_id:
            identifiers.add(exchange_id)
        if any(identifier in remote_open_ids for identifier in identifiers):
            if db.get_order_status(client_id) == OrderStatus.NEW.value:
                _transition_order_status(db, client_id, OrderStatus.OPEN)
            continue

        fill_payload: dict[str, float] | None = None
        if ok_fills:
            for identifier in identifiers:
                if identifier in recent_fills:
                    fill_payload = recent_fills[identifier]
                    break

        if fill_payload and _safe_float(fill_payload.get("size")) > 0:
            fill_size = min(_safe_float(fill_payload.get("size")), _safe_float(order["size"]))
            fill_price = _safe_float(fill_payload.get("price"), _safe_float(order["price"]))
            fee = _safe_float(fill_payload.get("fee"), fill_size * fill_price * fee_bps / 10000.0)
            if _fill_order(
                db=db,
                redis=redis,
                order=order,
                fill_price=fill_price,
                fee_bps=fee_bps,
                fee_override=fee,
                fill_size_override=fill_size,
            ):
                filled += 1
            continue

        age_seconds = (now - order["created_at"]).total_seconds()
        if age_seconds >= maker_timeout_seconds:
            live.cancel_orders([client_id])
            _cancel_order(db, redis, client_id)
    return filled


def _execution_quantity(db: Any, market_id: str, outcome_id: str, action: str, target_price: float, capped_size_usd: float) -> float:
    if action == SignalAction.EXIT.value:
        return abs(_safe_float(db.get_position(market_id, outcome_id)["qty"]))
    notional_price = max(target_price, 1e-6)
    return capped_size_usd / notional_price


def tick() -> None:
    settings = get_settings()
    service = "execution_engine"
    logger = logging.getLogger(service)
    redis = get_redis()
    db = get_db()
    mode = settings.execution_mode.strip().lower()

    if mode == "live":
        live = ClobLiveClient(settings)
        reconciled = _reconcile_live_orders(
            db=db,
            redis=redis,
            live=live,
            maker_timeout_seconds=settings.maker_timeout_seconds,
            fee_bps=settings.paper_fee_bps,
        )
    else:
        live = None
        reconciled = _reconcile_paper_orders(
            db=db,
            redis=redis,
            maker_timeout_seconds=settings.maker_timeout_seconds,
            fee_bps=settings.paper_fee_bps,
        )

    events = consume_events(
        redis=redis,
        topic=TOPIC_RISK_DECISION,
        offset_key=stream_offset_key(service, TOPIC_RISK_DECISION),
        count=200,
    )
    if not events:
        if reconciled > 0:
            logger.info("execution reconcile", extra={"mode": mode, "filled": reconciled})
        return

    EVENT_IN_TOTAL.labels(service=service, topic=TOPIC_RISK_DECISION).inc(len(events))

    created = 0
    rejected = 0
    for event in events:
        if not bool(event.get("allow", False)):
            continue

        market_id = str(event["market_id"])
        outcome_id = str(event["outcome_id"])
        side = Side(str(event.get("side", Side.BUY.value)))
        order_type = OrderType(str(event.get("order_type", OrderType.LIMIT.value)))
        target_price = _safe_float(event.get("target_price"), 0.5)
        capped_size_usd = _safe_float(event.get("capped_size_usd"))
        action = str(event.get("action", SignalAction.HOLD.value))
        if capped_size_usd <= 0:
            continue

        quantity = _execution_quantity(
            db=db,
            market_id=market_id,
            outcome_id=outcome_id,
            action=action,
            target_price=target_price,
            capped_size_usd=capped_size_usd,
        )
        if quantity <= 0:
            continue

        order = _new_local_order(
            db=db,
            market_id=market_id,
            outcome_id=outcome_id,
            side=side,
            order_type=order_type,
            target_price=target_price,
            quantity=quantity,
        )

        if mode == "live" and live is not None:
            submit = live.submit_order(
                client_order_id=str(order["client_order_id"]),
                market_id=market_id,
                outcome_id=outcome_id,
                side=side.value,
                order_type=order_type.value,
                price=target_price,
                size=quantity,
            )
            if not submit.accepted:
                _transition_order_status(db, str(order["client_order_id"]), OrderStatus.REJECTED)
                rejected += 1
                continue

            if submit.exchange_order_id:
                db.set_order_exchange_id(str(order["client_order_id"]), submit.exchange_order_id)
                order["exchange_order_id"] = submit.exchange_order_id

            is_immediate_fill = submit.status.lower() in {"filled", "matched"} or submit.filled_size > 0
            if is_immediate_fill:
                fill_size = min(quantity, submit.filled_size) if submit.filled_size > 0 else quantity
                fill_price = submit.avg_fill_price if submit.avg_fill_price > 0 else target_price
                fee = submit.fee_paid if submit.fee_paid > 0 else None
                _fill_order(
                    db=db,
                    redis=redis,
                    order=order,
                    fill_price=fill_price,
                    fee_bps=settings.paper_fee_bps,
                    fee_override=fee,
                    fill_size_override=fill_size,
                )
            else:
                _transition_order_status(db, str(order["client_order_id"]), OrderStatus.OPEN)
        else:
            _transition_order_status(db, str(order["client_order_id"]), OrderStatus.OPEN)
            best_bid, best_ask, _ = _book_prices(redis, market_id, outcome_id)
            if order_type == OrderType.MARKET:
                fill_price = best_ask if side == Side.BUY else best_bid
                _fill_order(db, redis, order, fill_price=fill_price, fee_bps=settings.paper_fee_bps)
            else:
                crossed = (side == Side.BUY and target_price >= best_ask) or (side == Side.SELL and target_price <= best_bid)
                if crossed:
                    fill_price = best_ask if side == Side.BUY else best_bid
                    _fill_order(db, redis, order, fill_price=fill_price, fee_bps=settings.paper_fee_bps)
        created += 1

    logger.info(
        "execution tick",
        extra={
            "mode": mode,
            "decisions": len(events),
            "orders_created": created,
            "rejected": rejected,
            "reconciled": reconciled,
        },
    )


if __name__ == "__main__":
    run_loop("execution_engine", tick)
