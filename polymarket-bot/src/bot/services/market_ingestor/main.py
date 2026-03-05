import logging
import json
from hashlib import sha256
from datetime import UTC, datetime
from datetime import timedelta
from typing import Any

import httpx

from bot.config import get_settings
from bot.constants import TOPIC_MARKET_SNAPSHOT, book_key
from bot.db import get_db
from bot.event_bus import publish_event
from bot.metrics import EVENT_OUT_TOTAL
from bot.redis_client import get_redis
from bot.schemas import MarketSnapshot
from bot.services.common import run_loop


def _safe_float(raw: Any, fallback: float = 0.0) -> float:
    try:
        if raw is None:
            return fallback
        return float(raw)
    except (TypeError, ValueError):
        return fallback


def _parse_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _parse_close_time(raw: Any) -> datetime:
    if isinstance(raw, str):
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC) + timedelta(days=30)


def _extract_market_id(item: dict[str, Any]) -> str | None:
    for key in ("conditionId", "id", "marketId", "slug"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _extract_markets(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    return []


def _extract_outcomes(item: dict[str, Any]) -> list[tuple[str, str, float]]:
    token_ids = _parse_list(item.get("clobTokenIds") or item.get("tokenIds"))
    outcomes = _parse_list(item.get("outcomes"))
    prices = _parse_list(item.get("outcomePrices"))
    result: list[tuple[str, str, float]] = []
    for index, token_id in enumerate(token_ids):
        outcome_id = str(token_id)
        outcome_label = str(outcomes[index]) if index < len(outcomes) else f"outcome_{index}"
        last_price = _safe_float(prices[index], fallback=0.5) if index < len(prices) else 0.5
        result.append((outcome_id, outcome_label, last_price))
    return result


def _book_levels(book: dict[str, Any], side: str) -> list[dict[str, Any]]:
    levels = book.get(side)
    if isinstance(levels, list):
        return [entry for entry in levels if isinstance(entry, dict)]
    return []


def _book_stats(book: dict[str, Any]) -> tuple[float, float, float, float]:
    bids = _book_levels(book, "bids")
    asks = _book_levels(book, "asks")
    bid_prices = [_safe_float(level.get("price")) for level in bids]
    ask_prices = [_safe_float(level.get("price"), 1.0) for level in asks]
    best_bid = max(bid_prices) if bid_prices else 0.0
    best_ask = min(ask_prices) if ask_prices else 1.0
    spread = max(best_ask - best_bid, 0.0)
    depth_bid = sum(_safe_float(level.get("size")) for level in bids[:10])
    depth_ask = sum(_safe_float(level.get("size")) for level in asks[:10])
    return best_bid, best_ask, spread, depth_bid, depth_ask


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


def tick() -> None:
    settings = get_settings()
    logger = logging.getLogger("market_ingestor")
    if not settings.market_ingestor_enabled:
        logger.debug("ingestor disabled by config")
        return
    db = get_db()
    redis = get_redis()
    now = datetime.now(UTC)
    client = httpx.Client(
        timeout=settings.ingest_timeout_seconds,
        follow_redirects=True,
        headers={"User-Agent": "polymarket-bot-blueprint/0.1"},
    )

    try:
        response = client.get(
            f"{settings.polymarket_gamma_base_url}/markets",
            params={"limit": settings.ingest_market_limit, "closed": "false"},
        )
        response.raise_for_status()
        markets = _extract_markets(response.json())
    finally:
        client.close()

    snapshots_published = 0
    book_client = httpx.Client(
        timeout=settings.ingest_timeout_seconds,
        headers={"User-Agent": "polymarket-bot-blueprint/0.1"},
    )
    try:
        for market in markets:
            market_id = _extract_market_id(market)
            if not market_id:
                continue

            title = str(market.get("question") or market.get("title") or market_id)
            category = str(market.get("category") or "unknown")
            close_time = _parse_close_time(
                market.get("endDate") or market.get("endDateIso") or market.get("closeTime")
            )
            rules_source = str(market.get("description") or market.get("rules") or title)
            rules_hash = sha256(rules_source.encode("utf-8")).hexdigest()
            status = "active" if bool(market.get("active", True)) else "inactive"
            db.upsert_market(
                market_id=market_id,
                title=title,
                category=category,
                close_time=close_time,
                rules_hash=rules_hash,
                status=status,
            )

            volume = _safe_float(market.get("volume24hr"), 0.0)
            outcomes = _extract_outcomes(market)
            for outcome_id, _, inferred_last_price in outcomes:
                book_response = book_client.get(
                    f"{settings.polymarket_clob_base_url}/book",
                    params={"token_id": outcome_id},
                )
                if book_response.status_code != 200:
                    continue
                raw_book = book_response.json()
                book = raw_book.get("book") if isinstance(raw_book, dict) and isinstance(raw_book.get("book"), dict) else raw_book
                if not isinstance(book, dict):
                    continue

                best_bid, best_ask, spread, depth_bid, depth_ask = _book_stats(book)
                if best_bid == 0.0 and best_ask == 1.0 and depth_bid == 0.0 and depth_ask == 0.0:
                    continue

                mid = (best_bid + best_ask) / 2.0 if best_ask >= best_bid else inferred_last_price
                snapshot = MarketSnapshot(
                    timestamp=now,
                    market_id=market_id,
                    outcome_id=outcome_id,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    depth_bid=depth_bid,
                    depth_ask=depth_ask,
                    last_price=mid,
                    volume_1h=volume / 24.0 if volume > 0 else 0.0,
                )
                db.insert_orderbook(
                    ts=snapshot.timestamp,
                    market_id=snapshot.market_id,
                    outcome_id=snapshot.outcome_id,
                    best_bid=snapshot.best_bid,
                    best_ask=snapshot.best_ask,
                    spread=spread,
                    depth_bid=snapshot.depth_bid,
                    depth_ask=snapshot.depth_ask,
                )
                publish_event(redis, TOPIC_MARKET_SNAPSHOT, snapshot)
                EVENT_OUT_TOTAL.labels(service="market_ingestor", topic=TOPIC_MARKET_SNAPSHOT).inc()
                redis.set(book_key(snapshot.market_id, snapshot.outcome_id), json.dumps(_book_payload(snapshot)), ex=300)
                snapshots_published += 1
    finally:
        book_client.close()

    logger.info(
        "ingestor tick complete",
        extra={"timestamp": now.isoformat(), "markets_seen": len(markets), "snapshots": snapshots_published},
    )


if __name__ == "__main__":
    run_loop("market_ingestor", tick)
