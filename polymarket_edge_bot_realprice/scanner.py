import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from config import (
    GAMMA_MARKETS_API,
    PAGE_SIZE,
    REQUEST_BACKOFF_SECONDS,
    REQUEST_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
)


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_json_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _parse_yes_price(market):
    prices = _parse_json_list(market.get("outcomePrices"))
    outcomes = _parse_json_list(market.get("outcomes"))
    if not prices:
        return None

    yes_index = 0
    for index, outcome in enumerate(outcomes):
        if isinstance(outcome, str) and outcome.strip().lower() in {"yes", "true"}:
            yes_index = index
            break

    if yes_index >= len(prices):
        yes_index = 0
    return _safe_float(prices[yes_index])


def _parse_yes_token_id(market):
    token_ids = _parse_json_list(market.get("clobTokenIds"))
    outcomes = _parse_json_list(market.get("outcomes"))
    if not token_ids:
        return None

    yes_index = 0
    for index, outcome in enumerate(outcomes):
        if isinstance(outcome, str) and outcome.strip().lower() in {"yes", "true"}:
            yes_index = index
            break

    if yes_index >= len(token_ids):
        yes_index = 0
    return str(token_ids[yes_index])


def _extract_event_slug(market):
    event_slug = market.get("eventSlug")
    if isinstance(event_slug, str) and event_slug.strip():
        return event_slug.strip()

    events = market.get("events")
    if isinstance(events, str):
        try:
            events = json.loads(events)
        except json.JSONDecodeError:
            events = []
    if isinstance(events, list):
        for event in events:
            if isinstance(event, dict):
                slug = event.get("slug")
                if isinstance(slug, str) and slug.strip():
                    return slug.strip()
    return None


def _hours_to_close(end_date):
    if not end_date:
        return None
    try:
        end_dt = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = end_dt - now
        return delta.total_seconds() / 3600
    except ValueError:
        return None


def _fetch_json(url):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; edge-bot/2.0)"}
    last_error = None

    for attempt in range(REQUEST_RETRIES + 1):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < REQUEST_RETRIES:
                sleep_seconds = REQUEST_BACKOFF_SECONDS * (2**attempt)
                time.sleep(sleep_seconds)

    raise RuntimeError(f"request failed: {last_error}")


def _normalize_market(raw):
    best_bid = _safe_float(raw.get("bestBid"))
    best_ask = _safe_float(raw.get("bestAsk"))
    spread = None
    mid_from_book = None

    if (
        best_bid is not None
        and best_ask is not None
        and 0 <= best_bid <= 1
        and 0 <= best_ask <= 1
        and best_ask >= best_bid
    ):
        spread = best_ask - best_bid
        mid_from_book = (best_ask + best_bid) / 2

    yes_price = _parse_yes_price(raw)
    ref_price = mid_from_book if mid_from_book is not None else yes_price

    return {
        "id": raw.get("id"),
        "question": raw.get("question"),
        "slug": raw.get("slug"),
        "event_slug": _extract_event_slug(raw),
        "active": bool(raw.get("active", False)),
        "closed": bool(raw.get("closed", False)),
        "volume": _safe_float(raw.get("volume")) or 0.0,
        "volume24h": _safe_float(raw.get("volume24hr"))
        or _safe_float(raw.get("volume24hrClob"))
        or 0.0,
        "liquidity": _safe_float(raw.get("liquidity")) or 0.0,
        "yes_price": yes_price,
        "ref_price": ref_price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "last_trade": _safe_float(raw.get("lastTradePrice")),
        "one_hour_change": _safe_float(raw.get("oneHourPriceChange")) or 0.0,
        "one_day_change": _safe_float(raw.get("oneDayPriceChange")) or 0.0,
        "one_week_change": _safe_float(raw.get("oneWeekPriceChange")) or 0.0,
        "end_date": raw.get("endDate"),
        "hours_to_close": _hours_to_close(raw.get("endDate")),
        "token_yes": _parse_yes_token_id(raw),
    }


def fetch_markets(limit=5000):
    markets = []
    offset = 0
    page_size = max(10, min(PAGE_SIZE, 500))

    while len(markets) < limit:
        params = {
            "limit": page_size,
            "offset": offset,
            "closed": "false",
            "archived": "false",
        }
        url = f"{GAMMA_MARKETS_API}?{urllib.parse.urlencode(params)}"

        try:
            data = _fetch_json(url)
        except Exception as exc:  # noqa: BLE001
            print("API error", exc)
            break

        if not isinstance(data, list) or not data:
            break

        for raw_market in data:
            markets.append(_normalize_market(raw_market))
            if len(markets) >= limit:
                break

        offset += page_size

    return markets[:limit]
