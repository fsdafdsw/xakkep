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
from entity_normalization import extract_market_entities
from graph_residuals import annotate_relation_residuals
from market_profile import enrich_market_profile
from relations import annotate_market_relations
from resolution_parser import parse_resolution_semantics


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
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


def _selected_outcome_index(outcomes, prices, token_ids):
    if not outcomes and not prices and not token_ids:
        return 0
    size = max(len(outcomes), len(prices), len(token_ids))
    if size <= 0:
        return 0

    for index, outcome in enumerate(outcomes):
        if isinstance(outcome, str) and outcome.strip().lower() in {"yes", "true"}:
            if index < size:
                return index
    return 0


def _selected_outcome_name(outcomes, index):
    if 0 <= index < len(outcomes):
        name = str(outcomes[index]).strip()
        if name:
            return name
    return f"Outcome #{index + 1}"


def _value_at(seq, index, cast=str):
    if not (0 <= index < len(seq)):
        return None
    value = seq[index]
    try:
        return cast(value)
    except (TypeError, ValueError):
        return None


def _extract_event_meta(market):
    meta = {
        "event_slug": None,
        "event_id": str(market.get("eventId")) if market.get("eventId") else None,
        "event_title": market.get("eventTitle") or market.get("title"),
        "event_description": market.get("eventDescription"),
        "event_category": market.get("category"),
        "resolution_source": market.get("resolutionSource"),
        "event_market_count": _safe_int(
            market.get("eventMarketCount") or market.get("marketCount") or market.get("marketsCount")
        ),
    }

    event_slug = market.get("eventSlug")
    if isinstance(event_slug, str) and event_slug.strip():
        meta["event_slug"] = event_slug.strip()

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
                if meta["event_slug"] is None and isinstance(slug, str) and slug.strip():
                    meta["event_slug"] = slug.strip()

                ev_id = event.get("id")
                if meta["event_id"] is None and ev_id is not None:
                    meta["event_id"] = str(ev_id)

                if not meta["event_title"]:
                    meta["event_title"] = event.get("title")
                if not meta["event_description"]:
                    meta["event_description"] = event.get("description")
                if not meta["event_category"]:
                    meta["event_category"] = event.get("category")
                if not meta["resolution_source"]:
                    meta["resolution_source"] = event.get("resolutionSource")
                if meta["event_market_count"] is None:
                    event_markets = event.get("markets")
                    if isinstance(event_markets, list):
                        meta["event_market_count"] = len(event_markets)
                    else:
                        meta["event_market_count"] = _safe_int(
                            event.get("marketsCount") or event.get("marketCount") or event.get("numMarkets")
                        )
    return meta


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
    event_meta = _extract_event_meta(raw)
    outcomes = _parse_json_list(raw.get("outcomes"))
    prices = _parse_json_list(raw.get("outcomePrices"))
    token_ids = _parse_json_list(raw.get("clobTokenIds"))
    selected_idx = _selected_outcome_index(outcomes, prices, token_ids)
    selected_outcome = _selected_outcome_name(outcomes, selected_idx)
    selected_price = _value_at(prices, selected_idx, cast=float)
    selected_token = _value_at(token_ids, selected_idx, cast=str)

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

    ref_price = mid_from_book if mid_from_book is not None else selected_price

    market = {
        "id": raw.get("id"),
        "event_id": event_meta["event_id"],
        "question": raw.get("question"),
        "slug": raw.get("slug"),
        "event_slug": event_meta["event_slug"],
        "event_title": event_meta["event_title"],
        "event_description": event_meta["event_description"],
        "event_category": event_meta["event_category"],
        "resolution_source": event_meta["resolution_source"],
        "event_market_count": event_meta["event_market_count"],
        "active": bool(raw.get("active", False)),
        "closed": bool(raw.get("closed", False)),
        "volume": _safe_float(raw.get("volume")) or 0.0,
        "volume24h": _safe_float(raw.get("volume24hr"))
        or _safe_float(raw.get("volume24hrClob"))
        or 0.0,
        "liquidity": _safe_float(raw.get("liquidity")) or 0.0,
        "outcome_count": max(len(outcomes), len(prices), len(token_ids)),
        "outcomes": outcomes,
        "selected_outcome_index": selected_idx,
        "selected_outcome": selected_outcome,
        "selected_price": selected_price,
        "selected_token_id": selected_token,
        "market_description": raw.get("description"),
        # Backward compatibility for older code paths.
        "yes_price": selected_price,
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
        "token_yes": selected_token,
    }
    enrich_market_profile(market)
    market["resolution_metadata"] = parse_resolution_semantics(market)
    market["entity_metadata"] = extract_market_entities(market)
    market["primary_entity_key"] = (
        market["resolution_metadata"].get("subject_entity_key")
        or next(iter(market["entity_metadata"].get("entity_keys") or []), None)
    )
    return market


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

    selected = markets[:limit]
    relation_graph = annotate_market_relations(selected)
    annotate_relation_residuals(selected, relation_graph=relation_graph)
    return selected
