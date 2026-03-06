import argparse
import heapq
import json
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from config import (
    EDGE_THRESHOLD,
    ESTIMATED_SLIPPAGE_BPS,
    EVENT_NEUTRALIZATION_STRENGTH,
    EXCLUDED_QUESTION_PATTERNS,
    KELLY_FRACTION,
    MAX_BET_USD,
    MAX_PRICE,
    MAX_SIGNALS_PER_EVENT,
    MAX_SPREAD,
    MAX_TOTAL_EXPOSURE_PCT,
    MIN_CONFIDENCE,
    MIN_GROSS_EDGE,
    MIN_HOURS_TO_CLOSE,
    MIN_LIQUIDITY,
    MIN_PRICE,
    MIN_VOLUME,
    MODEL_ADJUSTMENT_SCALE,
    REQUIRE_ORDERBOOK,
    REQUEST_BACKOFF_SECONDS,
    REQUEST_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    TAKER_FEE_BPS,
)
from diagnostics import distribution_stats
from market_profile import enrich_market_profile
from probability_model import estimated_probability, kelly_bet_fraction, net_edge_after_costs
from strategy import evaluate_market

GAMMA_EVENTS_API = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY_API = "https://clob.polymarket.com/prices-history"


def _safe_float(value: Any):
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
            data = json.loads(value)
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _to_unix(iso_str: str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        return None


def _to_utc_str(ts: int):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _request_json(url: str):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; edge-bot-backtest/1.0)"}
    last_error = None
    for attempt in range(REQUEST_RETRIES + 1):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < REQUEST_RETRIES:
                sleep_time = REQUEST_BACKOFF_SECONDS * (2**attempt)
                time.sleep(sleep_time)
    raise RuntimeError(f"request failed: {last_error}")


def _closed_event_exists(offset: int):
    params = {"closed": "true", "limit": 1, "offset": offset}
    url = f"{GAMMA_EVENTS_API}?{urllib.parse.urlencode(params)}"
    data = _request_json(url)
    return bool(data)


def find_total_closed_events():
    low = 0
    high = 1
    while _closed_event_exists(high):
        high *= 2
        if high > 2_000_000:
            break

    while low < high:
        mid = (low + high + 1) // 2
        if _closed_event_exists(mid):
            low = mid
        else:
            high = mid - 1
    return low + 1


def fetch_closed_events(start_offset: int, max_events: int, page_size: int = 500):
    events = []
    offset = start_offset
    while len(events) < max_events:
        params = {"closed": "true", "limit": page_size, "offset": offset}
        url = f"{GAMMA_EVENTS_API}?{urllib.parse.urlencode(params)}"
        batch = _request_json(url)
        if not isinstance(batch, list) or not batch:
            break
        events.extend(batch)
        offset += page_size
    return events[:max_events]


def fetch_price_history(token_id: str, start_ts: int, end_ts: int, fidelity: int):
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity,
    }
    url = f"{CLOB_HISTORY_API}?{urllib.parse.urlencode(params)}"
    payload = _request_json(url)
    history = payload.get("history", []) if isinstance(payload, dict) else []
    parsed = []
    for point in history:
        if not isinstance(point, dict):
            continue
        ts = int(point.get("t", 0))
        price = _safe_float(point.get("p"))
        if ts > 0 and price is not None and 0 <= price <= 1:
            parsed.append((ts, price))
    parsed.sort(key=lambda x: x[0])
    return parsed


def price_at_or_before(history, ts):
    candidate = None
    for point_ts, point_price in history:
        if point_ts <= ts:
            candidate = point_price
        else:
            break
    return candidate


def change_over(history, ts, horizon_seconds):
    now_price = price_at_or_before(history, ts)
    prev_price = price_at_or_before(history, ts - horizon_seconds)
    if now_price is None or prev_price is None:
        return 0.0
    return now_price - prev_price


def _select_outcome_index(outcomes):
    for i, outcome in enumerate(outcomes):
        if isinstance(outcome, str) and outcome.strip().lower() in {"yes", "true"}:
            return i
    return 0


def _resolved_binary_outcome(price):
    if price is None:
        return None
    if price >= 0.99:
        return 1
    if price <= 0.01:
        return 0
    return None


def _clamp(value, low=0.01, high=0.99):
    return max(low, min(high, value))


def _candidate_event_key(candidate):
    return candidate.event_id or candidate.event_slug or candidate.market_slug


def _recompute_candidate_edges(candidate):
    candidate.gross_edge = candidate.fair - candidate.entry
    candidate.net_edge = net_edge_after_costs(
        fair_probability=candidate.fair,
        entry_price=candidate.entry,
        taker_fee_bps=TAKER_FEE_BPS,
        slippage_bps=ESTIMATED_SLIPPAGE_BPS,
        spread=candidate.spread,
    )


def _neutralize_candidates_by_event(candidates):
    grouped = {}
    for c in candidates:
        grouped.setdefault(_candidate_event_key(c), []).append(c)

    for group in grouped.values():
        if len(group) <= 1:
            continue
        deltas = [c.fair - c.entry for c in group]
        mean_delta = sum(deltas) / len(deltas)
        for c in group:
            centered = (c.fair - c.entry) - mean_delta
            c.fair = _clamp(c.entry + (centered * EVENT_NEUTRALIZATION_STRENGTH))
            _recompute_candidate_edges(c)


def _dedupe_per_event(candidates):
    grouped = {}
    for c in candidates:
        grouped.setdefault(_candidate_event_key(c), []).append(c)

    selected = []
    for group in grouped.values():
        group = sorted(group, key=lambda x: x.net_edge, reverse=True)
        selected.extend(group[:MAX_SIGNALS_PER_EVENT])
    return selected


def _passes_backtest_filters(market, rejects, use_liquidity_filter):
    question = str(market.get("question") or "").lower()
    for pattern in EXCLUDED_QUESTION_PATTERNS:
        if pattern and pattern in question:
            rejects["excluded_pattern"] += 1
            return False

    volume_ref = market.get("volume24h", 0.0) or market.get("volume", 0.0)
    price = market.get("ref_price")

    if use_liquidity_filter and market.get("liquidity", 0.0) < MIN_LIQUIDITY:
        rejects["low_liquidity"] += 1
        return False

    if volume_ref < MIN_VOLUME:
        rejects["low_volume"] += 1
        return False

    if price is None:
        rejects["no_price"] += 1
        return False

    if REQUIRE_ORDERBOOK and (
        market.get("best_bid") is None or market.get("best_ask") is None
    ):
        rejects["no_orderbook"] += 1
        return False

    if price < MIN_PRICE or price > MAX_PRICE:
        rejects["extreme_price"] += 1
        return False

    spread = market.get("spread")
    if spread is not None and spread > MAX_SPREAD:
        rejects["wide_spread"] += 1
        return False

    if market.get("hours_to_close", 0.0) < MIN_HOURS_TO_CLOSE:
        rejects["near_expiry"] += 1
        return False

    return True


@dataclass
class Candidate:
    event_id: str
    question: str
    event_slug: str
    market_slug: str
    market_type: str
    category_group: str
    token_id: str
    entry_ts: int
    settle_ts: int
    entry: float
    fair: float
    gross_edge: float
    net_edge: float
    confidence: float
    resolved_outcome: int
    spread: Optional[float]


@dataclass
class OpenPosition:
    question: str
    settle_ts: int
    shares: float
    cost_basis: float
    resolved_outcome: int
    entry_price: float


def _bump_stage(diag, stage_name, market_type=None, category_group=None):
    diag["stage_counts"][stage_name] += 1
    if market_type:
        diag["market_type_stage_counts"][stage_name][market_type] += 1
    if category_group:
        diag["category_stage_counts"][stage_name][category_group] += 1


def _finalize_stage_map(stage_map):
    return {
        stage_name: dict(sorted(counts.items()))
        for stage_name, counts in stage_map.items()
    }


def _build_rejection_payload(candidate, reason):
    return {
        "question": candidate.question,
        "event_slug": candidate.event_slug,
        "market_slug": candidate.market_slug,
        "market_type": candidate.market_type,
        "category_group": candidate.category_group,
        "entry_utc": _to_utc_str(candidate.entry_ts),
        "entry": candidate.entry,
        "fair": candidate.fair,
        "gross_edge": candidate.gross_edge,
        "net_edge": candidate.net_edge,
        "confidence": candidate.confidence,
        "reject_reason": reason,
        "link": f"https://polymarket.com/event/{candidate.event_slug}?tid={candidate.token_id}",
    }


def _top_edge_rejections(rejections, limit=8):
    ranked = sorted(
        rejections,
        key=lambda item: (
            item["net_edge"] if item["net_edge"] is not None else item["gross_edge"],
            item["gross_edge"],
        ),
        reverse=True,
    )
    return ranked[:limit]


_STAGE_NAMES = (
    "markets_seen",
    "markets_in_time_window",
    "binary_markets",
    "resolved_binary_markets",
    "history_requests",
    "history_available",
    "snapshot_ready",
    "passed_filters",
    "scored",
    "post_neutralization",
    "after_confidence",
    "after_gross_edge",
    "after_net_edge",
    "final_candidates",
)


def build_candidates(
    events,
    start_ts: int,
    end_ts: int,
    entry_hours_before_close: int,
    history_window_days: int,
    max_markets: int,
    fidelity: int,
    use_liquidity_filter: bool,
    max_history_requests: int,
):
    rejects = {
        "low_liquidity": 0,
        "low_volume": 0,
        "excluded_pattern": 0,
        "no_price": 0,
        "no_orderbook": 0,
        "extreme_price": 0,
        "wide_spread": 0,
        "near_expiry": 0,
        "low_confidence": 0,
        "low_gross_edge": 0,
    }
    reasons = {
        "not_binary": 0,
        "not_resolved": 0,
        "missing_history": 0,
        "no_entry_price": 0,
    }
    diagnostics = {
        "stage_counts": defaultdict(int, {name: 0 for name in _STAGE_NAMES}),
        "market_type_stage_counts": defaultdict(lambda: defaultdict(int)),
        "category_stage_counts": defaultdict(lambda: defaultdict(int)),
        "rejects_by_market_type": defaultdict(lambda: defaultdict(int)),
        "rejects_by_category": defaultdict(lambda: defaultdict(int)),
    }

    selected = []
    edge_rejections = []
    history_requests = 0
    stop_scan = False
    for event in events:
        event_slug = event.get("slug") or ""
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue

        for m in markets:
            diagnostics["stage_counts"]["markets_seen"] += 1
            settle_ts = _to_unix(m.get("endDate") or event.get("endDate"))
            if settle_ts is None:
                continue
            if settle_ts < start_ts or settle_ts > end_ts:
                continue
            diagnostics["stage_counts"]["markets_in_time_window"] += 1

            outcomes = _parse_json_list(m.get("outcomes"))
            outcome_prices = _parse_json_list(m.get("outcomePrices"))
            token_ids = _parse_json_list(m.get("clobTokenIds"))
            if len(outcomes) != 2 or len(outcome_prices) != 2 or len(token_ids) != 2:
                reasons["not_binary"] += 1
                continue
            diagnostics["stage_counts"]["binary_markets"] += 1

            outcome_idx = _select_outcome_index(outcomes)
            if outcome_idx >= len(token_ids) or outcome_idx >= len(outcome_prices):
                reasons["not_binary"] += 1
                continue

            final_price = _safe_float(outcome_prices[outcome_idx])
            resolved_outcome = _resolved_binary_outcome(final_price)
            if resolved_outcome is None:
                reasons["not_resolved"] += 1
                continue
            diagnostics["stage_counts"]["resolved_binary_markets"] += 1

            token_id = str(token_ids[outcome_idx])
            entry_ts = settle_ts - (entry_hours_before_close * 3600)
            if entry_ts <= 0:
                continue

            if history_requests >= max_history_requests:
                stop_scan = True
                break

            hist_start_ts = entry_ts - (history_window_days * 24 * 3600)
            history_requests += 1
            diagnostics["stage_counts"]["history_requests"] = history_requests
            history = fetch_price_history(token_id, hist_start_ts, entry_ts, fidelity=fidelity)
            if not history:
                reasons["missing_history"] += 1
                continue
            diagnostics["stage_counts"]["history_available"] += 1

            entry_price = price_at_or_before(history, entry_ts)
            if entry_price is None:
                reasons["no_entry_price"] += 1
                continue

            best_bid = _safe_float(m.get("bestBid"))
            best_ask = _safe_float(m.get("bestAsk"))
            spread = None
            if (
                best_bid is not None
                and best_ask is not None
                and 0 <= best_bid <= 1
                and 0 <= best_ask <= 1
                and best_ask >= best_bid
            ):
                spread = best_ask - best_bid
            else:
                best_bid = None
                best_ask = None
                spread = _safe_float(m.get("spread"))
                if spread is not None and (spread < 0 or spread > 1):
                    spread = None

            volume_total = _safe_float(m.get("volume")) or _safe_float(event.get("volume")) or 0.0
            volume_1wk = _safe_float(m.get("volume1wk")) or 0.0
            volume24h_proxy = (volume_1wk / 7.0) if volume_1wk > 0 else (volume_total / 30.0)

            snapshot = {
                "question": m.get("question") or "",
                "slug": m.get("slug") or "",
                "event_slug": event_slug,
                "event_title": event.get("title") or event.get("question") or "",
                "event_description": event.get("description") or m.get("description") or "",
                "event_category": event.get("category") or m.get("category") or "",
                "resolution_source": event.get("resolutionSource") or m.get("resolutionSource") or "",
                "event_market_count": len(markets),
                "outcome_count": len(outcomes),
                "selected_outcome": outcomes[outcome_idx] if outcome_idx < len(outcomes) else "",
                "token_yes": token_id,
                "volume": volume_total,
                "volume24h": volume24h_proxy,
                "liquidity": _safe_float(m.get("liquidity")) or 0.0,
                "ref_price": entry_price,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "last_trade": entry_price,
                "one_hour_change": change_over(history, entry_ts, 3600),
                "one_day_change": change_over(history, entry_ts, 24 * 3600),
                "one_week_change": change_over(history, entry_ts, 7 * 24 * 3600),
                "hours_to_close": float(entry_hours_before_close),
            }

            if spread is not None and best_bid is None and best_ask is None:
                snapshot["best_bid"] = max(0.0, entry_price - spread / 2.0)
                snapshot["best_ask"] = min(1.0, entry_price + spread / 2.0)

            profile = enrich_market_profile(snapshot)
            _bump_stage(
                diagnostics,
                "snapshot_ready",
                market_type=profile["market_type"],
                category_group=profile["category_group"],
            )
            reject_snapshot = dict(rejects)
            if not _passes_backtest_filters(snapshot, rejects, use_liquidity_filter):
                for reason_name, count in rejects.items():
                    if count != reject_snapshot.get(reason_name, 0):
                        diagnostics["rejects_by_market_type"][reason_name][profile["market_type"]] += 1
                        diagnostics["rejects_by_category"][reason_name][profile["category_group"]] += 1
                        break
                continue
            _bump_stage(
                diagnostics,
                "passed_filters",
                market_type=profile["market_type"],
                category_group=profile["category_group"],
            )

            metrics = evaluate_market(snapshot)
            fair = estimated_probability(
                snapshot,
                metrics,
                adjustment_scale=MODEL_ADJUSTMENT_SCALE,
            )
            if fair is None:
                continue
            _bump_stage(
                diagnostics,
                "scored",
                market_type=metrics.get("market_type"),
                category_group=metrics.get("category_group"),
            )

            selected.append(
                Candidate(
                    event_id=str(event.get("id") or ""),
                    question=snapshot["question"],
                    event_slug=event_slug,
                    market_slug=snapshot["slug"],
                    market_type=metrics.get("market_type", "unknown"),
                    category_group=metrics.get("category_group", "other"),
                    token_id=token_id,
                    entry_ts=entry_ts,
                    settle_ts=settle_ts,
                    entry=entry_price,
                    fair=fair,
                    gross_edge=0.0,
                    net_edge=0.0,
                    confidence=metrics.get("confidence", 0.5),
                    resolved_outcome=resolved_outcome,
                    spread=spread,
                )
            )
            if len(selected) >= max_markets:
                break
        if stop_scan:
            break
        if len(selected) >= max_markets:
            break

    for candidate in selected:
        _recompute_candidate_edges(candidate)

    _neutralize_candidates_by_event(selected)

    for candidate in selected:
        _bump_stage(
            diagnostics,
            "post_neutralization",
            market_type=candidate.market_type,
            category_group=candidate.category_group,
        )

    filtered = []
    for candidate in selected:
        if candidate.confidence < MIN_CONFIDENCE:
            rejects["low_confidence"] += 1
            continue
        _bump_stage(
            diagnostics,
            "after_confidence",
            market_type=candidate.market_type,
            category_group=candidate.category_group,
        )
        if candidate.gross_edge < MIN_GROSS_EDGE:
            rejects["low_gross_edge"] += 1
            edge_rejections.append(_build_rejection_payload(candidate, "low_gross_edge"))
            continue
        _bump_stage(
            diagnostics,
            "after_gross_edge",
            market_type=candidate.market_type,
            category_group=candidate.category_group,
        )
        if candidate.net_edge is None or candidate.net_edge <= EDGE_THRESHOLD:
            edge_rejections.append(_build_rejection_payload(candidate, "low_net_edge"))
            continue
        _bump_stage(
            diagnostics,
            "after_net_edge",
            market_type=candidate.market_type,
            category_group=candidate.category_group,
        )
        filtered.append(candidate)

    filtered = _dedupe_per_event(filtered)
    filtered = sorted(filtered, key=lambda x: x.net_edge, reverse=True)[:max_markets]
    for candidate in filtered:
        _bump_stage(
            diagnostics,
            "final_candidates",
            market_type=candidate.market_type,
            category_group=candidate.category_group,
        )

    diagnostics_payload = {
        "stage_counts": dict(sorted(diagnostics["stage_counts"].items())),
        "market_type_stage_counts": _finalize_stage_map(diagnostics["market_type_stage_counts"]),
        "category_stage_counts": _finalize_stage_map(diagnostics["category_stage_counts"]),
        "rejects_by_market_type": _finalize_stage_map(diagnostics["rejects_by_market_type"]),
        "rejects_by_category": _finalize_stage_map(diagnostics["rejects_by_category"]),
        "edge_distributions": {
            "confidence": distribution_stats([candidate.confidence for candidate in selected]),
            "gross_edge": distribution_stats([candidate.gross_edge for candidate in selected]),
            "net_edge": distribution_stats([candidate.net_edge for candidate in selected]),
        },
        "market_type_edge_summary": {},
        "top_rejected_by_edge": _top_edge_rejections(edge_rejections),
    }

    grouped_by_type = defaultdict(list)
    for candidate in selected:
        grouped_by_type[candidate.market_type].append(candidate)
    for market_type, group in grouped_by_type.items():
        diagnostics_payload["market_type_edge_summary"][market_type] = {
            "count": len(group),
            "confidence": distribution_stats([candidate.confidence for candidate in group]),
            "gross_edge": distribution_stats([candidate.gross_edge for candidate in group]),
            "net_edge": distribution_stats([candidate.net_edge for candidate in group]),
        }

    return filtered, rejects, reasons, diagnostics_payload


def run_simulation(candidates, initial_bankroll: float):
    candidates = sorted(candidates, key=lambda x: x.entry_ts)
    open_positions = []
    seq = 0

    cash = initial_bankroll
    equity_peak = initial_bankroll
    max_drawdown = 0.0
    total_trades = 0
    winning_trades = 0
    realized_pnl = 0.0
    skipped_no_cash = 0
    skipped_exposure = 0
    executed = []

    fee_rate = (TAKER_FEE_BPS + ESTIMATED_SLIPPAGE_BPS) / 10000.0

    def settle_up_to(ts):
        nonlocal cash, winning_trades, realized_pnl, equity_peak, max_drawdown
        while open_positions and open_positions[0][0] <= ts:
            _, _, pos = heapq.heappop(open_positions)
            payout = pos.shares * float(pos.resolved_outcome)
            pnl = payout - pos.cost_basis
            realized_pnl += pnl
            if pnl > 0:
                winning_trades += 1
            cash += payout
            if cash > equity_peak:
                equity_peak = cash
            if equity_peak > 0:
                dd = (equity_peak - cash) / equity_peak
                if dd > max_drawdown:
                    max_drawdown = dd

    for c in candidates:
        settle_up_to(c.entry_ts)

        kelly = kelly_bet_fraction(c.fair, c.entry)
        proposed_stake = cash * kelly * KELLY_FRACTION * c.confidence
        proposed_stake = min(MAX_BET_USD, proposed_stake)
        if proposed_stake <= 0:
            continue

        shares = proposed_stake / c.entry
        half_spread = (c.spread / 2.0) if c.spread is not None else 0.0
        spread_cost = shares * half_spread
        fee_cost = proposed_stake * fee_rate
        total_outlay = proposed_stake + spread_cost + fee_cost

        open_cost_basis = sum(position.cost_basis for _, _, position in open_positions)
        exposure_cap = (cash + open_cost_basis) * MAX_TOTAL_EXPOSURE_PCT
        if open_cost_basis + total_outlay > exposure_cap:
            skipped_exposure += 1
            continue

        if total_outlay > cash:
            skipped_no_cash += 1
            continue

        cash -= total_outlay
        pos = OpenPosition(
            question=c.question,
            settle_ts=c.settle_ts,
            shares=shares,
            cost_basis=total_outlay,
            resolved_outcome=c.resolved_outcome,
            entry_price=c.entry,
        )
        seq += 1
        heapq.heappush(open_positions, (c.settle_ts, seq, pos))
        total_trades += 1
        executed.append((c, total_outlay))

    settle_up_to(10**12)

    roi = (cash / initial_bankroll) - 1 if initial_bankroll > 0 else 0.0
    win_rate = (winning_trades / total_trades) if total_trades > 0 else 0.0

    return {
        "final_bankroll": cash,
        "roi": roi,
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "win_rate": win_rate,
        "realized_pnl": realized_pnl,
        "max_drawdown": max_drawdown,
        "skipped_no_cash": skipped_no_cash,
        "skipped_exposure": skipped_exposure,
        "executed": executed,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Backtest polymarket_edge_bot_realprice on closed markets.")
    parser.add_argument("--start-date", default="2026-01-01", help="UTC date, e.g. 2026-01-01")
    parser.add_argument("--end-date", default="2026-03-01", help="UTC date, e.g. 2026-03-01")
    parser.add_argument("--entry-hours-before-close", type=int, default=24)
    parser.add_argument("--history-window-days", type=int, default=8)
    parser.add_argument("--history-fidelity", type=int, default=60)
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--lookback-events", type=int, default=50000)
    parser.add_argument("--max-events-fetch", type=int, default=50000)
    parser.add_argument("--max-candidate-markets", type=int, default=1500)
    parser.add_argument("--max-history-requests", type=int, default=1200)
    parser.add_argument("--initial-bankroll", type=float, default=10.0)
    parser.add_argument("--json-output", default=None, help="Write machine-readable summary to a JSON file.")
    parser.add_argument(
        "--use-liquidity-filter",
        action="store_true",
        help="Enable liquidity filter (usually too strict on closed snapshots).",
    )
    return parser.parse_args()


def _parse_date_to_ts(date_text: str):
    dt = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def main():
    args = parse_args()
    start_ts = _parse_date_to_ts(args.start_date)
    end_ts = _parse_date_to_ts(args.end_date) + (24 * 3600) - 1

    if args.start_offset is None:
        print("Finding total number of closed events...")
        total_closed = find_total_closed_events()
        print(f"Closed events total: {total_closed}")
        start_offset = max(0, total_closed - args.lookback_events)
        max_events = min(args.max_events_fetch, total_closed - start_offset)
    else:
        start_offset = max(0, args.start_offset)
        max_events = args.max_events_fetch

    print(f"Fetching closed events from offset {start_offset} (max {max_events})...")
    events = fetch_closed_events(start_offset=start_offset, max_events=max_events, page_size=args.page_size)
    print(f"Fetched events: {len(events)}")

    print("Building candidates and replaying model signals...")
    candidates, rejects, reasons, diagnostics = build_candidates(
        events=events,
        start_ts=start_ts,
        end_ts=end_ts,
        entry_hours_before_close=args.entry_hours_before_close,
        history_window_days=args.history_window_days,
        max_markets=args.max_candidate_markets,
        fidelity=args.history_fidelity,
        use_liquidity_filter=args.use_liquidity_filter,
        max_history_requests=args.max_history_requests,
    )
    print(f"Candidates passing edge threshold: {len(candidates)}")

    summary = run_simulation(candidates, initial_bankroll=args.initial_bankroll)

    print("\n=== Backtest Summary ===")
    print(f"Period: {args.start_date} .. {args.end_date} (UTC)")
    print(f"Entry timing: {args.entry_hours_before_close}h before market close")
    print(f"Initial bankroll: ${args.initial_bankroll:.2f}")
    print(f"Final bankroll: ${summary['final_bankroll']:.2f}")
    print(f"Realized PnL: ${summary['realized_pnl']:.2f}")
    print(f"ROI: {summary['roi']:.2%}")
    print(f"Trades: {summary['total_trades']}")
    print(f"Wins: {summary['winning_trades']} (win rate {summary['win_rate']:.2%})")
    print(f"Max drawdown (realized-cash): {summary['max_drawdown']:.2%}")
    print(f"Skipped (cash): {summary['skipped_no_cash']}")
    print(f"Skipped (exposure cap): {summary['skipped_exposure']}")

    print("\nReject counters:")
    print(rejects)
    print("Drop reasons before scoring:")
    print(reasons)
    print("Pipeline stage counts:")
    print(diagnostics["stage_counts"])
    rejects_by_type = diagnostics.get("rejects_by_market_type", {})
    if rejects_by_type:
        print("Reject reasons by market type:")
        print(rejects_by_type)

    market_type_final = diagnostics["market_type_stage_counts"].get("final_candidates", {})
    if market_type_final:
        print("Final candidates by market type:")
        print(market_type_final)

    print("Edge distributions after neutralization:")
    print(diagnostics["edge_distributions"])

    top_rejected = diagnostics.get("top_rejected_by_edge", [])
    if top_rejected:
        print("Top rejected by edge:")
        for item in top_rejected[:5]:
            print(
                f"- {item['reject_reason']} | {item['market_type']} | "
                f"net_edge={item['net_edge']:.3f} gross_edge={item['gross_edge']:.3f} "
                f"conf={item['confidence']:.2f}\n"
                f"  {item['question']}\n"
                f"  {item['link']}"
            )

    top = sorted(summary["executed"], key=lambda x: x[0].net_edge, reverse=True)[:5]
    top_payload = []
    if top:
        print("\nTop executed signals by net_edge:")
        for c, outlay in top:
            event_link = f"https://polymarket.com/event/{c.event_slug}?tid={c.token_id}"
            top_payload.append(
                {
                    "question": c.question,
                    "event_slug": c.event_slug,
                    "token_id": c.token_id,
                    "entry_ts": c.entry_ts,
                    "entry_utc": _to_utc_str(c.entry_ts),
                    "entry": c.entry,
                    "fair": c.fair,
                    "gross_edge": c.gross_edge,
                    "net_edge": c.net_edge,
                    "confidence": c.confidence,
                    "market_type": c.market_type,
                    "category_group": c.category_group,
                    "outlay_usd": outlay,
                    "resolved_outcome": c.resolved_outcome,
                    "link": event_link,
                }
            )
            print(
                f"- {_to_utc_str(c.entry_ts)} | edge={c.net_edge:.3f} | "
                f"entry={c.entry:.3f} fair={c.fair:.3f} outlay=${outlay:.2f}\n"
                f"  {c.question}\n  {event_link}"
            )

    if args.json_output:
        payload = {
            "train_or_test_period": {
                "start_date": args.start_date,
                "end_date": args.end_date,
            },
            "parameters": {
                "entry_hours_before_close": args.entry_hours_before_close,
                "history_window_days": args.history_window_days,
                "history_fidelity": args.history_fidelity,
                "start_offset": args.start_offset,
                "page_size": args.page_size,
                "lookback_events": args.lookback_events,
                "max_events_fetch": args.max_events_fetch,
                "max_candidate_markets": args.max_candidate_markets,
                "max_history_requests": args.max_history_requests,
                "initial_bankroll": args.initial_bankroll,
                "use_liquidity_filter": args.use_liquidity_filter,
                "env_overrides": {
                    "MIN_CONFIDENCE": os.getenv("MIN_CONFIDENCE"),
                    "MIN_GROSS_EDGE": os.getenv("MIN_GROSS_EDGE"),
                    "EDGE_THRESHOLD": os.getenv("EDGE_THRESHOLD"),
                    "WATCH_THRESHOLD": os.getenv("WATCH_THRESHOLD"),
                    "MODEL_ADJUSTMENT_SCALE": os.getenv("MODEL_ADJUSTMENT_SCALE"),
                    "MAX_SIGNALS_PER_EVENT": os.getenv("MAX_SIGNALS_PER_EVENT"),
                },
            },
            "summary": {
                "final_bankroll": summary["final_bankroll"],
                "roi": summary["roi"],
                "total_trades": summary["total_trades"],
                "winning_trades": summary["winning_trades"],
                "win_rate": summary["win_rate"],
                "realized_pnl": summary["realized_pnl"],
                "max_drawdown": summary["max_drawdown"],
                "skipped_no_cash": summary["skipped_no_cash"],
                "skipped_exposure": summary["skipped_exposure"],
                "candidate_count": len(candidates),
            },
            "rejects": rejects,
            "drop_reasons": reasons,
            "diagnostics": diagnostics,
            "top_executed": top_payload,
        }
        with open(args.json_output, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=True)


if __name__ == "__main__":
    main()
