import json
from collections import defaultdict
from datetime import datetime, timezone

from config import *
from probability_model import (
    estimated_probability,
    kelly_bet_fraction,
    net_edge_after_costs,
)
from scanner import fetch_markets
from strategy import evaluate_market
from telegram import send_message


def _clamp(value, low=0.01, high=0.99):
    return max(low, min(high, value))


def _entry_price(market):
    if market.get("best_ask") is not None:
        return market["best_ask"]
    return market.get("ref_price")


def _event_key(market):
    return market.get("event_id") or market.get("event_slug") or market.get("id")


def _market_link(market):
    slug = market.get("event_slug") or market.get("slug")
    if slug:
        token_id = market.get("selected_token_id") or market.get("token_yes")
        if token_id:
            return f"https://polymarket.com/event/{slug}?tid={token_id}"
        return f"https://polymarket.com/event/{slug}"
    return "https://polymarket.com/"


def _passes_filters(market, rejects):
    question = str(market.get("question") or "").lower()
    for pattern in EXCLUDED_QUESTION_PATTERNS:
        if pattern and pattern in question:
            rejects["excluded_pattern"] += 1
            return False

    volume_ref = market.get("volume24h", 0.0) or market.get("volume", 0.0)
    entry = _entry_price(market)

    if market.get("liquidity", 0.0) < MIN_LIQUIDITY:
        rejects["low_liquidity"] += 1
        return False

    if volume_ref < MIN_VOLUME:
        rejects["low_volume"] += 1
        return False

    if market.get("ref_price") is None:
        rejects["no_price"] += 1
        return False

    if REQUIRE_ORDERBOOK and (
        market.get("best_bid") is None or market.get("best_ask") is None
    ):
        rejects["no_orderbook"] += 1
        return False

    if entry is None or entry < MIN_PRICE or entry > MAX_PRICE:
        rejects["extreme_price"] += 1
        return False

    spread = market.get("spread")
    if spread is not None and spread > MAX_SPREAD:
        rejects["wide_spread"] += 1
        return False

    hours_to_close = market.get("hours_to_close")
    if hours_to_close is not None and hours_to_close < MIN_HOURS_TO_CLOSE:
        rejects["near_expiry"] += 1
        return False

    return True


def _recompute_trade_fields(item):
    fair = item["fair"]
    entry = item["entry"]
    market = item["market"]
    item["gross_edge"] = fair - entry
    item["net_edge"] = net_edge_after_costs(
        fair_probability=fair,
        entry_price=entry,
        taker_fee_bps=TAKER_FEE_BPS,
        slippage_bps=ESTIMATED_SLIPPAGE_BPS,
        spread=market.get("spread"),
    )
    if item["net_edge"] is None:
        item["net_edge"] = -999.0

    kelly = kelly_bet_fraction(fair, entry)
    item["stake_usd"] = min(
        MAX_BET_USD,
        BANKROLL_USD * kelly * KELLY_FRACTION * item["metrics"].get("confidence", 0.5),
    )


def _neutralize_by_event(items):
    grouped = defaultdict(list)
    for idx, item in enumerate(items):
        grouped[item["event_key"]].append(idx)

    for indices in grouped.values():
        if len(indices) <= 1:
            continue

        deltas = []
        for i in indices:
            market = items[i]["market"]
            implied = market.get("ref_price")
            if implied is None:
                implied = items[i]["entry"]
            deltas.append(items[i]["fair"] - implied)

        mean_delta = sum(deltas) / len(deltas)
        for i in indices:
            market = items[i]["market"]
            implied = market.get("ref_price")
            if implied is None:
                implied = items[i]["entry"]

            centered_delta = (items[i]["fair"] - implied) - mean_delta
            adjusted_fair = implied + (centered_delta * EVENT_NEUTRALIZATION_STRENGTH)
            items[i]["fair"] = _clamp(adjusted_fair)
            _recompute_trade_fields(items[i])


def _format_signal(rank, candidate):
    outcome_text = candidate.get("selected_outcome") or "unknown"
    outcome_num = (candidate.get("selected_outcome_index") or 0) + 1
    return (
        f"{rank}. {candidate['question']}\n"
        f"BET: BUY {outcome_text} (outcome #{outcome_num})\n"
        f"{candidate['link']}\n"
        f"entry={candidate['entry']:.3f} fair={candidate['fair']:.3f} "
        f"gross_edge={candidate['gross_edge']:.3f} net_edge={candidate['net_edge']:.3f}\n"
        f"confidence={candidate['confidence']:.2f} stake=${candidate['stake_usd']:.2f}"
    )


def _write_report_artifacts(report_payload):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_path = REPORTS_DIR / f"scan_{ts}.json"
    latest_path = REPORTS_DIR / "latest_scan.json"

    with run_path.open("w", encoding="utf-8") as fh:
        json.dump(report_payload, fh, indent=2, ensure_ascii=True)
    with latest_path.open("w", encoding="utf-8") as fh:
        json.dump(report_payload, fh, indent=2, ensure_ascii=True)


def run():
    markets = fetch_markets(SCAN_LIMIT)

    accepted = []
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
    coverage = {
        "price_available": 0,
        "book_available": 0,
    }

    for market in markets:
        if market.get("ref_price") is not None:
            coverage["price_available"] += 1
        if market.get("best_bid") is not None and market.get("best_ask") is not None:
            coverage["book_available"] += 1

        if not _passes_filters(market, rejects):
            continue

        metrics = evaluate_market(market)
        entry = _entry_price(market)
        fair = estimated_probability(
            market,
            metrics,
            adjustment_scale=MODEL_ADJUSTMENT_SCALE,
        )
        if fair is None:
            continue

        item = {
            "event_key": _event_key(market),
            "market": market,
            "metrics": metrics,
            "fair": fair,
            "entry": entry,
        }
        _recompute_trade_fields(item)
        accepted.append(item)

    # Event-level neutralization reduces false positives across mutually exclusive outcomes.
    _neutralize_by_event(accepted)

    value_bets = []
    watchlist = []
    skipped_by_exposure = 0

    for item in accepted:
        confidence = item["metrics"].get("confidence", 0.5)
        if confidence < MIN_CONFIDENCE:
            rejects["low_confidence"] += 1
            continue

        if item["gross_edge"] < MIN_GROSS_EDGE:
            rejects["low_gross_edge"] += 1
            continue

        candidate = {
            "event_key": item["event_key"],
            "question": item["market"].get("question"),
            "market_type": item["metrics"].get("market_type"),
            "category_group": item["metrics"].get("category_group"),
            "selected_outcome": item["market"].get("selected_outcome"),
            "selected_outcome_index": item["market"].get("selected_outcome_index"),
            "link": _market_link(item["market"]),
            "entry": item["entry"],
            "fair": item["fair"],
            "gross_edge": item["gross_edge"],
            "net_edge": item["net_edge"],
            "confidence": confidence,
            "stake_usd": max(item["stake_usd"], 0.0),
            "model": {
                "quality": item["metrics"].get("quality"),
                "momentum": item["metrics"].get("momentum"),
                "anomaly": item["metrics"].get("anomaly"),
                "orderbook": item["metrics"].get("orderbook"),
                "news": item["metrics"].get("news"),
                "external": item["metrics"].get("external"),
                "external_confidence": item["metrics"].get("external_confidence"),
                "adjustment_multiplier": item["metrics"].get("adjustment_multiplier"),
                "factor_weights": item["metrics"].get("factor_weights"),
                "external_components": item["metrics"].get("external_components"),
            },
        }

        if item["net_edge"] > EDGE_THRESHOLD:
            value_bets.append(candidate)
        elif item["net_edge"] > WATCH_THRESHOLD:
            watchlist.append(candidate)

    value_bets_sorted = sorted(value_bets, key=lambda x: x["net_edge"], reverse=True)
    exposure_cap = BANKROLL_USD * MAX_TOTAL_EXPOSURE_PCT
    exposure_used = 0.0
    event_usage = defaultdict(int)
    value_bets_limited = []

    for candidate in value_bets_sorted:
        stake = candidate.get("stake_usd", 0.0)
        if stake <= 0:
            continue

        if event_usage[candidate["event_key"]] >= MAX_SIGNALS_PER_EVENT:
            continue

        if exposure_used + stake > exposure_cap:
            skipped_by_exposure += 1
            continue

        value_bets_limited.append(candidate)
        event_usage[candidate["event_key"]] += 1
        exposure_used += stake
        if len(value_bets_limited) >= MAX_SIGNALS:
            break

    watchlist_sorted = sorted(watchlist, key=lambda x: x["net_edge"], reverse=True)
    watch_event_usage = defaultdict(int)
    watchlist_limited = []
    for candidate in watchlist_sorted:
        if watch_event_usage[candidate["event_key"]] >= MAX_SIGNALS_PER_EVENT:
            continue
        watch_event_usage[candidate["event_key"]] += 1
        watchlist_limited.append(candidate)
        if len(watchlist_limited) >= MAX_WATCHLIST:
            break

    value_bets = value_bets_limited
    watchlist = watchlist_limited

    signals = (
        "\n\n".join(_format_signal(i + 1, v) for i, v in enumerate(value_bets))
        if value_bets
        else "none"
    )
    near = (
        "\n\n".join(_format_signal(i + 1, w) for i, w in enumerate(watchlist))
        if watchlist
        else "none"
    )

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    report = f"""Polymarket edge scan - {utc_now}
build={BOT_BUILD_ID} | format=v3
source={BOT_SOURCE}

Scanned: {len(markets)}
Passed base filters: {len(accepted)}
Price coverage: {coverage['price_available']}/{len(markets)}
Orderbook coverage: {coverage['book_available']}/{len(markets)}

Top value bets (net edge after costs)

{signals}

Near misses

{near}

Reject reasons:
{rejects}
Skipped by exposure cap: {skipped_by_exposure}

Risk params:
bankroll=${BANKROLL_USD:.0f} | kelly_fraction={KELLY_FRACTION:.2f} | max_bet=${MAX_BET_USD:.0f} | max_total_exposure={MAX_TOTAL_EXPOSURE_PCT:.0%} | max_signals_per_event={MAX_SIGNALS_PER_EVENT}
"""

    report_payload = {
        "generated_at_utc": utc_now,
        "build": BOT_BUILD_ID,
        "source": BOT_SOURCE,
        "scanned": len(markets),
        "passed_base_filters": len(accepted),
        "price_coverage": coverage["price_available"],
        "orderbook_coverage": coverage["book_available"],
        "value_bets": value_bets,
        "near_misses": watchlist,
        "rejects": rejects,
        "skipped_by_exposure_cap": skipped_by_exposure,
        "risk_params": {
            "bankroll_usd": BANKROLL_USD,
            "kelly_fraction": KELLY_FRACTION,
            "max_bet_usd": MAX_BET_USD,
            "max_total_exposure_pct": MAX_TOTAL_EXPOSURE_PCT,
            "max_signals_per_event": MAX_SIGNALS_PER_EVENT,
        },
        "report_text": report,
    }
    _write_report_artifacts(report_payload)
    send_message(report)


if __name__ == "__main__":
    run()
