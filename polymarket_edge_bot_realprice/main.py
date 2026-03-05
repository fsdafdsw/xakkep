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


def _entry_price(market):
    if market.get("best_ask") is not None:
        return market["best_ask"]
    return market.get("ref_price")


def _market_link(market):
    slug = market.get("event_slug") or market.get("slug")
    if slug:
        token_id = market.get("token_yes")
        if token_id:
            return f"https://polymarket.com/event/{slug}?tid={token_id}"
        return f"https://polymarket.com/event/{slug}"
    return "https://polymarket.com/"


def _passes_filters(market, rejects):
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


def _format_signal(rank, candidate):
    return (
        f"{rank}. {candidate['question']}\n"
        f"{candidate['link']}\n"
        f"entry={candidate['entry']:.3f} fair={candidate['fair']:.3f} "
        f"net_edge={candidate['net_edge']:.3f}\n"
        f"confidence={candidate['confidence']:.2f} stake=${candidate['stake_usd']:.2f}"
    )


def run():
    markets = fetch_markets(SCAN_LIMIT)

    accepted = []
    rejects = {
        "low_liquidity": 0,
        "low_volume": 0,
        "no_price": 0,
        "extreme_price": 0,
        "wide_spread": 0,
        "near_expiry": 0,
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
        fair = estimated_probability(market, metrics)
        entry = _entry_price(market)
        net_edge = net_edge_after_costs(
            fair_probability=fair,
            entry_price=entry,
            taker_fee_bps=TAKER_FEE_BPS,
            slippage_bps=ESTIMATED_SLIPPAGE_BPS,
            spread=market.get("spread"),
        )
        if net_edge is None:
            continue

        kelly = kelly_bet_fraction(fair, entry)
        stake_usd = min(
            MAX_BET_USD,
            BANKROLL_USD * kelly * KELLY_FRACTION * metrics.get("confidence", 0.5),
        )

        accepted.append(
            {
                "market": market,
                "metrics": metrics,
                "fair": fair,
                "entry": entry,
                "net_edge": net_edge,
                "stake_usd": max(stake_usd, 0.0),
            }
        )

    value_bets = []
    watchlist = []
    skipped_by_exposure = 0

    for item in accepted:
        market = item["market"]
        candidate = {
            "question": market.get("question"),
            "link": _market_link(market),
            "entry": item["entry"],
            "fair": item["fair"],
            "net_edge": item["net_edge"],
            "confidence": item["metrics"]["confidence"],
            "stake_usd": item["stake_usd"],
        }

        if item["net_edge"] > EDGE_THRESHOLD:
            value_bets.append(candidate)
        elif item["net_edge"] > WATCH_THRESHOLD:
            watchlist.append(candidate)

    value_bets_sorted = sorted(value_bets, key=lambda x: x["net_edge"], reverse=True)
    exposure_cap = BANKROLL_USD * MAX_TOTAL_EXPOSURE_PCT
    exposure_used = 0.0
    value_bets_limited = []

    for candidate in value_bets_sorted:
        stake = candidate.get("stake_usd", 0.0)
        if stake <= 0:
            continue
        if exposure_used + stake > exposure_cap:
            skipped_by_exposure += 1
            continue
        value_bets_limited.append(candidate)
        exposure_used += stake
        if len(value_bets_limited) >= MAX_SIGNALS:
            break

    value_bets = value_bets_limited
    watchlist = sorted(watchlist, key=lambda x: x["net_edge"], reverse=True)[:MAX_WATCHLIST]

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

Scanned: {len(markets)}
Passed filters: {len(accepted)}
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
bankroll=${BANKROLL_USD:.0f} | kelly_fraction={KELLY_FRACTION:.2f} | max_bet=${MAX_BET_USD:.0f} | max_total_exposure={MAX_TOTAL_EXPOSURE_PCT:.0%}
"""

    send_message(report)


if __name__ == "__main__":
    run()
