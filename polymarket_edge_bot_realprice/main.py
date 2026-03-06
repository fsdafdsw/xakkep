import json
from collections import defaultdict
from datetime import datetime, timezone

from config import *
from event_graph import compute_event_graph_metrics
from filter_policy import (
    filter_reason,
    scoring_policy_for_market,
    signal_bucket,
)
from probability_model import (
    estimated_probability,
    kelly_bet_fraction,
    net_edge_after_costs,
)
from robust_signal import compute_robust_signal
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
    entry = _entry_price(market)
    reason = filter_reason(market, entry_price=entry, use_liquidity_filter=True)
    if reason:
        rejects[reason] += 1
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


def _annotate_event_graph_and_robust_signal(items):
    nodes = []
    for item in items:
        market = item["market"]
        implied = market.get("ref_price")
        if implied is None:
            implied = item["entry"]
        nodes.append(
            {
                "event_key": item["event_key"],
                "implied": implied,
                "fair": item["fair"],
                "market_type": item["metrics"].get("market_type"),
                "event_market_count": market.get("event_market_count"),
            }
        )

    graph_metrics = compute_event_graph_metrics(nodes)
    for item, graph in zip(items, graph_metrics):
        item["graph"] = graph or {}
        robust = compute_robust_signal(
            market=item["market"],
            metrics=item["metrics"],
            fair=item["fair"],
            graph_metrics=item["graph"],
        )
        item["robust"] = robust
        item["fair_lcb"] = robust["fair_lcb"]
        item["gross_edge_lcb"] = robust["gross_edge_lcb"]
        item["net_edge_lcb"] = robust["net_edge_lcb"]


def _baseline_confidence(metrics):
    base_confidence = (
        (metrics.get("quality", 0.5) * 0.45)
        + (metrics.get("orderbook", 0.5) * 0.30)
        + ((1.0 - metrics.get("anomaly", 0.5)) * 0.15)
        + (metrics.get("news", 0.5) * 0.10)
    )
    return max(0.0, min(base_confidence, 1.0))


def _prepare_live_metrics(metrics):
    if LIVE_USE_RESEARCH_GATES:
        return metrics

    return {
        "quality": metrics.get("quality", 0.5),
        "momentum": metrics.get("momentum", 0.5),
        "anomaly": metrics.get("anomaly", 0.5),
        "orderbook": metrics.get("orderbook", 0.5),
        "news": metrics.get("news", 0.5),
        "external": 0.5,
        "external_confidence": 0.5,
        "domain_name": "disabled",
        "domain_signal": 0.5,
        "domain_confidence": 0.5,
        "market_type": metrics.get("market_type"),
        "category_group": metrics.get("category_group"),
        "confidence": _baseline_confidence(metrics),
    }


def _candidate_domain_components(metrics):
    return ((metrics.get("external_components") or {}).get("domain") or {}).get("components", {})


def _build_candidate(item, score_policy):
    graph = item.get("graph") or {}
    robust = item.get("robust") or {}
    domain_components = _candidate_domain_components(item["metrics"])

    fair_lcb = item.get("fair_lcb")
    gross_edge_lcb = item.get("gross_edge_lcb")
    net_edge_lcb = item.get("net_edge_lcb")

    if fair_lcb is None:
        fair_lcb = item["fair"]
    if gross_edge_lcb is None:
        gross_edge_lcb = item["gross_edge"]
    if net_edge_lcb is None:
        net_edge_lcb = item["net_edge"]

    confidence = item["metrics"].get("confidence", 0.5)
    candidate = {
        "event_key": item["event_key"],
        "question": item["market"].get("question"),
        "market_type": item["metrics"].get("market_type") or item["market"].get("market_type"),
        "category_group": item["metrics"].get("category_group") or item["market"].get("category_group"),
        "selected_outcome": item["market"].get("selected_outcome"),
        "selected_outcome_index": item["market"].get("selected_outcome_index"),
        "link": _market_link(item["market"]),
        "entry": item["entry"],
        "fair": item["fair"],
        "fair_lcb": fair_lcb,
        "gross_edge": item["gross_edge"],
        "net_edge": item["net_edge"],
        "gross_edge_lcb": gross_edge_lcb,
        "net_edge_lcb": net_edge_lcb,
        "confidence": confidence,
        "meta_confidence": robust.get("meta_confidence", confidence),
        "graph_consistency": graph.get("consistency", 1.0),
        "robustness_score": robust.get("robustness_score", confidence),
        "domain_name": item["metrics"].get("domain_name"),
        "domain_signal": item["metrics"].get("domain_signal"),
        "domain_confidence": item["metrics"].get("domain_confidence"),
        "odds_implied_probability": domain_components.get("implied_probability"),
        "odds_bookmaker_count": domain_components.get("bookmaker_count"),
        "stake_usd": max(item.get("stake_usd", 0.0), 0.0),
        "model": {
            "quality": item["metrics"].get("quality"),
            "momentum": item["metrics"].get("momentum"),
            "anomaly": item["metrics"].get("anomaly"),
            "orderbook": item["metrics"].get("orderbook"),
            "news": item["metrics"].get("news"),
            "external": item["metrics"].get("external"),
            "external_confidence": item["metrics"].get("external_confidence"),
            "domain_name": item["metrics"].get("domain_name"),
            "domain_signal": item["metrics"].get("domain_signal"),
            "domain_confidence": item["metrics"].get("domain_confidence"),
            "adjustment_multiplier": item["metrics"].get("adjustment_multiplier"),
            "factor_weights": item["metrics"].get("factor_weights"),
            "external_components": item["metrics"].get("external_components"),
            "graph": graph,
            "robust": robust,
        },
        "policy": {
            "min_confidence": score_policy["min_confidence"],
            "min_gross_edge": score_policy["min_gross_edge"],
            "edge_threshold": score_policy["edge_threshold"],
            "watch_threshold": score_policy["watch_threshold"],
            "min_meta_confidence": score_policy["min_meta_confidence"],
            "min_graph_consistency": score_policy["min_graph_consistency"],
            "min_robustness_score": score_policy["min_robustness_score"],
            "min_lcb_edge": score_policy["min_lcb_edge"],
            "watch_lcb_floor": score_policy["watch_lcb_floor"],
        },
    }
    return candidate


def _format_signal(rank, candidate):
    outcome_text = candidate.get("selected_outcome") or "unknown"
    outcome_num = (candidate.get("selected_outcome_index") or 0) + 1
    odds_bits = ""
    if candidate.get("odds_implied_probability") is not None:
        odds_bits = (
            f" odds={candidate['odds_implied_probability']:.3f}"
            f" books={candidate.get('odds_bookmaker_count', 0)}"
        )
    return (
        f"{rank}. {candidate['question']}\n"
        f"BET: BUY {outcome_text} (outcome #{outcome_num})\n"
        f"{candidate['link']}\n"
        f"entry={candidate['entry']:.3f} fair={candidate['fair']:.3f} "
        f"gross_edge={candidate['gross_edge']:.3f} net_edge={candidate['net_edge']:.3f}\n"
        f"confidence={candidate['confidence']:.2f}{odds_bits} "
        f"stake=${candidate['stake_usd']:.2f}"
    )


def _simple_signal_bucket(net_edge, policy):
    if net_edge is None:
        return None
    if net_edge > policy["edge_threshold"]:
        return "value"
    if net_edge > policy["watch_threshold"]:
        return "watch"
    return None


def _rejection_shortfall(reason, candidate):
    policy = candidate["policy"]
    if reason == "low_confidence":
        return max(0.0, policy["min_confidence"] - candidate["confidence"])
    if reason == "low_gross_edge":
        return max(0.0, policy["min_gross_edge"] - candidate["gross_edge"])
    if reason == "low_net_edge":
        return max(0.0, policy["watch_threshold"] - candidate["net_edge"])
    if reason == "low_meta_confidence":
        return max(0.0, policy["min_meta_confidence"] - candidate["meta_confidence"])
    if reason == "low_graph_consistency":
        return max(0.0, policy["min_graph_consistency"] - candidate["graph_consistency"])
    if reason == "low_robustness":
        return max(0.0, policy["min_robustness_score"] - candidate["robustness_score"])
    if reason == "low_lcb_edge":
        return max(0.0, policy["watch_lcb_floor"] - candidate["net_edge_lcb"])
    return 0.0


def _record_rejected(rejected_candidates, candidate, reason):
    rejected = dict(candidate)
    rejected["rejection_reason"] = reason
    rejected["diagnostic_shortfall"] = _rejection_shortfall(reason, candidate)
    rejected_candidates.append(rejected)


def _format_rejected(rank, candidate):
    outcome_text = candidate.get("selected_outcome") or "unknown"
    outcome_num = (candidate.get("selected_outcome_index") or 0) + 1
    return (
        f"{rank}. {candidate['question']}\n"
        f"BET: BUY {outcome_text} (outcome #{outcome_num})\n"
        f"{candidate['link']}\n"
        f"blocked_by={candidate['rejection_reason']} shortfall={candidate['diagnostic_shortfall']:.3f}\n"
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
    rejected_candidates = []
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
        "low_net_edge": 0,
        "low_meta_confidence": 0,
        "low_graph_consistency": 0,
        "low_robustness": 0,
        "low_lcb_edge": 0,
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

        metrics = _prepare_live_metrics(evaluate_market(market))
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

    if LIVE_USE_RESEARCH_GATES:
        # Research mode keeps the stricter event-relative and lower-bound logic.
        _neutralize_by_event(accepted)
        _annotate_event_graph_and_robust_signal(accepted)

    value_bets = []
    watchlist = []
    skipped_by_exposure = 0

    for item in accepted:
        score_policy = scoring_policy_for_market(item["market"])
        candidate = _build_candidate(item, score_policy)
        confidence = item["metrics"].get("confidence", 0.5)
        if confidence < score_policy["min_confidence"]:
            rejects["low_confidence"] += 1
            _record_rejected(rejected_candidates, candidate, "low_confidence")
            continue

        if item["gross_edge"] < score_policy["min_gross_edge"]:
            rejects["low_gross_edge"] += 1
            _record_rejected(rejected_candidates, candidate, "low_gross_edge")
            continue

        if LIVE_USE_RESEARCH_GATES:
            meta_confidence = candidate["meta_confidence"]
            graph_consistency = candidate["graph_consistency"]
            robustness_score = candidate["robustness_score"]
            net_edge_lcb = candidate["net_edge_lcb"]

            if meta_confidence < score_policy["min_meta_confidence"]:
                rejects["low_meta_confidence"] += 1
                _record_rejected(rejected_candidates, candidate, "low_meta_confidence")
                continue

            if graph_consistency < score_policy["min_graph_consistency"]:
                rejects["low_graph_consistency"] += 1
                _record_rejected(rejected_candidates, candidate, "low_graph_consistency")
                continue

            if robustness_score < score_policy["min_robustness_score"]:
                rejects["low_robustness"] += 1
                _record_rejected(rejected_candidates, candidate, "low_robustness")
                continue

            kelly_probability = candidate["fair_lcb"]
            size_multiplier = min(confidence, robustness_score)
        else:
            net_edge_lcb = candidate["net_edge_lcb"]
            kelly_probability = candidate["fair"]
            size_multiplier = confidence

        stake_usd = min(
            MAX_BET_USD,
            BANKROLL_USD * kelly_bet_fraction(kelly_probability, item["entry"]) * KELLY_FRACTION * size_multiplier,
        )
        candidate["stake_usd"] = max(stake_usd, 0.0)

        if LIVE_USE_RESEARCH_GATES:
            bucket = signal_bucket(item["net_edge"], score_policy, net_edge_lcb=net_edge_lcb)
        else:
            bucket = _simple_signal_bucket(item["net_edge"], score_policy)
        if bucket == "value":
            value_bets.append(candidate)
        elif bucket == "watch":
            watchlist.append(candidate)
        else:
            if LIVE_USE_RESEARCH_GATES and (net_edge_lcb is None or net_edge_lcb <= score_policy["min_lcb_edge"]):
                rejects["low_lcb_edge"] += 1
                _record_rejected(rejected_candidates, candidate, "low_lcb_edge")
            else:
                rejects["low_net_edge"] += 1
                _record_rejected(rejected_candidates, candidate, "low_net_edge")

    value_bets_sorted = sorted(
        value_bets,
        key=lambda x: (x["net_edge_lcb"], x["robustness_score"], x["net_edge"]),
        reverse=True,
    )
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

    watchlist_sorted = sorted(
        watchlist,
        key=lambda x: (x["net_edge_lcb"], x["robustness_score"], x["net_edge"]),
        reverse=True,
    )
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

    diagnostic_candidates = sorted(
        rejected_candidates,
        key=lambda x: (
            x["diagnostic_shortfall"],
            -x["net_edge"],
            -x["gross_edge"],
            -x["confidence"],
        ),
    )[:MAX_DIAGNOSTIC_CANDIDATES]

    signals = (
        "\n\n".join(_format_signal(i + 1, v) for i, v in enumerate(value_bets))
        if value_bets
        else "none"
    )
    near_candidates = watchlist if watchlist else diagnostic_candidates
    near_title = "Near misses" if watchlist else "Near misses / diagnostics"
    near_formatter = _format_signal if watchlist else _format_rejected
    near = "\n\n".join(near_formatter(i + 1, w) for i, w in enumerate(near_candidates)) if near_candidates else "none"

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    live_mode = "research-gated" if LIVE_USE_RESEARCH_GATES else "baseline"
    report = f"""Polymarket edge scan - {utc_now}
build={BOT_BUILD_ID} | format=v3
source={BOT_SOURCE}
mode={live_mode}

Scanned: {len(markets)}
Passed base filters: {len(accepted)}
Price coverage: {coverage['price_available']}/{len(markets)}
Orderbook coverage: {coverage['book_available']}/{len(markets)}

Top value bets

{signals}

{near_title}

{near}

Reject reasons:
{rejects}
Skipped by exposure cap: {skipped_by_exposure}

Risk params:
bankroll=${BANKROLL_USD:.0f} | kelly_fraction={KELLY_FRACTION:.2f} | max_bet=${MAX_BET_USD:.0f} | max_total_exposure={MAX_TOTAL_EXPOSURE_PCT:.0%} | max_signals_per_event={MAX_SIGNALS_PER_EVENT}
gates: confidence>={MIN_CONFIDENCE:.2f} | gross_edge>={MIN_GROSS_EDGE:.3f} | edge>{EDGE_THRESHOLD:.3f} | watch>{WATCH_THRESHOLD:.3f}
"""

    report_payload = {
        "generated_at_utc": utc_now,
        "build": BOT_BUILD_ID,
        "source": BOT_SOURCE,
        "mode": live_mode,
        "scanned": len(markets),
        "passed_base_filters": len(accepted),
        "price_coverage": coverage["price_available"],
        "orderbook_coverage": coverage["book_available"],
        "value_bets": value_bets,
        "near_misses": near_candidates,
        "diagnostic_candidates": diagnostic_candidates,
        "rejects": rejects,
        "skipped_by_exposure_cap": skipped_by_exposure,
        "risk_params": {
            "bankroll_usd": BANKROLL_USD,
            "kelly_fraction": KELLY_FRACTION,
            "max_bet_usd": MAX_BET_USD,
            "max_total_exposure_pct": MAX_TOTAL_EXPOSURE_PCT,
            "max_signals_per_event": MAX_SIGNALS_PER_EVENT,
            "live_use_research_gates": LIVE_USE_RESEARCH_GATES,
            "max_diagnostic_candidates": MAX_DIAGNOSTIC_CANDIDATES,
        },
        "report_text": report,
    }
    _write_report_artifacts(report_payload)
    send_message(report)


if __name__ == "__main__":
    run()
