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
from exit_policy import live_exit_plan
from meta_model import build_meta_feature_row, load_meta_model, score_meta_row
from meeting_subtype import meeting_subtype_label
from probability_model import (
    estimated_probability,
    kelly_bet_fraction,
    net_edge_after_costs,
)
from paper_trading import run_paper_cycle
from repricing_selector import score_repricing_signal
from robust_signal import compute_robust_signal
from report_sections import build_report_sections
from scanner import fetch_markets
from strategy import evaluate_market
from telegram import send_message


_META_MODEL_CACHE = {}


def _clamp(value, low=0.01, high=0.99):
    return max(low, min(high, value))


def _entry_price(market):
    if market.get("best_ask") is not None:
        return market["best_ask"]
    return market.get("ref_price")


def _event_key(market):
    return market.get("event_id") or market.get("event_slug") or market.get("id")


def _market_key(market):
    slug = market.get("event_slug") or market.get("slug") or market.get("id") or "unknown"
    token = market.get("selected_token_id") or market.get("token_yes") or ""
    return f"{slug}|{token}"


def _market_link(market):
    slug = market.get("event_slug") or market.get("slug")
    if slug:
        token_id = market.get("selected_token_id") or market.get("token_yes")
        if token_id:
            return f"https://polymarket.com/event/{slug}?tid={token_id}"
        return f"https://polymarket.com/event/{slug}"
    return "https://polymarket.com/"


def _get_meta_model_artifact():
    path = (META_MODEL_ARTIFACT_PATH or "").strip()
    if not path:
        return None
    cached = _META_MODEL_CACHE.get(path)
    if cached is None:
        cached = load_meta_model(path)
        _META_MODEL_CACHE[path] = cached
    return cached


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
        (metrics.get("quality", 0.5) * 0.40)
        + (metrics.get("orderbook", 0.5) * 0.26)
        + ((1.0 - metrics.get("anomaly", 0.5)) * 0.14)
        + (metrics.get("news", 0.5) * 0.08)
        + (metrics.get("volume_confirmation", 0.5) * 0.12)
    )
    return max(0.0, min(base_confidence, 1.0))


def _prepare_live_metrics(metrics):
    if LIVE_USE_RESEARCH_GATES:
        return metrics

    return {
        "quality": metrics.get("quality", 0.5),
        "momentum": metrics.get("momentum", 0.5),
        "anomaly": metrics.get("anomaly", 0.5),
        "volume_anomaly": metrics.get("volume_anomaly", 0.0),
        "volume_confirmation": metrics.get("volume_confirmation", 0.5),
        "volume_pressure": metrics.get("volume_pressure", 0.5),
        "orderbook": metrics.get("orderbook", 0.5),
        "news": metrics.get("news", 0.5),
        "external": metrics.get("external", 0.5),
        "external_confidence": metrics.get("external_confidence", 0.5),
        "domain_name": metrics.get("domain_name"),
        "domain_signal": metrics.get("domain_signal", 0.5),
        "domain_confidence": metrics.get("domain_confidence", 0.5),
        "market_type": metrics.get("market_type"),
        "category_group": metrics.get("category_group"),
        "adjustment_multiplier": metrics.get("adjustment_multiplier", 1.0),
        "factor_weights": metrics.get("factor_weights"),
        "external_components": metrics.get("external_components"),
        "confidence": _baseline_confidence(metrics),
    }


def _candidate_domain_components(metrics):
    return ((metrics.get("external_components") or {}).get("domain") or {}).get("components", {})


def _repricing_size_multiplier(candidate):
    verdict = str(candidate.get("repricing_verdict") or "")
    if verdict != "buy_now":
        return 1.0

    lane_prior = candidate.get("repricing_lane_prior")
    try:
        lane_prior = float(lane_prior)
    except (TypeError, ValueError):
        lane_prior = 0.50

    multiplier = REPRICING_BUY_BASE_SIZE_MULTIPLIER + (lane_prior * REPRICING_BUY_PRIOR_SIZE_WEIGHT)
    lane_key = str(candidate.get("repricing_lane_key") or "")
    if lane_key == "conflict_fast":
        multiplier += CONFLICT_BUY_SIZE_BONUS
    elif lane_key == "release_hearing":
        multiplier += RELEASE_HEARING_BUY_SIZE_BONUS

    return max(0.50, min(REPRICING_BUY_MAX_SIZE_MULTIPLIER, multiplier))


def _build_candidate(item, score_policy):
    graph = item.get("graph") or {}
    robust = item.get("robust") or {}
    domain_components = _candidate_domain_components(item["metrics"])
    relation_metrics = item["market"].get("relation_metrics") or {}
    relation_residual = item["market"].get("relation_residual") or {}
    resolution_metadata = item["market"].get("resolution_metadata") or {}

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
    outcomes = item["market"].get("outcomes") or []
    candidate = {
        "event_key": item["event_key"],
        "market_id": item["market"].get("id"),
        "event_slug": item["market"].get("event_slug"),
        "selected_token_id": item["market"].get("selected_token_id"),
        "market_key": _market_key(item["market"]),
        "question": item["market"].get("question"),
        "event_title": item["market"].get("event_title"),
        "primary_entity_key": item["market"].get("primary_entity_key"),
        "market_type": item["metrics"].get("market_type") or item["market"].get("market_type"),
        "category_group": item["metrics"].get("category_group") or item["market"].get("category_group"),
        "outcomes": outcomes,
        "selected_outcome": item["market"].get("selected_outcome"),
        "selected_outcome_index": item["market"].get("selected_outcome_index"),
        "link": _market_link(item["market"]),
        "entry": item["entry"],
        "spread": item["market"].get("spread"),
        "cost_per_share": (item["entry"] * ((TAKER_FEE_BPS + ESTIMATED_SLIPPAGE_BPS) / 10000.0))
        + (((item["market"].get("spread") or 0.0) / 2.0) if item["market"].get("spread") is not None else 0.0),
        "fair": item["fair"],
        "fair_lcb": fair_lcb,
        "gross_edge": item["gross_edge"],
        "net_edge": item["net_edge"],
        "gross_edge_lcb": gross_edge_lcb,
        "net_edge_lcb": net_edge_lcb,
        "confidence": confidence,
        "meta_confidence": robust.get("meta_confidence", confidence),
        "meta_trade_prob": None,
        "meta_trade_score": None,
        "graph_consistency": graph.get("consistency", 1.0),
        "robustness_score": robust.get("robustness_score", confidence),
        "domain_name": item["metrics"].get("domain_name"),
        "domain_signal": item["metrics"].get("domain_signal"),
        "domain_confidence": item["metrics"].get("domain_confidence"),
        "domain_action_family": domain_components.get("action_family"),
        "repricing_potential": domain_components.get("repricing_potential"),
        "repricing_score": None,
        "repricing_watch_score": None,
        "repricing_verdict": None,
        "repricing_reason": None,
        "repricing_attention_gap": None,
        "repricing_stale_score": None,
        "repricing_already_priced_penalty": None,
        "repricing_underreaction_score": None,
        "repricing_fresh_catalyst_score": None,
        "repricing_trend_chase_penalty": None,
        "repricing_optionality_score": None,
        "repricing_conflict_setup_score": None,
        "repricing_conflict_urgency_score": None,
        "repricing_release_subject_score": None,
        "repricing_release_legitimacy_score": None,
        "repricing_recent_runup": None,
        "repricing_lane_key": None,
        "repricing_lane_label": None,
        "repricing_lane_prior": None,
        "repricing_size_multiplier": 1.0,
        "catalyst_type": domain_components.get("catalyst_type"),
        "catalyst_strength": domain_components.get("catalyst_strength"),
        "odds_implied_probability": domain_components.get("implied_probability"),
        "odds_bookmaker_count": domain_components.get("bookmaker_count"),
        "relation_degree": relation_metrics.get("relation_degree", 0),
        "exclusive_degree": relation_metrics.get("exclusive_degree", 0),
        "monotonic_degree": relation_metrics.get("monotonic_degree", 0),
        "relation_confidence": relation_metrics.get("relation_confidence", 0.0),
        "relation_support_price": relation_residual.get("support_price"),
        "relation_residual": relation_residual.get("residual", 0.0),
        "relation_support_confidence": relation_residual.get("support_confidence", 0.0),
        "relation_inconsistency": relation_residual.get("inconsistency_score", 0.0),
        "semantic_family": resolution_metadata.get("family"),
        "semantic_confidence": resolution_metadata.get("confidence", 0.0),
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
            "relation_metrics": relation_metrics,
            "relation_residual": relation_residual,
            "resolution_metadata": resolution_metadata,
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
    artifact = _get_meta_model_artifact()
    if artifact:
        prediction = score_meta_row(build_meta_feature_row(candidate), artifact)
        candidate["meta_trade_prob"] = prediction["probability"]
        candidate["meta_trade_score"] = prediction["trade_score"]
        candidate["model"]["meta_model"] = prediction

    repricing = score_repricing_signal(
        entry_price=item["entry"],
        confidence=confidence,
        net_edge=item["net_edge"],
        net_edge_lcb=net_edge_lcb,
        spread=item["market"].get("spread"),
        liquidity=item["market"].get("liquidity"),
        volume24h=item["market"].get("volume24h"),
        one_hour_change=item["market"].get("one_hour_change"),
        one_day_change=item["market"].get("one_day_change"),
        one_week_change=item["market"].get("one_week_change"),
        hours_to_close=item["market"].get("hours_to_close"),
        volume_anomaly=candidate["model"].get("volume_anomaly"),
        volume_confirmation=candidate["model"].get("volume_confirmation"),
        model=candidate["model"],
        market_type=candidate["market_type"],
        category_group=candidate["category_group"],
        question=candidate.get("question"),
    )
    candidate["model"]["repricing"] = repricing
    candidate["repricing_score"] = repricing.get("score")
    candidate["repricing_watch_score"] = repricing.get("watch_score")
    candidate["repricing_verdict"] = repricing.get("verdict")
    candidate["repricing_reason"] = repricing.get("reason")
    candidate["repricing_attention_gap"] = repricing.get("attention_gap")
    candidate["meeting_subtype"] = repricing.get("meeting_subtype")
    candidate["repricing_stale_score"] = repricing.get("stale_score")
    candidate["repricing_already_priced_penalty"] = repricing.get("already_priced_penalty")
    candidate["repricing_underreaction_score"] = repricing.get("underreaction_score")
    candidate["repricing_fresh_catalyst_score"] = repricing.get("fresh_catalyst_score")
    candidate["repricing_trend_chase_penalty"] = repricing.get("trend_chase_penalty")
    candidate["repricing_optionality_score"] = repricing.get("optionality_score")
    candidate["repricing_conflict_setup_score"] = repricing.get("conflict_setup_score")
    candidate["repricing_conflict_urgency_score"] = repricing.get("conflict_urgency_score")
    candidate["repricing_release_subject_score"] = repricing.get("release_subject_score")
    candidate["repricing_release_legitimacy_score"] = repricing.get("release_legitimacy_score")
    candidate["repricing_recent_runup"] = repricing.get("recent_runup")
    candidate["repricing_lane_key"] = repricing.get("lane_key")
    candidate["repricing_lane_label"] = repricing.get("lane_label")
    candidate["repricing_lane_prior"] = repricing.get("lane_prior")
    candidate["repricing_size_multiplier"] = _repricing_size_multiplier(candidate)
    candidate["catalyst_type"] = repricing.get("catalyst_type") or candidate.get("catalyst_type")
    candidate["catalyst_strength"] = repricing.get("catalyst_strength") or candidate.get("catalyst_strength")
    return candidate


def _display_outcomes(candidate):
    outcomes = [str(outcome).strip() for outcome in (candidate.get("outcomes") or []) if str(outcome).strip()]
    if not outcomes:
        return None
    if len(outcomes) > 6:
        return None
    return " | ".join(outcomes)


def _header_lines(rank, candidate):
    market_label = candidate.get("event_title") or candidate.get("question")
    recommendation = candidate.get("selected_outcome") or f"Outcome #{(candidate.get('selected_outcome_index') or 0) + 1}"
    lines = [
        f"{rank}. {market_label}",
        f"Recommendation: BUY '{recommendation}' (outcome #{(candidate.get('selected_outcome_index') or 0) + 1})",
    ]
    if candidate.get("event_title") and candidate.get("event_title") != candidate.get("question"):
        lines.append(f"Market: {candidate['question']}")
    available_outcomes = _display_outcomes(candidate)
    if available_outcomes:
        lines.append(f"Outcomes: {available_outcomes}")
    lines.append(f"Link: {candidate['link']}")
    return lines


def _suggested_exit_line(candidate):
    if str(candidate.get("repricing_verdict") or "") != "buy_now":
        return None
    entry_price = candidate.get("entry")
    if entry_price is None:
        return None
    plan = live_exit_plan(
        candidate.get("domain_action_family"),
        repricing_verdict=candidate.get("repricing_verdict"),
        entry_price=entry_price,
        catalyst_type=candidate.get("catalyst_type"),
    )
    take_profit = plan.get("take_profit_price")
    stop_loss = plan.get("stop_loss_price")
    time_stop_days = plan.get("time_stop_days")
    if take_profit is None or stop_loss is None or time_stop_days is None:
        return None
    return (
        f"Suggested exit: take profit near {take_profit:.3f} | "
        f"stop near {stop_loss:.3f} | time stop {int(round(time_stop_days))}d"
    )


def _format_signal(rank, candidate):
    odds_bits = ""
    if candidate.get("odds_implied_probability") is not None:
        odds_bits = f" | odds={candidate['odds_implied_probability']:.3f} | books={candidate.get('odds_bookmaker_count', 0)}"
    meta_bits = ""
    if candidate.get("meta_trade_prob") is not None:
        meta_bits = f" | meta_p={candidate['meta_trade_prob']:.2f}"
    theme_bits = f"Theme {candidate.get('domain_name') or 'neutral'}"
    if candidate.get("domain_action_family"):
        theme_bits += f" | action={candidate['domain_action_family']}"
    if candidate.get("repricing_potential") is not None:
        theme_bits += f" | repricing={candidate['repricing_potential']:.2f}"
    lines = _header_lines(rank, candidate)
    lines.append(
        f"Entry {candidate['entry']:.3f} | Fair {candidate['fair']:.3f} | Gross edge {candidate['gross_edge']:.3f} | Net edge {candidate['net_edge']:.3f}"
    )
    relation_bits = ""
    if candidate.get("relation_degree"):
        relation_bits = f" | relations={candidate['relation_degree']}"
    lines.append(theme_bits)
    lines.append(
        f"Confidence {candidate['confidence']:.2f} | Stake ${candidate['stake_usd']:.2f}{meta_bits}{odds_bits}{relation_bits}"
    )
    exit_line = _suggested_exit_line(candidate)
    if exit_line:
        lines.append(exit_line)
    return "\n".join(lines)


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
    if reason == "low_meta_model_prob":
        return max(0.0, MIN_META_TRADE_PROB - (candidate.get("meta_trade_prob") or 0.0))
    return 0.0


def _record_rejected(rejected_candidates, candidate, reason):
    rejected = dict(candidate)
    rejected["rejection_reason"] = reason
    rejected["diagnostic_shortfall"] = _rejection_shortfall(reason, candidate)
    rejected_candidates.append(rejected)


def _format_rejected(rank, candidate):
    lines = _header_lines(rank, candidate)
    lines.append("Verdict: DO NOT BUY")
    lines.append(f"Why: {_radar_reason(candidate).capitalize()}")
    lines.append(f"Price now: {candidate['entry']:.3f}")
    return "\n".join(lines)


def _radar_verdict(candidate):
    verdict = str(candidate.get("repricing_verdict") or "")
    if verdict == "buy_now":
        return "BUY NOW"
    if verdict == "watch":
        return "WATCH, NO BUY YET"
    if verdict == "watch_high_upside":
        return "WATCH CLOSELY, HIGH UPSIDE"
    if verdict == "watch_late":
        return "WATCH, MAY BE LATE"
    source = str(candidate.get("radar_source") or "")
    if source == "value":
        return "BUY NOW"
    if source == "watch":
        return "WATCH, NO BUY YET"
    return "DO NOT BUY"


def _radar_reason(candidate):
    repricing_reason = candidate.get("repricing_reason")
    action_family = str(candidate.get("domain_action_family") or "")
    catalyst_type = str(candidate.get("catalyst_type") or "")
    verdict = str(candidate.get("repricing_verdict") or "")

    if action_family == "release" and catalyst_type == "hostage_release":
        if verdict == "watch":
            return "possible hostage-release setup, but it still needs confirmation"
        if verdict == "watch_high_upside":
            return "hostage-release idea has upside, but confirmation is still missing"
        if verdict == "watch_late":
            return "hostage-release setup looks real, but part of the move may already be gone"
        if verdict == "buy_now":
            return "hostage-release setup looks credible and still has room to move"

    if action_family == "diplomacy":
        if catalyst_type == "ceasefire":
            if verdict == "watch_late":
                return "ceasefire talks matter here, but the market may already have reacted"
            return "ceasefire talks matter here, but confirmation is still missing"
        if catalyst_type in {"negotiation", "call_or_meeting", "summit"}:
            meeting_subtype = str(candidate.get("meeting_subtype") or "")
            if catalyst_type == "call_or_meeting" and meeting_subtype == "talk_call":
                if verdict == "watch_high_upside":
                    return "high-level contact setup has strong upside if the call is confirmed"
                if verdict == "watch_late":
                    return "high-level contact setup is live, but much of the move may already be gone"
                if verdict == "watch":
                    return "possible high-level call setup, but it still needs confirmation"
            if catalyst_type == "call_or_meeting" and meeting_subtype in {"meeting", "meeting_generic"}:
                if verdict == "watch_high_upside":
                    return "possible meeting setup has upside, but the meeting still needs firmer confirmation"
                if verdict == "watch_late":
                    return "meeting setup is live, but most of the repricing may already be behind it"
                if verdict == "watch":
                    return "possible meeting setup is forming, but it is still early and not confirmed"
            if catalyst_type == "call_or_meeting" and meeting_subtype == "resume_talks":
                if verdict == "watch_high_upside":
                    return "restart-of-talks setup has strong upside if formal talks really resume"
                if verdict == "watch_late":
                    return "restart-of-talks setup is live, but much of the move may already be behind it"
                if verdict == "watch":
                    return "possible restart-of-talks setup, but it still needs firmer confirmation"
            if verdict == "watch_high_upside":
                return "talks theme has upside, but the catalyst is still soft"
            if verdict == "watch_late":
                return "meeting or negotiation theme is live, but part of the move may be gone"
            if verdict == "watch":
                return "meeting or negotiation theme is interesting, but not confirmed yet"

    if repricing_reason:
        return str(repricing_reason)
    reason = candidate.get("rejection_reason")
    if not reason:
        source = str(candidate.get("radar_source") or "")
        if source == "value":
            return "Passed trade filters"
        if source == "watch":
            return "Interesting theme, but not strong enough for top recommendations"
        return "Theme candidate only"

    labels = {
        "low_confidence": "confidence too low",
        "low_gross_edge": "gross edge too low",
        "low_net_edge": "net edge too low",
        "low_meta_confidence": "meta confidence too low",
        "low_meta_model_prob": "meta-model probability too low",
        "low_graph_consistency": "graph consistency too low",
        "low_robustness": "robustness too low",
        "low_lcb_edge": "lower-bound edge is negative",
    }
    return labels.get(reason, str(reason).replace("_", " "))


def _repricing_case_label(candidate):
    action_family = str(candidate.get("domain_action_family") or "")
    catalyst_type = str(candidate.get("catalyst_type") or "")
    meeting_subtype = str(candidate.get("meeting_subtype") or "")

    if action_family == "release" and catalyst_type == "hostage_release":
        return "Hostage release"
    if action_family == "release" and catalyst_type in {"hearing", "court_ruling", "appeal"}:
        return "Legal catalyst"
    if action_family == "diplomacy":
        if catalyst_type == "call_or_meeting" and meeting_subtype:
            return meeting_subtype_label(meeting_subtype)
        labels = {
            "negotiation": "Negotiation",
            "ceasefire": "Ceasefire talks",
            "call_or_meeting": "Call or meeting",
            "summit": "Summit",
        }
        return labels.get(catalyst_type, "Diplomatic move")
    if action_family == "conflict":
        return "Conflict catalyst"
    if action_family == "regime_shift":
        return "Regime shift"
    return None


def _format_geopolitical_radar(rank, candidate):
    lines = _header_lines(rank, candidate)
    case_label = _repricing_case_label(candidate)
    verdict = _radar_verdict(candidate)
    reason = _radar_reason(candidate)
    if case_label:
        lines.append(f"Case: {case_label}")
    lines.append(f"Verdict: {verdict}")
    lines.append(f"Why: {reason.capitalize()}")
    lines.append(f"Price now: {candidate['entry']:.3f}")
    exit_line = _suggested_exit_line(candidate)
    if exit_line:
        lines.append(exit_line)
    return "\n".join(lines)


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
        "excluded_intraday_crypto": 0,
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
        "low_meta_model_prob": 0,
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

            if USE_META_MODEL_SELECTOR and candidate.get("meta_trade_prob") is not None:
                if candidate["meta_trade_prob"] < MIN_META_TRADE_PROB:
                    rejects["low_meta_model_prob"] += 1
                    _record_rejected(rejected_candidates, candidate, "low_meta_model_prob")
                    continue

            kelly_probability = candidate["fair_lcb"]
            size_multiplier = min(confidence, robustness_score)
        else:
            net_edge_lcb = candidate["net_edge_lcb"]
            if USE_META_MODEL_SELECTOR and candidate.get("meta_trade_prob") is not None:
                if candidate["meta_trade_prob"] < MIN_META_TRADE_PROB:
                    rejects["low_meta_model_prob"] += 1
                    _record_rejected(rejected_candidates, candidate, "low_meta_model_prob")
                    continue
            kelly_probability = candidate["fair"]
            size_multiplier = confidence

        size_multiplier *= candidate.get("repricing_size_multiplier", 1.0)
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
        key=lambda x: (
            x["meta_trade_score"] if x.get("meta_trade_score") is not None else float("-inf"),
            x["net_edge_lcb"],
            x["robustness_score"],
            x["net_edge"],
        ),
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
        key=lambda x: (
            x["meta_trade_score"] if x.get("meta_trade_score") is not None else float("-inf"),
            x["net_edge_lcb"],
            x["robustness_score"],
            x["net_edge"],
        ),
        reverse=True,
    )
    watch_event_usage = defaultdict(int)
    watchlist_pool = []
    for candidate in watchlist_sorted:
        if watch_event_usage[candidate["event_key"]] >= MAX_SIGNALS_PER_EVENT:
            continue
        watch_event_usage[candidate["event_key"]] += 1
        watchlist_pool.append(candidate)
        if len(watchlist_pool) >= MAX_WATCH_CANDIDATE_POOL:
            break

    value_bets = value_bets_limited
    watchlist = watchlist_pool

    diagnostic_candidates = sorted(
        rejected_candidates,
        key=lambda x: (
            -(x.get("repricing_potential") or 0.0),
            x["diagnostic_shortfall"],
            -x["net_edge"],
            -x["gross_edge"],
            -x["confidence"],
        ),
    )[:MAX_DIAGNOSTIC_CANDIDATES]
    near_candidates = watchlist if watchlist else diagnostic_candidates
    sections = build_report_sections(value_bets, watchlist, rejected_candidates)
    geopolitical_radar = sections["geopolitical_radar"]
    geopolitical_radar_core = sections["geopolitical_radar_core"]
    conflict_leaderboard = sections["conflict_leaderboard"]
    legal_catalyst_leaders = sections["legal_catalyst_leaders"]
    release_buy_now = sections["release_buy_now"]
    release_watchlist = sections["release_watchlist"]
    ceasefire_watchlist = sections["ceasefire_watchlist"]
    talk_call_watchlist = sections["talk_call_watchlist"]
    meeting_watchlist = sections["meeting_watchlist"]
    resume_talks_watchlist = sections["resume_talks_watchlist"]
    call_meeting_watchlist = sections["call_meeting_watchlist"]
    hostage_negotiation_watchlist = sections["hostage_negotiation_watchlist"]
    best_watchlist = sections["best_watchlist"]
    paper_scout_candidates = sections["paper_scout_candidates"]
    buy_text = (
        "\n\n".join(_format_signal(i + 1, v) for i, v in enumerate(value_bets))
        if value_bets
        else "none"
    )
    watch_text = (
        "\n\n".join(_format_geopolitical_radar(i + 1, v) for i, v in enumerate(best_watchlist))
        if best_watchlist
        else "none"
    )
    radar_text = (
        "\n\n".join(_format_geopolitical_radar(i + 1, v) for i, v in enumerate(geopolitical_radar_core))
        if geopolitical_radar_core
        else "none"
    )

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    day_summary = f"Today: {len(value_bets)} buy now | {len(best_watchlist)} watchlist | {len(geopolitical_radar_core)} radar"
    report = f"""Polymarket edge scan - {utc_now}
{day_summary}

Scan: {len(markets)} markets | {len(accepted)} passed filters | price {coverage['price_available']}/{len(markets)} | book {coverage['book_available']}/{len(markets)}

Buy Now

{buy_text}

Best Watchlist

{watch_text}

Radar

{radar_text}
"""

    report_payload = {
        "generated_at_utc": utc_now,
        "day_summary": day_summary,
        "build": BOT_BUILD_ID,
        "source": BOT_SOURCE,
        "mode": "research-gated" if LIVE_USE_RESEARCH_GATES else "baseline",
        "scanned": len(markets),
        "passed_base_filters": len(accepted),
        "price_coverage": coverage["price_available"],
        "orderbook_coverage": coverage["book_available"],
        "value_bets": value_bets,
        "conflict_leaderboard": conflict_leaderboard,
        "legal_catalyst_leaders": legal_catalyst_leaders,
        "release_buy_now": release_buy_now,
        "release_watchlist": release_watchlist,
        "ceasefire_watchlist": ceasefire_watchlist,
        "talk_call_watchlist": talk_call_watchlist,
        "meeting_watchlist": meeting_watchlist,
        "resume_talks_watchlist": resume_talks_watchlist,
        "call_meeting_watchlist": call_meeting_watchlist,
        "hostage_negotiation_watchlist": hostage_negotiation_watchlist,
        "best_watchlist": best_watchlist,
        "geopolitical_radar": geopolitical_radar_core,
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
            "max_geopolitical_radar": MAX_GEOPOLITICAL_RADAR,
            "max_conflict_leaderboard": MAX_CONFLICT_LEADERBOARD,
            "max_legal_catalyst_leaders": MAX_LEGAL_CATALYST_LEADERS,
            "max_release_buy_now": MAX_RELEASE_BUY_NOW,
            "max_release_watchlist": MAX_RELEASE_WATCHLIST,
            "max_ceasefire_watchlist": MAX_CEASEFIRE_WATCHLIST,
            "max_talk_call_watchlist": MAX_TALK_CALL_WATCHLIST,
            "max_call_meeting_watchlist": MAX_CALL_MEETING_WATCHLIST,
            "max_resume_talks_watchlist": MAX_RESUME_TALKS_WATCHLIST,
            "max_hostage_negotiation_watchlist": MAX_HOSTAGE_NEGOTIATION_WATCHLIST,
            "min_geopolitical_repricing": MIN_GEOPOLITICAL_REPRICING,
            "use_meta_model_selector": USE_META_MODEL_SELECTOR,
            "min_meta_trade_prob": MIN_META_TRADE_PROB,
            "meta_model_artifact_path": META_MODEL_ARTIFACT_PATH,
        },
        "report_text": report,
    }
    if PAPER_TRADING_ENABLED:
        paper_result = run_paper_cycle(
            markets,
            value_bets,
            best_watchlist=best_watchlist,
            scout_candidates=paper_scout_candidates,
            radar_candidates=geopolitical_radar_core,
            generated_at_utc=utc_now,
        )
        report_payload["paper_trading"] = paper_result["summary"]
        report_payload["report_text"] = paper_result["report_text"]
        report = paper_result["report_text"]

    _write_report_artifacts(report_payload)
    send_message(report)


if __name__ == "__main__":
    run()
