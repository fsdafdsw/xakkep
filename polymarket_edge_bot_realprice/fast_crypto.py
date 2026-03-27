from collections import Counter

from config import (
    BANKROLL_USD,
    ESTIMATED_SLIPPAGE_BPS,
    FAST_CRYPTO_ALLOWED_SYMBOLS,
    FAST_CRYPTO_FAIR_SHIFT,
    FAST_CRYPTO_MAX_HOURS_TO_CLOSE,
    FAST_CRYPTO_MIN_CONFIDENCE,
    FAST_CRYPTO_MIN_DIRECTION_BIAS,
    FAST_CRYPTO_MIN_EDGE,
    FAST_CRYPTO_MIN_HOURS_TO_CLOSE,
    FAST_CRYPTO_MIN_ORDERBOOK,
    FAST_CRYPTO_MIN_VOLUME_CONFIRMATION,
    KELLY_FRACTION,
    MAX_BET_USD,
    TAKER_FEE_BPS,
)
from probability_model import kelly_bet_fraction, net_edge_after_costs
from utils import clamp_probability, safe_float


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _market_link(market, token_id):
    slug = market.get("event_slug") or market.get("slug")
    if slug and token_id:
        return f"https://polymarket.com/event/{slug}?tid={token_id}"
    if slug:
        return f"https://polymarket.com/event/{slug}"
    return "https://polymarket.com/"


def _question_text(market):
    return str(market.get("question") or "").lower().strip()


def _symbol_for_market(market):
    question = _question_text(market)
    symbols = (
        ("bitcoin", "BTC"),
        ("btc", "BTC"),
        ("ethereum", "ETH"),
        ("eth", "ETH"),
        ("solana", "SOL"),
        ("sol", "SOL"),
        ("ripple", "XRP"),
        ("xrp", "XRP"),
    )
    for token, label in symbols:
        if token in question:
            return label
    return "CRYPTO"


def is_fast_crypto_market(market):
    question = _question_text(market)
    if market.get("category_group") != "crypto":
        return False
    if "up or down" not in question:
        return False
    if FAST_CRYPTO_ALLOWED_SYMBOLS and not any(symbol in question for symbol in FAST_CRYPTO_ALLOWED_SYMBOLS):
        return False

    hours_to_close = safe_float(market.get("hours_to_close"))
    if hours_to_close is None:
        return False
    if hours_to_close < FAST_CRYPTO_MIN_HOURS_TO_CLOSE or hours_to_close > FAST_CRYPTO_MAX_HOURS_TO_CLOSE:
        return False

    outcomes = [str(outcome).strip().lower() for outcome in (market.get("outcomes") or [])]
    return "up" in outcomes and "down" in outcomes and len(market.get("outcome_prices") or []) >= 2


def _outcome_index(outcomes, target):
    target = str(target or "").lower()
    for idx, outcome in enumerate(outcomes):
        if str(outcome).strip().lower() == target:
            return idx
    return None


def _directional_bias(metrics, market):
    momentum = safe_float(metrics.get("momentum"), default=0.5)
    one_hour_change = safe_float(market.get("one_hour_change"), default=0.0)
    one_day_change = safe_float(market.get("one_day_change"), default=0.0)
    raw = ((momentum - 0.5) * 2.0) * 0.65
    raw += _clamp(one_hour_change / 0.020, low=-1.0, high=1.0) * 0.25
    raw += _clamp(one_day_change / 0.060, low=-1.0, high=1.0) * 0.10
    return max(-1.0, min(1.0, raw))


def _micro_confidence(metrics, market):
    orderbook = safe_float(metrics.get("orderbook"), default=0.5)
    volume_confirmation = safe_float(metrics.get("volume_confirmation"), default=0.0)
    anomaly = safe_float(metrics.get("anomaly"), default=0.5)
    spread = safe_float(market.get("spread"), default=0.05)
    spread_penalty = _clamp(spread / 0.05)
    confidence = 0.42
    confidence += orderbook * 0.24
    confidence += volume_confirmation * 0.22
    confidence += (1.0 - anomaly) * 0.14
    confidence -= spread_penalty * 0.10
    return _clamp(confidence)


def _fast_crypto_candidate(item):
    market = item["market"]
    metrics = item["metrics"]
    if not is_fast_crypto_market(market):
        return None

    outcomes = market.get("outcomes") or []
    prices = market.get("outcome_prices") or []
    token_ids = market.get("token_ids") or []
    up_idx = _outcome_index(outcomes, "up")
    down_idx = _outcome_index(outcomes, "down")
    if up_idx is None or down_idx is None:
        return None

    up_entry = safe_float(prices[up_idx] if up_idx < len(prices) else None)
    down_entry = safe_float(prices[down_idx] if down_idx < len(prices) else None)
    if up_entry is None or down_entry is None:
        return None

    base_selected_idx = int(market.get("selected_outcome_index") or 0)
    base_selected_label = str(outcomes[base_selected_idx]).strip().lower() if 0 <= base_selected_idx < len(outcomes) else "up"
    base_fair = safe_float(item.get("fair"), default=0.5)
    if base_selected_label == "up":
        fair_up = base_fair
    elif base_selected_label == "down":
        fair_up = 1.0 - base_fair
    else:
        fair_up = base_fair if up_idx == 0 else (1.0 - base_fair)

    direction_bias = _directional_bias(metrics, market)
    volume_confirmation = safe_float(metrics.get("volume_confirmation"), default=0.0)
    orderbook = safe_float(metrics.get("orderbook"), default=0.5)
    anomaly = safe_float(metrics.get("anomaly"), default=0.5)

    fair_up = clamp_probability(
        fair_up
        + (direction_bias * FAST_CRYPTO_FAIR_SHIFT)
        + ((volume_confirmation - 0.5) * 0.04)
        + ((orderbook - 0.5) * 0.03)
        - (max(0.0, anomaly - 0.5) * 0.04)
    )
    fair_down = clamp_probability(1.0 - fair_up)

    spread = market.get("spread")
    up_edge = net_edge_after_costs(fair_up, up_entry, TAKER_FEE_BPS, ESTIMATED_SLIPPAGE_BPS, spread)
    down_edge = net_edge_after_costs(fair_down, down_entry, TAKER_FEE_BPS, ESTIMATED_SLIPPAGE_BPS, spread)

    if (up_edge or -999.0) >= (down_edge or -999.0):
        selected_outcome = "Up"
        selected_index = up_idx
        selected_entry = up_entry
        selected_fair = fair_up
        selected_edge = up_edge
        selected_bias = direction_bias
    else:
        selected_outcome = "Down"
        selected_index = down_idx
        selected_entry = down_entry
        selected_fair = fair_down
        selected_edge = down_edge
        selected_bias = -direction_bias

    confidence = _micro_confidence(metrics, market)
    score = _clamp(
        0.45
        + max(0.0, selected_edge or 0.0) * 10.0
        + max(0.0, selected_bias) * 0.20
        + volume_confirmation * 0.10
        + orderbook * 0.08
    )

    verdict = "ignore"
    if (
        (selected_edge or 0.0) >= FAST_CRYPTO_MIN_EDGE
        and confidence >= FAST_CRYPTO_MIN_CONFIDENCE
        and selected_bias >= FAST_CRYPTO_MIN_DIRECTION_BIAS
        and volume_confirmation >= FAST_CRYPTO_MIN_VOLUME_CONFIRMATION
        and orderbook >= FAST_CRYPTO_MIN_ORDERBOOK
    ):
        verdict = "buy_now"
    elif (
        (selected_edge or 0.0) > 0.0
        and confidence >= max(0.50, FAST_CRYPTO_MIN_CONFIDENCE - 0.06)
    ):
        verdict = "watch"

    stake_usd = min(
        MAX_BET_USD,
        BANKROLL_USD * kelly_bet_fraction(selected_fair, selected_entry) * KELLY_FRACTION * confidence,
    )

    token_id = token_ids[selected_index] if selected_index < len(token_ids) else market.get("selected_token_id")
    symbol = _symbol_for_market(market)
    hours_to_close = safe_float(market.get("hours_to_close"), default=0.0)

    return {
        "event_key": market.get("event_id") or market.get("event_slug") or market.get("id"),
        "market_id": market.get("id"),
        "event_slug": market.get("event_slug"),
        "selected_token_id": token_id,
        "market_key": f"{market.get('event_slug') or market.get('slug') or market.get('id')}|{token_id or ''}",
        "question": market.get("question"),
        "event_title": market.get("event_title"),
        "primary_entity_key": symbol.lower(),
        "market_type": market.get("market_type"),
        "category_group": market.get("category_group"),
        "outcomes": outcomes,
        "selected_outcome": selected_outcome,
        "selected_outcome_index": selected_index,
        "link": _market_link(market, token_id),
        "entry": selected_entry,
        "spread": spread,
        "cost_per_share": (selected_entry * ((TAKER_FEE_BPS + ESTIMATED_SLIPPAGE_BPS) / 10000.0))
        + (((spread or 0.0) / 2.0) if spread is not None else 0.0),
        "fair": selected_fair,
        "fair_lcb": selected_fair,
        "gross_edge": selected_fair - selected_entry,
        "net_edge": selected_edge,
        "gross_edge_lcb": selected_fair - selected_entry,
        "net_edge_lcb": selected_edge,
        "confidence": confidence,
        "meta_confidence": confidence,
        "graph_consistency": 1.0,
        "robustness_score": confidence,
        "domain_name": "fast_crypto_micro",
        "domain_signal": selected_fair,
        "domain_confidence": confidence,
        "domain_action_family": "crypto_micro",
        "repricing_potential": score,
        "repricing_score": score,
        "repricing_watch_score": score,
        "repricing_verdict": verdict,
        "repricing_reason": "short crypto microstructure momentum",
        "repricing_attention_gap": max(0.0, selected_bias),
        "repricing_fresh_catalyst_score": volume_confirmation,
        "repricing_lane_key": "crypto_micro",
        "repricing_lane_label": "Crypto fast lane",
        "repricing_lane_prior": 0.86,
        "repricing_size_multiplier": 1.0,
        "thesis_id": f"crypto_micro:{market.get('event_slug') or market.get('id')}",
        "thesis_type": "standalone",
        "thesis_cluster_size": 1,
        "thesis_surface_selected": True,
        "catalyst_type": "up_down_short",
        "stake_usd": max(stake_usd, 0.0),
        "hours_to_close": hours_to_close,
        "end_ts": market.get("end_ts"),
        "fast_crypto_symbol": symbol,
        "fast_crypto_score": score,
        "fast_crypto_direction_bias": max(0.0, selected_bias),
        "fast_crypto_volume_confirmation": volume_confirmation,
        "fast_crypto_orderbook": orderbook,
    }


def build_fast_crypto_candidates(accepted_items):
    buy_candidates = []
    watch_candidates = []
    market_count = 0
    symbol_counter = Counter()

    for item in accepted_items or []:
        market = item.get("market") or {}
        if not is_fast_crypto_market(market):
            continue
        market_count += 1
        symbol_counter[_symbol_for_market(market)] += 1

        candidate = _fast_crypto_candidate(item)
        if not candidate:
            continue
        verdict = str(candidate.get("repricing_verdict") or "")
        if verdict == "buy_now":
            buy_candidates.append(candidate)
        elif verdict == "watch":
            watch_candidates.append(candidate)

    buy_candidates.sort(
        key=lambda row: (
            row.get("fast_crypto_score") or 0.0,
            row.get("net_edge") or float("-inf"),
            row.get("confidence") or 0.0,
        ),
        reverse=True,
    )
    watch_candidates.sort(
        key=lambda row: (
            row.get("fast_crypto_score") or 0.0,
            row.get("net_edge") or float("-inf"),
            row.get("confidence") or 0.0,
        ),
        reverse=True,
    )

    return {
        "buy_candidates": buy_candidates,
        "watch_candidates": watch_candidates[:5],
        "summary": {
            "active_short_markets": market_count,
            "buy_now_count": len(buy_candidates),
            "watch_count": len(watch_candidates),
            "symbols": dict(sorted(symbol_counter.items())),
        },
    }


def fast_crypto_report_prefix(summary):
    symbols = summary.get("symbols") or {}
    symbol_bits = ", ".join(f"{symbol}:{count}" for symbol, count in sorted(symbols.items())) or "none"
    return (
        "Fast crypto mode\n"
        f"Short markets: {summary.get('active_short_markets', 0)} | "
        f"buy now: {summary.get('buy_now_count', 0)} | "
        f"watch: {summary.get('watch_count', 0)}\n"
        f"Symbols: {symbol_bits}"
    )
