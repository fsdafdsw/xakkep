
def _clamp(value, low=0.01, high=0.99):
    return max(low, min(high, value))


def estimated_probability(market, metrics):
    implied = market.get("ref_price")
    if implied is None:
        return None

    signal = (
        (metrics["momentum"] - 0.5) * 0.45
        + (metrics["orderbook"] - 0.5) * 0.25
        + (metrics["news"] - 0.5) * 0.10
        - (metrics["anomaly"] - 0.5) * 0.20
    )
    # Keep adjustments intentionally small: market-implied probability stays anchor.
    raw_adjustment = signal * 0.12
    confidence = metrics.get("confidence", 0.5)
    adjusted = implied + (raw_adjustment * confidence)

    return _clamp(adjusted)


def net_edge_after_costs(fair_probability, entry_price, taker_fee_bps, slippage_bps, spread):
    if fair_probability is None or entry_price is None:
        return None

    fee = entry_price * (taker_fee_bps / 10000.0)
    slippage = entry_price * (slippage_bps / 10000.0)
    half_spread = (spread / 2.0) if spread is not None else 0.0
    total_cost = fee + slippage + half_spread

    return fair_probability - entry_price - total_cost


def kelly_bet_fraction(probability, entry_price):
    if probability is None or entry_price is None:
        return 0.0
    if not (0 < entry_price < 1):
        return 0.0

    q = 1 - probability
    b = (1 - entry_price) / entry_price
    numerator = (b * probability) - q
    if b <= 0:
        return 0.0

    fraction = numerator / b
    return max(0.0, min(fraction, 1.0))
