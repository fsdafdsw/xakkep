from calibration import apply_family_calibration, load_calibrators
from config import CALIBRATION_ARTIFACT_PATH, EXTERNAL_SIGNAL_WEIGHT


_CALIBRATION_CACHE = {
    "path": None,
    "artifact": None,
    "loaded": False,
}


def _clamp(value, low=0.01, high=0.99):
    return max(low, min(high, value))


def _get_calibration_artifact():
    path = CALIBRATION_ARTIFACT_PATH
    if not path:
        return None

    if _CALIBRATION_CACHE["loaded"] and _CALIBRATION_CACHE["path"] == path:
        return _CALIBRATION_CACHE["artifact"]

    artifact = None
    try:
        artifact = load_calibrators(path)
    except FileNotFoundError:
        artifact = None

    _CALIBRATION_CACHE["path"] = path
    _CALIBRATION_CACHE["artifact"] = artifact
    _CALIBRATION_CACHE["loaded"] = True
    return artifact


def estimated_probability(market, metrics, adjustment_scale=0.12):
    implied = market.get("ref_price")
    if implied is None:
        return None

    factor_weights = metrics.get(
        "factor_weights",
        {
            "momentum": 0.45,
            "orderbook": 0.25,
            "news": 0.10,
            "anomaly": 0.20,
        },
    )
    signal = (
        (metrics["momentum"] - 0.5) * factor_weights.get("momentum", 0.45)
        + (metrics["orderbook"] - 0.5) * factor_weights.get("orderbook", 0.25)
        + (metrics["news"] - 0.5) * factor_weights.get("news", 0.10)
        - (metrics["anomaly"] - 0.5) * factor_weights.get("anomaly", 0.20)
        + (metrics.get("external", 0.5) - 0.5) * EXTERNAL_SIGNAL_WEIGHT
    )
    # Keep adjustments intentionally small: market-implied probability stays anchor.
    raw_adjustment = signal * adjustment_scale * metrics.get("adjustment_multiplier", 1.0)
    confidence = metrics.get("confidence", 0.5)
    adjusted = implied + (raw_adjustment * confidence)
    adjusted = _clamp(adjusted)

    calibrators = _get_calibration_artifact()
    family = metrics.get("market_type") or market.get("market_type") or "unknown"
    if calibrators:
        return apply_family_calibration(adjusted, family, calibrators)
    return adjusted


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
