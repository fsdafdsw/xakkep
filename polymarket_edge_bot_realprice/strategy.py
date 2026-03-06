from config import EXTERNAL_CONFIDENCE_WEIGHT
from external_signal import compute_external_signal
from features import (
    compute_anomaly,
    compute_momentum,
    compute_orderbook_signal,
    compute_quality,
)
from news_signal import news_sentiment

def evaluate_market(m):
    quality = compute_quality(m)
    momentum = compute_momentum(m)
    anomaly = compute_anomaly(m)
    orderbook = compute_orderbook_signal(m)
    news = news_sentiment(m.get("question"))
    external = compute_external_signal(m)

    # Confidence favors deep/liquid books and stable microstructure.
    base_confidence = (
        (quality * 0.45)
        + (orderbook * 0.30)
        + ((1 - anomaly) * 0.15)
        + (news * 0.10)
    )
    confidence = base_confidence + (
        (external["confidence"] - 0.5) * EXTERNAL_CONFIDENCE_WEIGHT
    )

    return {
        "quality": quality,
        "momentum": momentum,
        "anomaly": anomaly,
        "orderbook": orderbook,
        "news": news,
        "external": external["signal"],
        "external_confidence": external["confidence"],
        "domain_name": external["domain_name"],
        "domain_signal": external["domain_signal"],
        "domain_confidence": external["domain_confidence"],
        "market_type": external["market_type"],
        "category_group": external["category_group"],
        "adjustment_multiplier": external["adjustment_multiplier"],
        "factor_weights": external["factor_weights"],
        "external_components": external["components"],
        "confidence": max(0.0, min(confidence, 1.0)),
    }
