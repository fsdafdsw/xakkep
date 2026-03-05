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

    # Confidence favors deep/liquid books and stable microstructure.
    confidence = (
        (quality * 0.45)
        + (orderbook * 0.30)
        + ((1 - anomaly) * 0.15)
        + (news * 0.10)
    )

    return {
        "quality": quality,
        "momentum": momentum,
        "anomaly": anomaly,
        "orderbook": orderbook,
        "news": news,
        "confidence": max(0.0, min(confidence, 1.0)),
    }
