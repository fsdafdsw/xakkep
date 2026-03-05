TOPIC_MARKET_SNAPSHOT = "market.snapshot.v1"
TOPIC_FEATURE_VECTOR = "feature.vector.v1"
TOPIC_FORECAST = "forecast.prediction.v1"
TOPIC_EDGE_SIGNAL = "edge.signal.v1"
TOPIC_RISK_DECISION = "risk.decision.v1"
TOPIC_EXECUTION_EVENT = "execution.order_event.v1"

KEY_TRADING_ENABLED = "ops:trading_enabled"
KEY_REPLAY_CURSOR = "backtest:replay_cursor"
KEY_REPLAY_ROWS = "backtest:replay_rows"
KEY_REPLAY_STATE = "backtest:replay_state"


def stream_offset_key(service_name: str, topic: str) -> str:
    return f"offset:{service_name}:{topic}"


def book_key(market_id: str, outcome_id: str) -> str:
    return f"book:{market_id}:{outcome_id}"
