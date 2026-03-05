from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


class OrderStatus(str, Enum):
    NEW = "new"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class SignalAction(str, Enum):
    ENTER = "enter"
    EXIT = "exit"
    HOLD = "hold"
    NO_BET = "no_bet"


@dataclass(slots=True)
class MarketSnapshot:
    timestamp: datetime
    market_id: str
    outcome_id: str
    best_bid: float
    best_ask: float
    depth_bid: float
    depth_ask: float
    last_price: float
    volume_1h: float


@dataclass(slots=True)
class FeatureVector:
    timestamp: datetime
    market_id: str
    outcome_id: str
    mid_price: float
    spread: float
    imbalance: float
    momentum_5m: float
    volatility_1h: float
    external_score: float


@dataclass(slots=True)
class Prediction:
    timestamp: datetime
    market_id: str
    outcome_id: str
    p_hat: float
    p_low: float
    p_high: float
    model_version: str


@dataclass(slots=True)
class EdgeSignal:
    timestamp: datetime
    market_id: str
    outcome_id: str
    action: SignalAction
    suggested_side: Side
    edge_net: float
    confidence: float
    reason: str


@dataclass(slots=True)
class RiskDecision:
    timestamp: datetime
    market_id: str
    outcome_id: str
    action: SignalAction
    side: Side
    order_type: OrderType
    target_price: float
    allow: bool
    capped_size_usd: float
    reason: str


@dataclass(slots=True)
class ExecutionRequest:
    timestamp: datetime
    market_id: str
    outcome_id: str
    side: Side
    order_type: OrderType
    price: float
    size: float
    client_order_id: str


@dataclass(slots=True)
class OrderEvent:
    timestamp: datetime
    client_order_id: str
    status: OrderStatus
    filled_size: float
    avg_fill_price: float
    fee_paid: float
