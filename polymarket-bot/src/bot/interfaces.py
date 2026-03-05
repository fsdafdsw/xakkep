from typing import Protocol

from bot.schemas import EdgeSignal, ExecutionRequest, FeatureVector, MarketSnapshot, Prediction, RiskDecision


class MarketDataClient(Protocol):
    def fetch_snapshot(self) -> list[MarketSnapshot]:
        """Return normalized market snapshots."""


class FeatureBuilder(Protocol):
    def build(self, snapshots: list[MarketSnapshot]) -> list[FeatureVector]:
        """Transform snapshots into model-ready features."""


class Forecaster(Protocol):
    def predict(self, features: list[FeatureVector]) -> list[Prediction]:
        """Return calibrated probabilities with uncertainty bounds."""


class EdgeEvaluator(Protocol):
    def evaluate(self, predictions: list[Prediction], snapshots: list[MarketSnapshot]) -> list[EdgeSignal]:
        """Return ranked entry/exit/no-bet signals."""


class RiskManager(Protocol):
    def approve(self, signals: list[EdgeSignal]) -> list[RiskDecision]:
        """Apply limits and cap proposed size."""


class ExecutionGateway(Protocol):
    def submit(self, decision: RiskDecision) -> ExecutionRequest | None:
        """Translate approved decision into executable order request."""
