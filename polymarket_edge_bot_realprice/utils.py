from typing import Any, Optional, TypeVar


T = TypeVar("T", int, float)


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def clamp(value: T, low: T, high: T) -> T:
    return max(low, min(high, value))


def clamp_probability(value: float, low: float = 0.01, high: float = 0.99) -> float:
    return clamp(value, low, high)
