import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any


def _normalize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: _normalize(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    return value


def encode_json(payload: Any) -> str:
    return json.dumps(_normalize(payload), separators=(",", ":"))


def decode_json(payload: bytes | str) -> dict[str, Any]:
    raw = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("event payload must be a JSON object")
    return data
