from dataclasses import asdict, is_dataclass
from typing import Any

from redis import Redis

from bot.codec import decode_json, encode_json


def publish_event(redis: Redis, topic: str, payload: Any) -> str:
    event = asdict(payload) if is_dataclass(payload) else payload
    return redis.xadd(topic, {"payload": encode_json(event)}, maxlen=50000, approximate=True).decode("utf-8")


def consume_events(
    redis: Redis,
    topic: str,
    offset_key: str,
    count: int = 100,
) -> list[dict[str, Any]]:
    last_id = redis.get(offset_key)
    stream_id = last_id.decode("utf-8") if isinstance(last_id, bytes) else (last_id or "0-0")
    rows = redis.xread({topic: stream_id}, count=count, block=1)
    if not rows:
        return []

    events: list[dict[str, Any]] = []
    latest = stream_id
    for _, entries in rows:
        for event_id, fields in entries:
            payload = fields.get(b"payload", b"{}")
            events.append(decode_json(payload))
            latest = event_id.decode("utf-8") if isinstance(event_id, bytes) else event_id

    redis.set(offset_key, latest)
    return events
