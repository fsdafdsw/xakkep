from fastapi import FastAPI
from fastapi import Query
from fastapi import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
import uvicorn

from bot.config import get_settings
from bot.constants import KEY_REPLAY_CURSOR, KEY_REPLAY_ROWS, KEY_REPLAY_STATE, KEY_TRADING_ENABLED
from bot.db import get_db
from bot.redis_client import get_redis

app = FastAPI(title="Bot Ops API", version="0.1.0")
OPS_ACTIONS_TOTAL = Counter("ops_actions_total", "Ops actions", ["action"])


def _read_trading_enabled() -> bool:
    settings = get_settings()
    redis = get_redis()
    raw = redis.get(KEY_TRADING_ENABLED)
    if raw is None:
        redis.set(KEY_TRADING_ENABLED, "1" if settings.trading_enabled else "0")
        return settings.trading_enabled
    value = raw.decode("utf-8").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _write_trading_enabled(value: bool) -> None:
    redis = get_redis()
    redis.set(KEY_TRADING_ENABLED, "1" if value else "0")


def _read_key_as_text(key: str, fallback: str = "") -> str:
    redis = get_redis()
    value = redis.get(key)
    if value is None:
        return fallback
    return value.decode("utf-8")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/status")
def status() -> dict[str, bool]:
    return {"trading_enabled": _read_trading_enabled()}


@app.post("/pause")
def pause() -> dict[str, bool]:
    _write_trading_enabled(False)
    OPS_ACTIONS_TOTAL.labels(action="pause").inc()
    return {"trading_enabled": False}


@app.post("/resume")
def resume() -> dict[str, bool]:
    _write_trading_enabled(True)
    OPS_ACTIONS_TOTAL.labels(action="resume").inc()
    return {"trading_enabled": True}


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/backtest/status")
def backtest_status() -> dict[str, str]:
    return {
        "state": _read_key_as_text(KEY_REPLAY_STATE, "idle"),
        "cursor": _read_key_as_text(KEY_REPLAY_CURSOR, ""),
        "rows_replayed": _read_key_as_text(KEY_REPLAY_ROWS, "0"),
    }


@app.post("/backtest/reset")
def backtest_reset(clear_offsets: bool = Query(default=True)) -> dict[str, str]:
    redis = get_redis()
    db = get_db()
    db.reset_runtime_state()
    redis.delete(KEY_REPLAY_CURSOR)
    redis.set(KEY_REPLAY_ROWS, "0")
    redis.set(KEY_REPLAY_STATE, "reset_done")
    if clear_offsets:
        for key in redis.scan_iter(match="offset:*"):
            redis.delete(key)
    OPS_ACTIONS_TOTAL.labels(action="backtest_reset").inc()
    return {"state": "reset_done"}


def main() -> None:
    settings = get_settings()
    uvicorn.run(app, host=settings.ops_api_host, port=settings.ops_api_port)


if __name__ == "__main__":
    main()
