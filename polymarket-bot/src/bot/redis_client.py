from functools import lru_cache

from redis import Redis

from bot.config import get_settings


@lru_cache(maxsize=1)
def get_redis() -> Redis:
    settings = get_settings()
    return Redis.from_url(settings.redis_url, decode_responses=False)
