import logging
import time
from collections.abc import Callable

from bot.config import get_settings
from bot.logging import configure_logging
from bot.metrics import ERROR_TOTAL, observe_tick, start_metrics_server


def run_loop(service_name: str, tick: Callable[[], None]) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings.metrics_port)
    logger = logging.getLogger(service_name)
    logger.info(
        "service started",
        extra={
            "loop_interval_seconds": settings.loop_interval_seconds,
            "metrics_port": settings.metrics_port,
        },
    )

    while True:
        try:
            with observe_tick(service_name):
                tick()
        except Exception:
            ERROR_TOTAL.labels(service=service_name).inc()
            logger.exception("service tick failed")
        time.sleep(settings.loop_interval_seconds)
