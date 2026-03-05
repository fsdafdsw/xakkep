import time
from contextlib import contextmanager
from typing import Iterator

from prometheus_client import Counter, Histogram, start_http_server


TICK_TOTAL = Counter("service_ticks_total", "Total ticks executed", ["service"])
ERROR_TOTAL = Counter("service_errors_total", "Total tick errors", ["service"])
TICK_DURATION = Histogram("service_tick_duration_seconds", "Tick duration", ["service"])
EVENT_IN_TOTAL = Counter("service_events_in_total", "Consumed events", ["service", "topic"])
EVENT_OUT_TOTAL = Counter("service_events_out_total", "Published events", ["service", "topic"])

_metrics_started = False


def start_metrics_server(port: int) -> None:
    global _metrics_started
    if _metrics_started:
        return
    start_http_server(port)
    _metrics_started = True


@contextmanager
def observe_tick(service_name: str) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        TICK_TOTAL.labels(service=service_name).inc()
        TICK_DURATION.labels(service=service_name).observe(elapsed)
