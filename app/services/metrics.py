from __future__ import annotations

import re
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter
from typing import Callable

from fastapi import FastAPI, Request

try:
    from prometheus_client import Counter, Histogram, make_asgi_app
except Exception:  # pragma: no cover - keeps local tests usable without optional metrics dependency.
    Counter = None
    Histogram = None
    make_asgi_app = None

from app.core.config import settings

_LABEL_PATTERN = re.compile(r"[^a-zA-Z0-9_.:-]+")
_EXPERIMENT_ID: ContextVar[str | None] = ContextVar("recommend_experiment_id", default=None)
_VARIANT: ContextVar[str | None] = ContextVar("recommend_variant", default=None)


@dataclass(frozen=True)
class ExperimentLabels:
    experiment_id: str
    variant: str


def current_experiment_labels() -> ExperimentLabels:
    return ExperimentLabels(
        experiment_id=_clean_label(_EXPERIMENT_ID.get() or settings.recommend_experiment_id),
        variant=_clean_label(_VARIANT.get() or settings.recommend_variant),
    )


def set_experiment_labels(*, experiment_id: str, variant: str) -> None:
    _EXPERIMENT_ID.set(experiment_id)
    _VARIANT.set(variant)


def setup_metrics(app: FastAPI) -> None:
    if make_asgi_app is None:
        return

    app.mount("/metrics", make_asgi_app())

    @app.middleware("http")
    async def prometheus_http_middleware(request: Request, call_next: Callable):
        if request.url.path.startswith("/metrics"):
            return await call_next(request)

        started_at = perf_counter()
        status_code = "500"
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            return response
        finally:
            record_http_response(
                method=request.method,
                path=_route_label(request),
                status_code=status_code,
                latency_seconds=perf_counter() - started_at,
            )


def record_http_response(*, method: str, path: str, status_code: str, latency_seconds: float) -> None:
    if _HTTP_REQUESTS_TOTAL is None or _HTTP_REQUEST_LATENCY is None:
        return
    _HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status_code=status_code).inc()
    _HTTP_REQUEST_LATENCY.labels(method=method, path=path).observe(latency_seconds)


def record_recommend_success(
    *,
    user_state: str,
    cache_status: str,
    latency_seconds: float,
    item_count: int,
    fallback_used: bool,
    fallback_reason: str | None,
    item_paths: list[str],
    prefetch_status: str,
    prefetch_trigger_path: str | None,
    session_event: str | None = None,
) -> None:
    labels = current_experiment_labels()
    base = {
        "user_state": _clean_label(user_state),
        "experiment_id": labels.experiment_id,
        "variant": labels.variant,
    }
    if _RECOMMEND_REQUESTS_TOTAL is None:
        return

    _RECOMMEND_REQUESTS_TOTAL.labels(status="success", **base).inc()
    _RECOMMEND_REQUEST_LATENCY.labels(**base).observe(latency_seconds)
    _RECOMMEND_CACHE_EVENTS_TOTAL.labels(cache_status=_clean_label(cache_status), **base).inc()
    _RECOMMEND_ITEMS_RETURNED.labels(**base).observe(item_count)
    if item_count == 0:
        _RECOMMEND_EMPTY_RESULTS_TOTAL.labels(**base).inc()
    if fallback_used:
        _RECOMMEND_FALLBACK_TOTAL.labels(reason=_clean_label(fallback_reason or "unknown"), **base).inc()
    for path, count in _count_values(item_paths).items():
        _RECOMMEND_PATH_ITEMS_TOTAL.labels(path=_clean_label(path), **base).inc(count)
    if prefetch_status:
        _RECOMMEND_PREFETCH_EVENTS_TOTAL.labels(
            status=_clean_label(prefetch_status),
            trigger_path=_clean_label(prefetch_trigger_path or "none"),
            **base,
        ).inc()
    if session_event:
        _RECOMMEND_SESSION_EVENTS_TOTAL.labels(event=_clean_label(session_event), **base).inc()


def record_recommend_error(*, error_code: str, user_state: str = "unknown") -> None:
    labels = current_experiment_labels()
    base = {
        "user_state": _clean_label(user_state),
        "experiment_id": labels.experiment_id,
        "variant": labels.variant,
    }
    if _RECOMMEND_REQUESTS_TOTAL is None:
        return
    _RECOMMEND_REQUESTS_TOTAL.labels(status="error", **base).inc()
    _RECOMMEND_ERRORS_TOTAL.labels(error_code=_clean_label(error_code), **base).inc()


def record_cursor_error(*, reason: str) -> None:
    labels = current_experiment_labels()
    if _RECOMMEND_CURSOR_ERRORS_TOTAL is None:
        return
    _RECOMMEND_CURSOR_ERRORS_TOTAL.labels(
        reason=_cursor_reason_label(reason),
        experiment_id=labels.experiment_id,
        variant=labels.variant,
    ).inc()


def _route_label(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str):
        return path
    return request.url.path


def _clean_label(value: object) -> str:
    raw = str(value or "none")[:64]
    cleaned = _LABEL_PATTERN.sub("_", raw).strip("_")
    return cleaned or "none"


def _cursor_reason_label(reason: str) -> str:
    normalized = reason.lower()
    if "format" in normalized:
        return "invalid_format"
    if "missing" in normalized:
        return "missing_fields"
    if "type" in normalized:
        return "invalid_types"
    if "version" in normalized:
        return "unsupported_version"
    if "limit" in normalized:
        return "limit_mismatch"
    if "request_id" in normalized:
        return "request_id_mismatch"
    if "session" in normalized:
        return "session_not_found"
    if "force_refresh" in normalized:
        return "force_refresh_with_cursor"
    return "invalid_cursor"


def _count_values(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = value or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _counter(name: str, documentation: str, labelnames: tuple[str, ...]):
    if Counter is None:
        return None
    return Counter(name, documentation, labelnames)


def _histogram(name: str, documentation: str, labelnames: tuple[str, ...], buckets: tuple[float, ...]):
    if Histogram is None:
        return None
    return Histogram(name, documentation, labelnames, buckets=buckets)


_HTTP_REQUESTS_TOTAL = _counter(
    "recommend_api_http_requests_total",
    "HTTP requests handled by the recommend API.",
    ("method", "path", "status_code"),
)
_HTTP_REQUEST_LATENCY = _histogram(
    "recommend_api_http_request_latency_seconds",
    "HTTP request latency for the recommend API.",
    ("method", "path"),
    (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
_RECOMMEND_REQUESTS_TOTAL = _counter(
    "recommend_requests_total",
    "Recommendation requests by outcome and user state.",
    ("status", "user_state", "experiment_id", "variant"),
)
_RECOMMEND_REQUEST_LATENCY = _histogram(
    "recommend_request_latency_seconds",
    "Recommendation service latency.",
    ("user_state", "experiment_id", "variant"),
    (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
_RECOMMEND_ERRORS_TOTAL = _counter(
    "recommend_errors_total",
    "Recommendation errors by code.",
    ("error_code", "user_state", "experiment_id", "variant"),
)
_RECOMMEND_CURSOR_ERRORS_TOTAL = _counter(
    "recommend_cursor_errors_total",
    "Recommendation cursor validation errors.",
    ("reason", "experiment_id", "variant"),
)
_RECOMMEND_CACHE_EVENTS_TOTAL = _counter(
    "recommend_cache_events_total",
    "Recommendation session cache events.",
    ("cache_status", "user_state", "experiment_id", "variant"),
)
_RECOMMEND_ITEMS_RETURNED = _histogram(
    "recommend_items_returned",
    "Number of items returned by recommendation responses.",
    ("user_state", "experiment_id", "variant"),
    (0, 1, 2, 5, 10, 20, 50, 100),
)
_RECOMMEND_EMPTY_RESULTS_TOTAL = _counter(
    "recommend_empty_results_total",
    "Recommendation responses with no items.",
    ("user_state", "experiment_id", "variant"),
)
_RECOMMEND_FALLBACK_TOTAL = _counter(
    "recommend_fallback_total",
    "Recommendation responses that used fallback behavior.",
    ("reason", "user_state", "experiment_id", "variant"),
)
_RECOMMEND_PATH_ITEMS_TOTAL = _counter(
    "recommend_path_items_total",
    "Returned recommendation items by public path.",
    ("path", "user_state", "experiment_id", "variant"),
)
_RECOMMEND_PREFETCH_EVENTS_TOTAL = _counter(
    "recommend_prefetch_events_total",
    "Recommendation prefetch events.",
    ("status", "trigger_path", "user_state", "experiment_id", "variant"),
)
_RECOMMEND_SESSION_EVENTS_TOTAL = _counter(
    "recommend_session_events_total",
    "Recommendation session lifecycle events.",
    ("event", "user_state", "experiment_id", "variant"),
)
