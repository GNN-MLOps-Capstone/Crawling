from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from time import time
from typing import Any

logger = logging.getLogger(__name__)


def context_hash(context: dict[str, Any]) -> str:
    raw = json.dumps(context, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class RequestLog:
    timestamp: float
    request_id: str
    session_id: str
    user_id: str
    limit: int
    offset: int
    latency_ms: int
    cache_status: str
    context_hash: str
    context_present: bool
    fallback_used: bool
    fallback_reason: str | None
    prefetch_triggered: bool
    prefetch_status: str
    prefetch_trigger_path: str | None
    batch_generation_id: int
    onboarding_remaining: int
    behavior_remaining: int
    breaking_remaining: int
    mix_ratio: dict[str, int]
    source: str


@dataclass(frozen=True)
class ImpressionLog:
    timestamp: float
    request_id: str
    session_id: str
    user_id: str
    news_id: int
    rank: int
    path: str
    batch_generation_id: int


@dataclass(frozen=True)
class ClickLog:
    timestamp: float
    request_id: str
    session_id: str
    user_id: str
    news_id: int
    rank: int
    path: str


def log_request(payload: RequestLog) -> None:
    logger.info("recommend_request %s", json.dumps(asdict(payload), ensure_ascii=False, sort_keys=True))


def now_ts() -> float:
    return time()
