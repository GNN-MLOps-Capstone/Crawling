from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RecommendationSession:
    request_id: str
    user_id: int
    limit: int
    timeline_ids: list[int]
    onboarding_queue: list[int]
    behavior_queue: list[int]
    breaking_queue: list[int]
    popular_queue: list[int] = field(default_factory=list)
    served_ids: list[int] = field(default_factory=list)
    current_mix_policy: dict[str, int] = field(default_factory=dict)
    batch_generation_id: int = 1
    created_at: float = field(default_factory=time.time)
    last_served_at: float = field(default_factory=time.time)
    fallback_used: bool = False
    fallback_reason: str | None = None
    source: str = "multipath_mixed"
    prefetch_triggered: bool = False
    prefetch_status: str = "idle"
    prefetch_trigger_path: str | None = None
    prefetched_timeline_ids: list[int] = field(default_factory=list)
    prefetched_onboarding_queue: list[int] = field(default_factory=list)
    prefetched_behavior_queue: list[int] = field(default_factory=list)
    prefetched_breaking_queue: list[int] = field(default_factory=list)
    prefetched_popular_queue: list[int] = field(default_factory=list)
    timeline_path_map: dict[int, str] = field(default_factory=dict)
    prefetched_timeline_path_map: dict[int, str] = field(default_factory=dict)
    debug_context: dict[str, Any] = field(default_factory=dict)
    cache_key: str = ""

    def touch(self) -> None:
        self.last_served_at = time.time()


class SessionCache:
    @staticmethod
    def make_key(request_id: str) -> str:
        return f"recommend:session:{request_id}"

    def get(self, request_id: str) -> RecommendationSession | None:
        raise NotImplementedError

    def set(self, request_id: str, session: RecommendationSession, ttl_seconds: int) -> None:
        raise NotImplementedError


class InMemorySessionCache(SessionCache):
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, RecommendationSession]] = {}

    def get(self, request_id: str) -> RecommendationSession | None:
        cached = self._store.get(self.make_key(request_id))
        if cached is None:
            return None
        expires_at, session = cached
        if expires_at < time.time():
            self._store.pop(self.make_key(request_id), None)
            return None
        return session

    def set(self, request_id: str, session: RecommendationSession, ttl_seconds: int) -> None:
        self._store[self.make_key(request_id)] = (time.time() + ttl_seconds, session)


class RedisSessionCache(SessionCache):
    def __init__(self, client: object) -> None:
        self._client = client

    def get(self, request_id: str) -> RecommendationSession | None:
        raw = self._client.get(self.make_key(request_id))
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
        return RecommendationSession(**payload)

    def set(self, request_id: str, session: RecommendationSession, ttl_seconds: int) -> None:
        self._client.setex(
            self.make_key(request_id),
            ttl_seconds,
            json.dumps(asdict(session)),
        )


class ResilientSessionCache(SessionCache):
    def __init__(self, primary: SessionCache, fallback: SessionCache) -> None:
        self._primary = primary
        self._fallback = fallback

    def get(self, request_id: str) -> RecommendationSession | None:
        try:
            session = self._primary.get(request_id)
            if session is not None:
                return session
        except Exception as exc:
            logger.warning("primary session cache get failed, using fallback backend: %s", exc)
        return self._fallback.get(request_id)

    def set(self, request_id: str, session: RecommendationSession, ttl_seconds: int) -> None:
        primary_failed = False
        try:
            self._primary.set(request_id, session, ttl_seconds)
        except Exception as exc:
            primary_failed = True
            logger.warning("primary session cache set failed, using fallback backend: %s", exc)

        if primary_failed or self._fallback.get(request_id) is not None:
            self._fallback.set(request_id, session, ttl_seconds)


def build_session_cache(*, host: str, port: int, password: str | None) -> SessionCache:
    fallback = InMemorySessionCache()
    try:
        import redis

        client = redis.Redis(
            host=host,
            port=port,
            password=password,
            decode_responses=False,
            socket_connect_timeout=0.2,
            socket_timeout=0.2,
        )
        client.ping()
    except Exception as exc:
        logger.warning("session cache falling back to in-memory backend: %s", exc)
        return fallback

    return ResilientSessionCache(RedisSessionCache(client), fallback)
