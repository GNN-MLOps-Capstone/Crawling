from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RecommendationSession:
    request_id: str
    user_id: str
    limit: int
    timeline_ids: list[int]
    path_a_queue: list[int]
    path_b_queue: list[int]
    path_c_queue: list[int] = field(default_factory=list)
    served_ids: list[int] = field(default_factory=list)
    current_mix_policy: dict[str, int] = field(default_factory=dict)
    batch_generation_id: int = 1
    created_at: float = field(default_factory=time.time)
    last_served_at: float = field(default_factory=time.time)
    fallback_used: bool = False
    fallback_reason: str | None = None
    source: str = "phase1a_hybrid"
    prefetch_triggered: bool = False
    prefetch_status: str = "idle"
    prefetched_timeline_ids: list[int] = field(default_factory=list)
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


def build_session_cache(*, host: str, port: int, password: str | None) -> SessionCache:
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
        return InMemorySessionCache()

    return RedisSessionCache(client)
