from __future__ import annotations

import hashlib
import logging
import random
from dataclasses import dataclass, field
from threading import Lock

from app.core.config import settings

logger = logging.getLogger(__name__)

SUPPORTED_VARIANTS = {"recommend", "latest", "popular", "random"}


@dataclass(frozen=True)
class ExperimentAssignment:
    experiment_id: str
    variant: str

    @property
    def strategy(self) -> str:
        return self.variant if self.variant in SUPPORTED_VARIANTS else "recommend"


class ExperimentCounterStore:
    def next_sequence(self, *, experiment_id: str, user_id: int) -> int:
        raise NotImplementedError


class InMemoryExperimentCounterStore(ExperimentCounterStore):
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters: dict[tuple[str, int], int] = {}

    def next_sequence(self, *, experiment_id: str, user_id: int) -> int:
        key = (experiment_id, user_id)
        with self._lock:
            next_value = self._counters.get(key, 0) + 1
            self._counters[key] = next_value
        return next_value


class RedisExperimentCounterStore(ExperimentCounterStore):
    def __init__(self, client: object, *, key_prefix: str) -> None:
        self._client = client
        self._key_prefix = key_prefix

    def next_sequence(self, *, experiment_id: str, user_id: int) -> int:
        return int(self._client.incr(self._key(experiment_id=experiment_id, user_id=user_id)))

    def _key(self, *, experiment_id: str, user_id: int) -> str:
        return f"{self._key_prefix}:{experiment_id}:user:{user_id}:sequence"


def build_experiment_counter_store(*, host: str, port: int, password: str | None) -> ExperimentCounterStore:
    fallback = InMemoryExperimentCounterStore()
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
        logger.warning("experiment counter store falling back to in-memory backend: %s", exc)
        return fallback

    return RedisExperimentCounterStore(client, key_prefix=settings.recommend_experiment_counter_key_prefix)


@dataclass(frozen=True)
class ExperimentService:
    counter_store: ExperimentCounterStore = field(default_factory=InMemoryExperimentCounterStore)

    def assign(self, *, user_id: int, eligible: bool = True) -> ExperimentAssignment:
        experiment_id = _clean(settings.recommend_experiment_id) or "control"
        override = _clean(settings.recommend_variant)
        if override and override != "default":
            return ExperimentAssignment(experiment_id=experiment_id, variant=override)
        if not eligible:
            return ExperimentAssignment(experiment_id=experiment_id, variant="recommend")

        slots = self._variant_slots(settings.recommend_experiment_variants)
        if not slots:
            slots = ("recommend",)

        sequence = self.counter_store.next_sequence(experiment_id=experiment_id, user_id=user_id)
        variant = slots[(sequence - 1) % len(slots)]
        return ExperimentAssignment(experiment_id=experiment_id, variant=variant)

    def stable_random(self, *, assignment: ExperimentAssignment, user_id: int, request_id: str) -> random.Random:
        seed = self._stable_bucket(
            user_id=user_id,
            experiment_id=f"{assignment.experiment_id}:{assignment.variant}:{request_id}",
            modulo=2**32,
        )
        return random.Random(seed)

    @staticmethod
    def _parse_variants(raw_value: str) -> tuple[tuple[str, int], ...]:
        variants: list[tuple[str, int]] = []
        for raw_part in raw_value.split(","):
            part = raw_part.strip()
            if not part:
                continue
            if ":" not in part:
                variant, raw_weight = part, "1"
            else:
                variant, raw_weight = part.split(":", 1)
            variant = _clean(variant)
            try:
                weight = int(raw_weight)
            except ValueError:
                continue
            if variant and weight > 0:
                variants.append((variant, weight))
        return tuple(variants)

    @classmethod
    def _variant_slots(cls, raw_value: str) -> tuple[str, ...]:
        slots: list[str] = []
        for variant, weight in cls._parse_variants(raw_value):
            slots.extend([variant] * weight)
        return tuple(slots)

    @staticmethod
    def _stable_bucket(*, user_id: int, experiment_id: str, modulo: int) -> int:
        if modulo <= 0:
            return 0
        raw = f"{experiment_id}:{user_id}".encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        return int(digest[:16], 16) % modulo


def _clean(value: object) -> str:
    return str(value or "").strip()
