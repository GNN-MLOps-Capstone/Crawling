from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BanditArm:
    path: str
    weight: int


@dataclass(frozen=True)
class BanditPosterior:
    alpha: float = 0.0
    beta: float = 0.0


@dataclass(frozen=True)
class MixPlan:
    timeline_entries: list[tuple[int, str]]
    mix_policy: dict[str, int]
    allocator: str
    arm_scores: dict[str, float] = field(default_factory=dict)
    fallback_reason: str | None = None


class BanditStateStore:
    def get_global_posteriors(self, *, paths: list[str]) -> dict[str, BanditPosterior]:
        raise NotImplementedError

    def get_user_posteriors(self, *, user_id: int, paths: list[str]) -> dict[str, BanditPosterior]:
        raise NotImplementedError


class NullBanditStateStore(BanditStateStore):
    def get_global_posteriors(self, *, paths: list[str]) -> dict[str, BanditPosterior]:
        return {path: BanditPosterior() for path in paths}

    def get_user_posteriors(self, *, user_id: int, paths: list[str]) -> dict[str, BanditPosterior]:
        del user_id
        return {path: BanditPosterior() for path in paths}


class RedisBanditStateStore(BanditStateStore):
    def __init__(self, client: object, *, key_prefix: str) -> None:
        self._client = client
        self._key_prefix = key_prefix

    def get_global_posteriors(self, *, paths: list[str]) -> dict[str, BanditPosterior]:
        return {
            path: self._read_posterior(self._global_key(path))
            for path in paths
        }

    def get_user_posteriors(self, *, user_id: int, paths: list[str]) -> dict[str, BanditPosterior]:
        return {
            path: self._read_posterior(self._user_key(user_id=user_id, path=path))
            for path in paths
        }

    def _global_key(self, path: str) -> str:
        return f"{self._key_prefix}:global:{path}"

    def _user_key(self, *, user_id: int, path: str) -> str:
        return f"{self._key_prefix}:user:{user_id}:{path}"

    def _read_posterior(self, key: str) -> BanditPosterior:
        raw = self._client.get(key)
        if raw is None:
            return BanditPosterior()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
        alpha = float(payload.get("alpha", 0.0))
        beta = float(payload.get("beta", 0.0))
        if alpha < 0 or beta < 0:
            raise ValueError(f"invalid posterior payload for key={key}")
        return BanditPosterior(alpha=alpha, beta=beta)


def build_bandit_state_store(
    *,
    host: str,
    port: int,
    password: str | None,
    key_prefix: str,
) -> BanditStateStore:
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
        logger.warning("bandit state store falling back to null backend: %s", exc)
        return NullBanditStateStore()

    return RedisBanditStateStore(client, key_prefix=key_prefix)


@dataclass(frozen=True)
class BanditService:
    arms: tuple[BanditArm, ...]
    first_page_window: int
    min_breaking_in_window: int
    allocator: str = "fixed"
    batch_size: int = 20
    min_per_arm: int = 2
    prior_alpha: float = 2.0
    prior_beta: float = 2.0
    state_store: BanditStateStore = field(default_factory=NullBanditStateStore)
    random_seed: int | None = None

    def build_mix_plan(
        self,
        *,
        path_candidates: dict[str, list[int]],
        user_id: int | None = None,
    ) -> MixPlan:
        if self.allocator == "thompson":
            try:
                return self._build_thompson_mix_plan(
                    path_candidates=path_candidates,
                    user_id=user_id,
                )
            except Exception as exc:
                logger.warning("bandit thompson allocator failed, falling back to fixed: %s", exc)
                fixed = self._build_fixed_mix_plan(path_candidates=path_candidates)
                return MixPlan(
                    timeline_entries=fixed.timeline_entries,
                    mix_policy=fixed.mix_policy,
                    allocator="fixed",
                    arm_scores=fixed.arm_scores,
                    fallback_reason="bandit_allocator_failed",
                )

        return self._build_fixed_mix_plan(path_candidates=path_candidates)

    def _build_fixed_mix_plan(self, *, path_candidates: dict[str, list[int]]) -> MixPlan:
        timeline_entries = self._build_fixed_mix_timeline(path_candidates=path_candidates)
        timeline_entries = self._apply_legacy_breaking_guardrail(timeline_entries)
        return MixPlan(
            timeline_entries=timeline_entries,
            mix_policy={arm.path: arm.weight for arm in self.arms},
            allocator="fixed",
        )

    def _build_thompson_mix_plan(
        self,
        *,
        path_candidates: dict[str, list[int]],
        user_id: int | None,
    ) -> MixPlan:
        remaining_candidates = {
            arm.path: path_candidates.get(arm.path, [])[:]
            for arm in self.arms
        }
        timeline_entries: list[tuple[int, str]] = []
        arm_scores = self._compute_arm_scores(
            user_id=user_id,
            paths=[arm.path for arm in self.arms],
        )

        while any(remaining_candidates[path] for path in remaining_candidates):
            window_entries = self._build_thompson_window(
                remaining_candidates=remaining_candidates,
                arm_scores=arm_scores,
            )
            if not window_entries:
                break
            timeline_entries.extend(window_entries)

        return MixPlan(
            timeline_entries=timeline_entries,
            mix_policy=self._count_paths(timeline_entries),
            allocator="thompson",
            arm_scores=arm_scores,
        )

    def _build_fixed_mix_timeline(self, *, path_candidates: dict[str, list[int]]) -> list[tuple[int, str]]:
        mixed: list[tuple[int, str]] = []
        seen: set[int] = set()
        indices = {arm.path: 0 for arm in self.arms}

        while any(indices[arm.path] < len(path_candidates.get(arm.path, [])) for arm in self.arms):
            for arm in self.arms:
                candidates = path_candidates.get(arm.path, [])
                for _ in range(arm.weight):
                    if indices[arm.path] >= len(candidates):
                        break
                    news_id = candidates[indices[arm.path]]
                    indices[arm.path] += 1
                    if news_id in seen:
                        continue
                    mixed.append((news_id, arm.path))
                    seen.add(news_id)

        return mixed

    def _build_thompson_window(
        self,
        *,
        remaining_candidates: dict[str, list[int]],
        arm_scores: dict[str, float],
    ) -> list[tuple[int, str]]:
        available_paths = [arm.path for arm in self.arms if remaining_candidates.get(arm.path)]
        if not available_paths:
            return []

        target_window = min(
            self.batch_size,
            sum(len(remaining_candidates[path]) for path in available_paths),
        )
        allocation = {path: 0 for path in available_paths}
        while sum(allocation.values()) < target_window:
            eligible_paths = [
                path
                for path in available_paths
                if allocation[path] < len(remaining_candidates[path])
            ]
            if not eligible_paths:
                break
            selected_path = max(
                eligible_paths,
                key=lambda path: (arm_scores.get(path, 0.0), -allocation[path], path),
            )
            allocation[selected_path] += 1

        allocation = self._apply_minimum_allocation_guardrail_to_allocation(
            allocation=allocation,
            remaining_candidates=remaining_candidates,
            arm_scores=arm_scores,
            target_window=target_window,
        )
        slot_paths = self._expand_allocation_to_slots(
            allocation=allocation,
            arm_scores=arm_scores,
        )

        window_entries: list[tuple[int, str]] = []
        for path in slot_paths:
            candidates = remaining_candidates[path]
            if not candidates:
                continue
            news_id = candidates.pop(0)
            window_entries.append((news_id, path))

        return window_entries

    def _compute_arm_scores(self, *, user_id: int | None, paths: list[str]) -> dict[str, float]:
        global_posteriors = self.state_store.get_global_posteriors(paths=paths)
        user_posteriors = (
            self.state_store.get_user_posteriors(user_id=user_id, paths=paths)
            if user_id is not None
            else {path: BanditPosterior() for path in paths}
        )
        rng = random.Random(self.random_seed)
        scores: dict[str, float] = {}
        for path in paths:
            global_posterior = global_posteriors.get(path, BanditPosterior())
            user_posterior = user_posteriors.get(path, BanditPosterior())
            alpha = self.prior_alpha + global_posterior.alpha + user_posterior.alpha
            beta = self.prior_beta + global_posterior.beta + user_posterior.beta
            scores[path] = self._sample_beta(alpha=alpha, beta=beta, rng=rng)
        return scores

    @staticmethod
    def _sample_beta(*, alpha: float, beta: float, rng: random.Random) -> float:
        return rng.betavariate(alpha, beta)

    @staticmethod
    def _expand_allocation_to_slots(
        *,
        allocation: dict[str, int],
        arm_scores: dict[str, float],
    ) -> list[str]:
        remaining = allocation.copy()
        slots: list[str] = []
        last_path: str | None = None

        while sum(remaining.values()) > 0:
            eligible = [path for path, count in remaining.items() if count > 0]
            if not eligible:
                break
            eligible.sort(
                key=lambda path: (
                    path == last_path,
                    -remaining[path],
                    -arm_scores.get(path, 0.0),
                    path,
                )
            )
            selected = eligible[0]
            slots.append(selected)
            remaining[selected] -= 1
            last_path = selected

        return slots

    def _apply_legacy_breaking_guardrail(self, timeline_entries: list[tuple[int, str]]) -> list[tuple[int, str]]:
        if not timeline_entries:
            return timeline_entries

        guarded = timeline_entries[:]
        window_size = min(self.first_page_window, len(guarded))
        requirements = (("breaking", self.min_breaking_in_window),)
        protected_paths = {required_path for required_path, min_count in requirements if min_count > 0}
        for required_path, min_count in requirements:
            if min_count <= 0:
                continue
            guarded = self._promote_path_into_window(
                timeline_entries=guarded,
                path=required_path,
                min_count=min_count,
                window_size=window_size,
                protected_paths=protected_paths,
            )
        return guarded

    def _apply_minimum_allocation_guardrail_to_allocation(
        self,
        *,
        allocation: dict[str, int],
        remaining_candidates: dict[str, list[int]],
        arm_scores: dict[str, float],
        target_window: int,
    ) -> dict[str, int]:
        guarded = allocation.copy()
        minimums = {
            path: min(self.min_per_arm, len(remaining_candidates[path]), target_window)
            for path in allocation
        }
        for path, min_count in minimums.items():
            while guarded[path] < min_count:
                donors = [
                    donor_path
                    for donor_path, donor_count in guarded.items()
                    if donor_path != path and donor_count > minimums.get(donor_path, 0)
                ]
                if not donors:
                    break
                donor = min(
                    donors,
                    key=lambda donor_path: (arm_scores.get(donor_path, 0.0), guarded[donor_path], donor_path),
                )
                guarded[donor] -= 1
                guarded[path] += 1
        return guarded

    @staticmethod
    def _promote_path_into_window(
        *,
        timeline_entries: list[tuple[int, str]],
        path: str,
        min_count: int,
        window_size: int,
        protected_paths: set[str],
    ) -> list[tuple[int, str]]:
        if window_size <= 0:
            return timeline_entries

        updated = timeline_entries[:]
        existing_count = sum(1 for _, item_path in updated[:window_size] if item_path == path)
        if existing_count >= min_count:
            return updated

        for index in range(window_size, len(updated)):
            if updated[index][1] != path:
                continue
            swap_target = BanditService._find_swap_target(
                timeline_entries=updated,
                protected_paths=protected_paths,
                window_size=window_size,
            )
            if swap_target is None:
                break
            updated[swap_target], updated[index] = updated[index], updated[swap_target]
            existing_count += 1
            if existing_count >= min_count:
                break
        return updated

    @staticmethod
    def _find_swap_target(
        *,
        timeline_entries: list[tuple[int, str]],
        protected_paths: set[str],
        window_size: int,
    ) -> int | None:
        for index in range(window_size - 1, -1, -1):
            if timeline_entries[index][1] not in protected_paths:
                return index
        return None

    @staticmethod
    def _count_paths(timeline_entries: list[tuple[int, str]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for _, path in timeline_entries:
            counts[path] = counts.get(path, 0) + 1
        return counts
