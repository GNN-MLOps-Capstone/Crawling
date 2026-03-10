from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from app.core.config import settings
from app.schemas.recommend import RecommendDebugContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NormalizedRecommendContext:
    profile: dict[str, Any] = field(default_factory=dict)
    recent_actions: list[dict[str, Any]] = field(default_factory=list)
    session_signals: dict[str, Any] = field(default_factory=dict)
    seed: dict[str, Any] = field(default_factory=dict)
    signal_source: str = "internal_lookup"
    lookup_errors: tuple[str, ...] = ()

    @property
    def is_personalizable(self) -> bool:
        return bool(self.profile or self.recent_actions or self.session_signals)

    @property
    def has_onboarding_signals(self) -> bool:
        return bool(self.profile)

    @property
    def has_behavior_signals(self) -> bool:
        return bool(self.recent_actions or self.session_signals)

    @property
    def user_state(self) -> str:
        return "warm" if len(self.recent_actions) >= settings.behavior_min_recent_actions else "cold"


class RecommendContextBuilder:
    def build(
        self,
        *,
        user_id: int,
        raw_context: RecommendDebugContext | dict[str, Any] | None,
        repository: Any,
    ) -> NormalizedRecommendContext:
        if isinstance(raw_context, RecommendDebugContext):
            payload = raw_context.model_dump(exclude_none=True)
        else:
            payload = raw_context or {}
        profile: dict[str, Any] = {}
        recent_actions: list[dict[str, Any]] = []
        session_signals = payload.get("session_signals")
        lookup_errors: list[str] = []
        signal_source = "internal_lookup"

        try:
            profile = repository.fetch_user_onboarding_profile(user_id=user_id)
        except Exception as exc:
            logger.warning("failed to load onboarding profile for user_id=%s: %s", user_id, exc)
            lookup_errors.append("profile_lookup_failed")
        try:
            recent_actions = repository.fetch_recent_actions(
                user_id=user_id,
                limit=settings.behavior_action_limit,
                dwell_threshold_seconds=settings.valid_read_dwell_seconds,
            )
        except Exception as exc:
            logger.warning("failed to load recent actions for user_id=%s: %s", user_id, exc)
            lookup_errors.append("behavior_lookup_failed")

        override_profile = payload.get("profile")
        override_recent_actions = payload.get("recent_actions")

        if isinstance(override_profile, dict):
            profile = override_profile
            signal_source = "debug_override"
        if isinstance(override_recent_actions, list):
            recent_actions = override_recent_actions
            signal_source = "debug_override"

        return NormalizedRecommendContext(
            profile=profile if isinstance(profile, dict) else {},
            recent_actions=recent_actions if isinstance(recent_actions, list) else [],
            session_signals=session_signals if isinstance(session_signals, dict) else {},
            seed={
                "mode": "latest_seed",
                "base_pool_hours": 72,
                "onboarding_hours": 72,
                "behavior_hours": 72,
                "breaking_hours": 2,
            },
            signal_source=signal_source,
            lookup_errors=tuple(lookup_errors),
        )
