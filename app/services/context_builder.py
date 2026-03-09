from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class NormalizedRecommendContext:
    profile: dict[str, Any] = field(default_factory=dict)
    recent_actions: list[dict[str, Any]] = field(default_factory=list)
    session_signals: dict[str, Any] = field(default_factory=dict)
    seed: dict[str, Any] = field(default_factory=dict)
    prompt_defaults: dict[str, str] = field(default_factory=dict)

    @property
    def is_personalizable(self) -> bool:
        return bool(self.profile or self.recent_actions or self.session_signals)


class RecommendContextBuilder:
    def build(self, raw_context: dict[str, Any] | None) -> NormalizedRecommendContext:
        payload = raw_context or {}

        profile = payload.get("profile")
        recent_actions = payload.get("recent_actions")
        session_signals = payload.get("session_signals")

        return NormalizedRecommendContext(
            profile=profile if isinstance(profile, dict) else {},
            recent_actions=recent_actions if isinstance(recent_actions, list) else [],
            session_signals=session_signals if isinstance(session_signals, dict) else {},
            seed={
                "mode": "latest_seed",
                "base_pool_hours": 72,
                "path_a_hours": 72,
                "path_b_hours": 2,
            },
            prompt_defaults={
                "user_interests": "No explicit interests provided. Prefer broadly useful finance news.",
                "recent_reads": "No recent reading history provided.",
                "avoidances": "No avoidances provided.",
            },
        )
