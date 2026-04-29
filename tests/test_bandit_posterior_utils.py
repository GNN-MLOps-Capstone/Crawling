from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dags.modules.serving.bandit_posterior import (
    PosteriorRecord,
    build_bandit_state_key,
    build_bandit_state_payload,
    rows_to_global_posteriors,
    rows_to_user_posteriors,
)


def test_build_bandit_state_key_formats_global_and_user_keys() -> None:
    assert build_bandit_state_key(
        key_prefix="recommend:bandit",
        scope="global",
        path="onboarding",
    ) == "recommend:bandit:global:onboarding"
    assert build_bandit_state_key(
        key_prefix="recommend:bandit",
        scope="user",
        path="behavior",
        user_id=42,
    ) == "recommend:bandit:user:42:behavior"


def test_rows_to_global_posteriors_adds_prior_to_alpha_beta() -> None:
    records = rows_to_global_posteriors(
        [("A1", 7, 10), ("B", 1, 5)],
        window_start="2026-03-12T00:00:00+00:00",
        window_end="2026-03-12T01:00:00+00:00",
    )

    assert records == [
            PosteriorRecord(
                scope="global",
                path="onboarding",
                alpha=7.0,
                beta=3.0,
                window_start="2026-03-12T00:00:00+00:00",
                window_end="2026-03-12T01:00:00+00:00",
                reward_count=7,
            impression_count=10,
        ),
            PosteriorRecord(
                scope="global",
                path="breaking",
                alpha=1.0,
                beta=4.0,
                window_start="2026-03-12T00:00:00+00:00",
                window_end="2026-03-12T01:00:00+00:00",
                reward_count=1,
            impression_count=5,
        ),
    ]


def test_rows_to_user_posteriors_uses_reward_and_failure_counts() -> None:
    records = rows_to_user_posteriors(
        [(7, "A2", 3, 8), (7, "C", 1, 2)],
        window_start="2026-03-12T00:00:00+00:00",
        window_end="2026-03-12T01:00:00+00:00",
    )

    assert records == [
        PosteriorRecord(
            scope="user",
            user_id=7,
            path="behavior",
            alpha=3.0,
            beta=5.0,
            window_start="2026-03-12T00:00:00+00:00",
            window_end="2026-03-12T01:00:00+00:00",
            reward_count=3,
            impression_count=8,
        ),
        PosteriorRecord(
            scope="user",
            user_id=7,
            path="popular",
            alpha=1.0,
            beta=1.0,
            window_start="2026-03-12T00:00:00+00:00",
            window_end="2026-03-12T01:00:00+00:00",
            reward_count=1,
            impression_count=2,
        ),
    ]


def test_build_bandit_state_payload_includes_metadata() -> None:
    payload = build_bandit_state_payload(
        PosteriorRecord(
            scope="global",
            path="onboarding",
            alpha=4,
            beta=5,
            window_start="2026-03-12T00:00:00+00:00",
            window_end="2026-03-12T01:00:00+00:00",
            reward_count=2,
            impression_count=5,
        )
    )

    assert '"alpha": 4' in payload
    assert '"beta": 5' in payload
    assert '"path": "onboarding"' in payload
