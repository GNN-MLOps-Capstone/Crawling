from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from app.api.recommend import get_recommend_service
from app.main import app
from app.repositories.news_repository import NewsCandidate, NewsRepository
from app.schemas.recommend import RecommendNewsRequest
import app.services.recommend_service as recommend_service_module
from app.services.context_builder import RecommendContextBuilder
from app.services.bandit_service import BanditArm, BanditPosterior, BanditService, NullBanditStateStore
from app.services.recommend_service import RecommendService
from app.services.retrieval_service import RetrievalService
from app.services.session_cache import InMemorySessionCache, RedisSessionCache


class FakeNewsRepository:
    def __init__(
        self,
        news_ids: list[int],
        *,
        profiles_by_user_id: dict[int, dict[str, list[str]]] | None = None,
        actions_by_user_id: dict[int, list[dict[str, object]]] | None = None,
    ) -> None:
        self.news_ids = news_ids
        self.profiles_by_user_id = profiles_by_user_id or {}
        self.actions_by_user_id = actions_by_user_id or {}
        self.news_entities_by_id: dict[int, dict[str, list[str]]] = {}
        self.entity_embeddings: dict[tuple[str, str], list[float]] = {}
        self.fetch_recent_candidates_calls: list[dict[str, object]] = []
        self.popular_candidates: list[NewsCandidate] = []

    def fetch_user_onboarding_profile(self, *, user_id: int) -> dict[str, list[str]]:
        return self.profiles_by_user_id.get(user_id, {})

    def fetch_recent_actions(
        self,
        *,
        user_id: int,
        limit: int,
        dwell_threshold_seconds: int,
    ) -> list[dict[str, object]]:
        del dwell_threshold_seconds
        return self.actions_by_user_id.get(user_id, [])[:limit]

    def fetch_news_entities(self, *, news_ids: list[int]) -> dict[int, dict[str, list[str]]]:
        return {
            news_id: self.news_entities_by_id.get(news_id, {"keyword_ids": [], "stock_ids": []})
            for news_id in news_ids
        }

    def fetch_entity_embeddings(
        self,
        *,
        entity_refs: list[tuple[str, str]],
    ) -> dict[tuple[str, str], list[float]]:
        return {
            entity_ref: self.entity_embeddings[entity_ref]
            for entity_ref in entity_refs
            if entity_ref in self.entity_embeddings
        }

    def fetch_latest_news_ids(self, *, limit: int, offset: int) -> list[int]:
        return self.news_ids[offset : offset + limit]

    def fetch_recent_candidates(
        self,
        *,
        limit: int,
        hours: int,
        exclude_ids: set[int],
        stale_cutoff_minutes: int | None = None,
        blocked_domains: tuple[str, ...] = (),
    ) -> list[NewsCandidate]:
        self.fetch_recent_candidates_calls.append(
            {
                "limit": limit,
                "hours": hours,
                "exclude_ids": set(exclude_ids),
                "stale_cutoff_minutes": stale_cutoff_minutes,
                "blocked_domains": blocked_domains,
            }
        )
        del blocked_domains, stale_cutoff_minutes
        if limit <= 0:
            return []
        candidates: list[NewsCandidate] = []
        now = datetime.now(UTC).replace(tzinfo=None)
        for index, news_id in enumerate(self.news_ids):
            if news_id in exclude_ids:
                continue
            candidates.append(
                NewsCandidate(
                    news_id=news_id,
                    title=f"title-{news_id}",
                    pub_date=now - timedelta(minutes=index),
                    url=f"https://example.com/{news_id}",
                    category="unknown",
                    domain="example.com",
                )
            )
            if len(candidates) >= limit:
                break
        return candidates

    def fetch_popular_candidates(
        self,
        *,
        limit: int,
        exclude_ids: set[int],
        blocked_domains: tuple[str, ...] = (),
    ) -> list[NewsCandidate]:
        del blocked_domains
        candidates = [candidate for candidate in self.popular_candidates if candidate.news_id not in exclude_ids]
        return candidates[:limit]

class PathBOnlyRepository(FakeNewsRepository):
    def fetch_recent_candidates(
        self,
        *,
        limit: int,
        hours: int,
        exclude_ids: set[int],
        stale_cutoff_minutes: int | None = None,
        blocked_domains: tuple[str, ...] = (),
    ) -> list[NewsCandidate]:
        del stale_cutoff_minutes
        if hours > 2:
            raise RuntimeError("primary path failed")
        return super().fetch_recent_candidates(
            limit=limit,
            hours=hours,
            exclude_ids=exclude_ids,
            stale_cutoff_minutes=None,
            blocked_domains=blocked_domains,
        )


class FakeRedisClient:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        del ttl_seconds
        self._store[key] = value


def _client_with_ids(news_ids: list[int]) -> TestClient:
    repository = FakeNewsRepository(news_ids)
    return _client_with_repository(repository)


def _client_with_repository(repository: FakeNewsRepository) -> TestClient:
    bandit_service = BanditService(
        arms=(
            BanditArm(path="onboarding", weight=2),
            BanditArm(path="behavior", weight=2),
            BanditArm(path="breaking", weight=1),
            BanditArm(path="popular", weight=1),
        ),
        first_page_window=recommend_service_module.settings.guardrail_first_page_window,
        min_breaking_in_window=recommend_service_module.settings.guardrail_min_breaking_in_window,
    )
    service = RecommendService(
        repository=repository,
        session_cache=InMemorySessionCache(),
        context_builder=RecommendContextBuilder(),
        retrieval_service=RetrievalService(
            repository=repository,
            base_pool_hours=72,
            onboarding_hours=72,
            behavior_hours=72,
            breaking_hours=2,
            breaking_stale_cutoff_minutes=120,
            onboarding_limit=50,
            behavior_limit=50,
            breaking_limit=30,
            popular_limit=30,
        ),
        bandit_service=bandit_service,
    )
    app.dependency_overrides[get_recommend_service] = lambda: service
    return TestClient(app)


def test_request_validation_limit_required() -> None:
    client = _client_with_ids([100, 99, 98])
    response = client.post("/recommend/news", json={"user_id": 1})
    assert response.status_code == 422


def test_cursor_limit_mismatch_returns_400() -> None:
    client = _client_with_ids([100, 99, 98, 97])

    first = client.post(
        "/recommend/news",
        json={"user_id": 1, "limit": 2, "request_id": "req-1"},
    )
    assert first.status_code == 200
    cursor = first.json()["next_cursor"]
    assert cursor is not None

    second = client.post(
        "/recommend/news",
        json={"user_id": 1, "limit": 3, "cursor": cursor},
    )
    assert second.status_code == 400
    detail = second.json()["detail"]["error"]
    assert detail["code"] == "INVALID_CURSOR"


def test_pagination_consistency_with_cursor() -> None:
    client = _client_with_ids([10, 9, 8, 7, 6])

    first = client.post("/recommend/news", json={"user_id": 1, "limit": 2})
    assert first.status_code == 200
    body1 = first.json()
    assert [item["news_id"] for item in body1["items"]] == [10, 9]
    assert [item["path"] for item in body1["items"]] == ["A1", "A1"]
    assert body1["next_cursor"] is not None

    second = client.post(
        "/recommend/news",
        json={"user_id": 1, "limit": 2, "cursor": body1["next_cursor"]},
    )
    assert second.status_code == 200
    body2 = second.json()
    assert body2["request_id"] == body1["request_id"]
    assert len(body2["items"]) == 2
    assert not ({item["news_id"] for item in body1["items"]} & {item["news_id"] for item in body2["items"]})
    assert {item["path"] for item in body2["items"]} <= {"A1", "B"}
    assert body2["request_id"] == body1["request_id"]


def test_cursor_pagination_keeps_paths_with_redis_session_cache() -> None:
    repository = FakeNewsRepository(list(range(60, 0, -1)))
    service = RecommendService(
        repository=repository,
        session_cache=RedisSessionCache(FakeRedisClient()),
        context_builder=RecommendContextBuilder(),
        retrieval_service=RetrievalService(
            repository=repository,
            base_pool_hours=72,
            onboarding_hours=72,
            behavior_hours=72,
            breaking_hours=2,
            breaking_stale_cutoff_minutes=120,
            onboarding_limit=50,
            behavior_limit=50,
            breaking_limit=30,
            popular_limit=30,
        ),
        bandit_service=BanditService(
            arms=(
                BanditArm(path="onboarding", weight=2),
                BanditArm(path="behavior", weight=2),
                BanditArm(path="breaking", weight=1),
                BanditArm(path="popular", weight=1),
            ),
            first_page_window=recommend_service_module.settings.guardrail_first_page_window,
            min_breaking_in_window=recommend_service_module.settings.guardrail_min_breaking_in_window,
        ),
    )

    first = service.recommend_news(
        RecommendNewsRequest(user_id=1, limit=20, request_id="redis-1", context={})
    )
    assert first.next_cursor is not None
    assert {item.path for item in first.items} <= {"A1", "B"}

    second = service.recommend_news(
        RecommendNewsRequest(user_id=1, limit=20, cursor=first.next_cursor, context={})
    )

    assert len(second.items) == 20
    assert {item.path for item in second.items} <= {"A1", "B"}


def test_response_contains_multipath_meta_fields() -> None:
    client = _client_with_ids([21, 20, 19, 18])

    response = client.post("/recommend/news", json={"user_id": 1, "limit": 2, "context": {}})

    assert response.status_code == 200
    meta = response.json()["meta"]
    assert meta["source"] == "multipath_cold"
    assert meta["fallback_used"] is False
    assert meta["fallback_reason"] == "profile_missing"


def test_response_items_include_public_path_codes() -> None:
    client = _client_with_ids([21, 20, 19, 18])

    response = client.post("/recommend/news", json={"user_id": 1, "limit": 2, "context": {}})

    assert response.status_code == 200
    assert response.json()["items"] == [
        {"news_id": 21, "path": "A1"},
        {"news_id": 20, "path": "A1"},
    ]


def test_profile_missing_fallback_reason_when_internal_signals_are_empty() -> None:
    client = _client_with_repository(FakeNewsRepository([21, 20, 19, 18]))

    response = client.post("/recommend/news", json={"user_id": 1, "limit": 2})

    assert response.status_code == 200
    meta = response.json()["meta"]
    assert meta["source"] == "multipath_cold"
    assert meta["fallback_reason"] == "profile_missing"


def test_primary_failure_falls_back_to_breaking() -> None:
    client = _client_with_repository(PathBOnlyRepository([21, 20, 19, 18]))

    response = client.post("/recommend/news", json={"user_id": 1, "limit": 2, "context": {}})

    assert response.status_code == 200
    body = response.json()
    assert [item["news_id"] for item in body["items"]] == [21, 20]
    assert body["meta"]["fallback_used"] is True
    assert body["meta"]["fallback_reason"] == "primary_failed_use_exploration"


def test_behavior_path_activates_when_recent_actions_exist() -> None:
    client = _client_with_repository(
        FakeNewsRepository(
            [30, 29, 28, 27, 26, 25],
            profiles_by_user_id={1: {"stock_ids": ["005930"]}},
            actions_by_user_id={
                1: [
                    {"news_id": 999, "timestamp": "2026-03-09T00:00:00+00:00", "keyword_ids": ["11"]},
                    {"news_id": 998, "timestamp": "2026-03-09T01:00:00+00:00", "stock_ids": ["005930"]},
                ]
            },
        )
    )

    response = client.post(
        "/recommend/news",
        json={
            "user_id": 1,
            "limit": 4,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert [item["news_id"] for item in body["items"]] == [30, 29, 28, 27]
    assert body["meta"]["source"] == "multipath_warm"


def test_single_recent_action_is_not_enough_for_warm_behavior_path() -> None:
    client = _client_with_repository(
        FakeNewsRepository(
            [30, 29, 28, 27, 26, 25],
            actions_by_user_id={1: [{"news_id": 999, "timestamp": "2026-03-09T00:00:00+00:00", "keyword_ids": ["11"]}]},
        )
    )

    response = client.post(
        "/recommend/news",
        json={
            "user_id": 1,
            "limit": 4,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["source"] == "multipath_cold"
    assert body["meta"]["fallback_reason"] == "behavior_insufficient"


def test_cold_user_replenishes_breaking_pool_when_enabled(monkeypatch) -> None:
    import app.services.retrieval_service as retrieval_service_module

    monkeypatch.setattr(
        retrieval_service_module,
        "settings",
        replace(retrieval_service_module.settings, breaking_replenish_min_count=1),
    )
    client = _client_with_repository(FakeNewsRepository([30, 29, 28, 27, 26, 25]))

    response = client.post("/recommend/news", json={"user_id": 1, "limit": 4})

    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["source"] == "multipath_cold"
    assert body["meta"]["fallback_reason"] == "profile_missing"
    assert any(item["path"] == "B" for item in body["items"])


def test_context_override_still_works_for_debug_requests() -> None:
    client = _client_with_ids([30, 29, 28, 27, 26, 25])

    response = client.post(
        "/recommend/news",
        json={
            "user_id": 1,
            "limit": 4,
            "context": {
                "profile": {"interests": ["stocks"]},
                "recent_actions": [
                    {"news_id": 999, "timestamp": "2026-03-09T00:00:00+00:00", "keyword_ids": ["11"]},
                    {"news_id": 998, "timestamp": "2026-03-09T01:00:00+00:00", "stock_ids": ["005930"]},
                ],
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert [item["news_id"] for item in body["items"]] == [30, 29, 28, 27]
    assert body["meta"]["source"] == "multipath_warm"


def test_context_schema_rejects_unknown_fields() -> None:
    client = _client_with_ids([30, 29, 28, 27])

    response = client.post(
        "/recommend/news",
        json={
            "user_id": 1,
            "limit": 2,
            "context": {"unexpected": True},
        },
    )

    assert response.status_code == 422


def test_onboarding_personalized_scoring_reorders_candidates() -> None:
    repository = FakeNewsRepository(
        [101, 102, 103],
        profiles_by_user_id={1: {"stock_ids": ["005930"], "keyword_ids": ["7"]}},
    )
    repository.news_entities_by_id = {
        101: {"stock_ids": ["000660"], "keyword_ids": ["8"]},
        102: {"stock_ids": ["005930"], "keyword_ids": ["7"]},
        103: {"stock_ids": ["000270"], "keyword_ids": ["9"]},
    }
    repository.entity_embeddings = {
        ("stock", "005930"): [1.0, 0.0],
        ("keyword", "7"): [0.0, 1.0],
        ("stock", "000660"): [0.7, 0.3],
        ("keyword", "8"): [0.2, 0.5],
        ("stock", "000270"): [0.0, 1.0],
        ("keyword", "9"): [0.1, 0.2],
    }
    client = _client_with_repository(repository)

    response = client.post("/recommend/news", json={"user_id": 1, "limit": 3})

    assert response.status_code == 200
    assert [item["news_id"] for item in response.json()["items"]] == [102, 101, 103]


def test_behavior_personalized_scoring_uses_recent_action_entities() -> None:
    repository = FakeNewsRepository(
        [201, 202, 203],
        actions_by_user_id={
            1: [
                {
                    "news_id": 9001,
                    "timestamp": "2026-03-09T00:00:00+00:00",
                    "keyword_ids": ["11"],
                    "stock_ids": ["035420"],
                },
                {
                    "news_id": 9002,
                    "timestamp": "2026-03-09T01:00:00+00:00",
                    "keyword_ids": ["11"],
                    "stock_ids": ["035420"],
                },
            ]
        },
    )
    repository.news_entities_by_id = {
        201: {"stock_ids": ["005930"], "keyword_ids": ["12"]},
        202: {"stock_ids": ["035420"], "keyword_ids": ["11"]},
        203: {"stock_ids": ["000660"], "keyword_ids": ["13"]},
    }
    repository.entity_embeddings = {
        ("keyword", "11"): [0.0, 1.0],
        ("stock", "035420"): [1.0, 0.0],
        ("keyword", "12"): [0.1, 0.1],
        ("stock", "005930"): [0.1, 0.2],
        ("keyword", "13"): [-1.0, 0.0],
        ("stock", "000660"): [0.0, -1.0],
    }
    retrieval_service = RetrievalService(
        repository=repository,
        base_pool_hours=72,
        onboarding_hours=72,
        behavior_hours=72,
        breaking_hours=2,
        breaking_stale_cutoff_minutes=120,
        onboarding_limit=50,
        behavior_limit=50,
        breaking_limit=30,
        popular_limit=30,
    )
    context = RecommendContextBuilder().build(
        user_id=1,
        raw_context={},
        repository=repository,
    )
    behavior_pool = repository.fetch_recent_candidates(
        limit=3,
        hours=72,
        exclude_ids=set(),
    )
    ranked, insufficient = retrieval_service._score_behavior_candidates(
        candidates=behavior_pool,
        context=context,
        limit=3,
    )

    assert insufficient is False
    assert [item.news_id for item in ranked] == [202, 201]


def test_a1_a2_share_three_day_base_pool_before_scoring() -> None:
    repository = FakeNewsRepository(
        [301, 302, 303, 304],
        profiles_by_user_id={1: {"stock_ids": ["005930"]}},
        actions_by_user_id={
            1: [{"news_id": 999, "timestamp": "2026-03-09T01:00:00+00:00", "keyword_ids": ["11"], "stock_ids": []}]
        },
    )
    repository.news_entities_by_id = {
        301: {"stock_ids": ["005930"], "keyword_ids": []},
        302: {"stock_ids": [], "keyword_ids": ["11"]},
        303: {"stock_ids": [], "keyword_ids": []},
        304: {"stock_ids": [], "keyword_ids": []},
    }
    repository.entity_embeddings = {
        ("stock", "005930"): [1.0, 0.0],
        ("keyword", "11"): [0.0, 1.0],
    }
    retrieval_service = RetrievalService(
        repository=repository,
        base_pool_hours=72,
        onboarding_hours=72,
        behavior_hours=72,
        breaking_hours=2,
        breaking_stale_cutoff_minutes=120,
        onboarding_limit=2,
        behavior_limit=2,
        breaking_limit=1,
        popular_limit=2,
    )
    context = RecommendContextBuilder().build(
        user_id=1,
        raw_context={},
        repository=repository,
    )

    retrieval_service.retrieve(context=context, exclude_ids=set())

    assert len(repository.fetch_recent_candidates_calls) == 2
    assert repository.fetch_recent_candidates_calls[0]["hours"] == 72
    assert repository.fetch_recent_candidates_calls[0]["limit"] == 8
    assert repository.fetch_recent_candidates_calls[1]["hours"] == 2


def test_replayed_cursor_returns_same_page() -> None:
    client = _client_with_ids([10, 9, 8, 7, 6])

    first = client.post("/recommend/news", json={"user_id": 1, "limit": 2})
    assert first.status_code == 200
    cursor = first.json()["next_cursor"]
    assert cursor is not None

    second = client.post(
        "/recommend/news",
        json={"user_id": 1, "limit": 2, "cursor": cursor},
    )
    assert second.status_code == 200

    replay = client.post(
        "/recommend/news",
        json={"user_id": 1, "limit": 2, "cursor": cursor},
    )
    assert replay.status_code == 200
    assert replay.json()["request_id"] == second.json()["request_id"]
    assert replay.json()["items"] == second.json()["items"]
    assert replay.json()["next_cursor"] == second.json()["next_cursor"]


def test_prefetch_rolls_over_only_after_current_batch_is_consumed() -> None:
    repository = FakeNewsRepository(list(range(120, 0, -1)))
    service = RecommendService(
        repository=repository,
        session_cache=InMemorySessionCache(),
        context_builder=RecommendContextBuilder(),
        retrieval_service=RetrievalService(
            repository=repository,
            base_pool_hours=72,
            onboarding_hours=72,
            behavior_hours=72,
            breaking_hours=2,
            breaking_stale_cutoff_minutes=120,
            onboarding_limit=50,
            behavior_limit=50,
            breaking_limit=30,
            popular_limit=30,
        ),
        bandit_service=BanditService(
            arms=(
                BanditArm(path="onboarding", weight=2),
                BanditArm(path="behavior", weight=2),
                BanditArm(path="breaking", weight=1),
                BanditArm(path="popular", weight=1),
            ),
            first_page_window=recommend_service_module.settings.guardrail_first_page_window,
            min_breaking_in_window=recommend_service_module.settings.guardrail_min_breaking_in_window,
        ),
    )

    first = service.recommend_news(
        RecommendNewsRequest(user_id=1, limit=40, request_id="r1", context={})
    )
    assert first.next_cursor is not None
    session = service.session_cache.get("r1")
    assert session is not None
    assert len(session.timeline_ids) == 80

    second = service.recommend_news(
        RecommendNewsRequest(user_id=1, limit=40, cursor=first.next_cursor, context={})
    )
    assert second.next_cursor is not None
    session = service.session_cache.get("r1")
    assert session is not None
    assert session.prefetched_timeline_ids

    third = service.recommend_news(
        RecommendNewsRequest(user_id=1, limit=40, cursor=second.next_cursor, context={})
    )
    assert len(third.items) == 40


def test_stale_session_is_restored_for_cursor_pagination() -> None:
    repository = FakeNewsRepository(list(range(20, 0, -1)))
    service = RecommendService(
        repository=repository,
        session_cache=InMemorySessionCache(),
        context_builder=RecommendContextBuilder(),
        retrieval_service=RetrievalService(
            repository=repository,
            base_pool_hours=72,
            onboarding_hours=72,
            behavior_hours=72,
            breaking_hours=2,
            breaking_stale_cutoff_minutes=120,
            onboarding_limit=10,
            behavior_limit=10,
            breaking_limit=5,
            popular_limit=5,
        ),
        bandit_service=BanditService(
            arms=(
                BanditArm(path="onboarding", weight=2),
                BanditArm(path="behavior", weight=2),
                BanditArm(path="breaking", weight=1),
                BanditArm(path="popular", weight=1),
            ),
            first_page_window=recommend_service_module.settings.guardrail_first_page_window,
            min_breaking_in_window=recommend_service_module.settings.guardrail_min_breaking_in_window,
        ),
    )

    first = service.recommend_news(
        RecommendNewsRequest(user_id=1, limit=3, request_id="stale-1", context={})
    )
    assert first.next_cursor is not None
    session = service.session_cache.get("stale-1")
    assert session is not None
    session.last_served_at = session.last_served_at - (recommend_service_module.settings.session_stale_after_seconds + 1)
    service.session_cache.set("stale-1", session, recommend_service_module.settings.session_cache_ttl_seconds)

    second = service.recommend_news(
        RecommendNewsRequest(user_id=1, limit=3, cursor=first.next_cursor, context={})
    )

    assert len(second.items) == 3
    assert second.meta.fallback_used is True
    assert second.meta.fallback_reason == "stale_session_restored"


def test_bandit_guardrail_promotes_breaking_into_first_window() -> None:
    entries = [
        (101, "onboarding"),
        (102, "onboarding"),
        (103, "behavior"),
        (104, "behavior"),
        (105, "onboarding"),
        (106, "behavior"),
        (107, "onboarding"),
        (108, "behavior"),
        (109, "onboarding"),
        (110, "behavior"),
        (201, "breaking"),
    ]

    bandit_service = BanditService(
        arms=(
            BanditArm(path="onboarding", weight=2),
            BanditArm(path="behavior", weight=2),
            BanditArm(path="breaking", weight=1),
            BanditArm(path="popular", weight=1),
        ),
        first_page_window=recommend_service_module.settings.guardrail_first_page_window,
        min_breaking_in_window=recommend_service_module.settings.guardrail_min_breaking_in_window,
    )
    guarded = bandit_service._apply_legacy_breaking_guardrail(entries)
    first_window_paths = [path for _, path in guarded[: recommend_service_module.settings.guardrail_first_page_window]]

    assert "breaking" in first_window_paths


def test_bandit_service_fixed_allocator_matches_legacy_mix_order() -> None:
    bandit_service = BanditService(
        arms=(
            BanditArm(path="onboarding", weight=2),
            BanditArm(path="behavior", weight=2),
            BanditArm(path="breaking", weight=1),
            BanditArm(path="popular", weight=1),
        ),
        first_page_window=10,
        min_breaking_in_window=0,
    )

    mix_plan = bandit_service.build_mix_plan(
        path_candidates={
            "onboarding": [101, 102, 103],
            "behavior": [201, 202],
            "breaking": [301, 302],
            "popular": [401],
        }
    )

    assert mix_plan.allocator == "fixed"
    assert mix_plan.mix_policy == {
        "onboarding": 2,
        "behavior": 2,
        "breaking": 1,
        "popular": 1,
    }
    assert mix_plan.timeline_entries == [
        (101, "onboarding"),
        (102, "onboarding"),
        (201, "behavior"),
        (202, "behavior"),
        (301, "breaking"),
        (401, "popular"),
        (103, "onboarding"),
        (302, "breaking"),
    ]


class FakeBanditStateStore(NullBanditStateStore):
    def __init__(
        self,
        *,
        global_posteriors: dict[str, BanditPosterior] | None = None,
        user_posteriors: dict[int, dict[str, BanditPosterior]] | None = None,
    ) -> None:
        self._global_posteriors = global_posteriors or {}
        self._user_posteriors = user_posteriors or {}

    def get_global_posteriors(self, *, paths: list[str]) -> dict[str, BanditPosterior]:
        return {
            path: self._global_posteriors.get(path, BanditPosterior())
            for path in paths
        }

    def get_user_posteriors(self, *, user_id: int, paths: list[str]) -> dict[str, BanditPosterior]:
        by_user = self._user_posteriors.get(user_id, {})
        return {
            path: by_user.get(path, BanditPosterior())
            for path in paths
        }


def test_bandit_service_thompson_uses_global_plus_user_posteriors(monkeypatch) -> None:
    bandit_service = BanditService(
        arms=(
            BanditArm(path="onboarding", weight=2),
            BanditArm(path="behavior", weight=2),
            BanditArm(path="breaking", weight=1),
            BanditArm(path="popular", weight=1),
        ),
        first_page_window=10,
        min_breaking_in_window=0,
        allocator="thompson",
        batch_size=8,
        min_per_arm=1,
        prior_alpha=2,
        prior_beta=2,
        state_store=FakeBanditStateStore(
            global_posteriors={
                "onboarding": BanditPosterior(alpha=1, beta=4),
                "behavior": BanditPosterior(alpha=2, beta=2),
                "breaking": BanditPosterior(alpha=1, beta=5),
                "popular": BanditPosterior(alpha=1, beta=6),
            },
            user_posteriors={
                7: {
                    "behavior": BanditPosterior(alpha=8, beta=1),
                }
            },
        ),
    )

    monkeypatch.setattr(
        BanditService,
        "_sample_beta",
        staticmethod(lambda *, alpha, beta, rng: alpha / (alpha + beta)),
    )

    mix_plan = bandit_service.build_mix_plan(
        path_candidates={
            "onboarding": [101, 102],
            "behavior": [201, 202, 203, 204],
            "breaking": [301],
            "popular": [401],
        },
        user_id=7,
    )

    assert mix_plan.allocator == "thompson"
    assert mix_plan.arm_scores["behavior"] > mix_plan.arm_scores["onboarding"]
    assert mix_plan.timeline_entries[0] == (201, "behavior")
    assert mix_plan.timeline_entries[1] == (101, "onboarding")
    assert {path for _, path in mix_plan.timeline_entries} == {"onboarding", "behavior", "breaking", "popular"}


def test_bandit_service_thompson_applies_minimum_per_path_guardrail(monkeypatch) -> None:
    bandit_service = BanditService(
        arms=(
            BanditArm(path="onboarding", weight=2),
            BanditArm(path="behavior", weight=2),
            BanditArm(path="breaking", weight=1),
            BanditArm(path="popular", weight=1),
        ),
        first_page_window=10,
        min_breaking_in_window=0,
        allocator="thompson",
        batch_size=8,
        min_per_arm=2,
        state_store=FakeBanditStateStore(),
    )

    monkeypatch.setattr(
        BanditService,
        "_sample_beta",
        staticmethod(
            lambda *, alpha, beta, rng: {
                (2.0, 2.0): 0.9,
            }.get((alpha, beta), 0.1)
        ),
    )

    mix_plan = bandit_service.build_mix_plan(
        path_candidates={
            "onboarding": [101, 102, 103, 104],
            "behavior": [201, 202, 203, 204],
            "breaking": [301, 302],
            "popular": [401, 402],
        },
        user_id=1,
    )

    first_window_paths = [path for _, path in mix_plan.timeline_entries[:8]]
    assert first_window_paths.count("onboarding") >= 2
    assert first_window_paths.count("behavior") >= 2
    assert first_window_paths.count("breaking") >= 2
    assert first_window_paths.count("popular") >= 2


def test_bandit_service_thompson_falls_back_to_fixed_on_state_error() -> None:
    class BrokenStore(NullBanditStateStore):
        def get_global_posteriors(self, *, paths: list[str]) -> dict[str, BanditPosterior]:
            del paths
            raise ValueError("bad payload")

    bandit_service = BanditService(
        arms=(
            BanditArm(path="onboarding", weight=2),
            BanditArm(path="behavior", weight=2),
            BanditArm(path="breaking", weight=1),
            BanditArm(path="popular", weight=1),
        ),
        first_page_window=10,
        min_breaking_in_window=0,
        allocator="thompson",
        state_store=BrokenStore(),
    )

    mix_plan = bandit_service.build_mix_plan(
        path_candidates={
            "onboarding": [101, 102, 103],
            "behavior": [201, 202],
            "breaking": [301],
            "popular": [401],
        },
        user_id=1,
    )

    assert mix_plan.allocator == "fixed"
    assert mix_plan.fallback_reason == "bandit_allocator_failed"


def test_popular_candidates_are_served_as_path_c() -> None:
    repository = FakeNewsRepository([21, 20, 19, 18])
    repository.popular_candidates = [
        NewsCandidate(
            news_id=501,
            title="popular-501",
            pub_date=datetime.now(UTC).replace(tzinfo=None),
            url="https://example.com/501",
            category="unknown",
            domain="example.com",
        ),
        NewsCandidate(
            news_id=500,
            title="popular-500",
            pub_date=datetime.now(UTC).replace(tzinfo=None),
            url="https://example.com/500",
            category="unknown",
            domain="example.com",
        ),
    ]
    service = RecommendService(
        repository=repository,
        session_cache=InMemorySessionCache(),
        context_builder=RecommendContextBuilder(),
        retrieval_service=RetrievalService(
            repository=repository,
            base_pool_hours=72,
            onboarding_hours=72,
            behavior_hours=72,
            breaking_hours=2,
            breaking_stale_cutoff_minutes=120,
            onboarding_limit=0,
            behavior_limit=0,
            breaking_limit=0,
            popular_limit=2,
        ),
        bandit_service=BanditService(
            arms=(
                BanditArm(path="onboarding", weight=2),
                BanditArm(path="behavior", weight=2),
                BanditArm(path="breaking", weight=1),
                BanditArm(path="popular", weight=1),
            ),
            first_page_window=recommend_service_module.settings.guardrail_first_page_window,
            min_breaking_in_window=recommend_service_module.settings.guardrail_min_breaking_in_window,
        ),
    )

    response = service.recommend_news(RecommendNewsRequest(user_id=1, limit=2, context={}))

    assert [item.news_id for item in response.items] == [501, 500]
    assert [item.path for item in response.items] == ["C", "C"]


def test_recommendation_response_emits_impression_logs(monkeypatch) -> None:
    client = _client_with_ids([21, 20, 19, 18])
    captured = []

    monkeypatch.setattr(recommend_service_module, "log_impression", lambda payload: captured.append(payload))

    response = client.post("/recommend/news", json={"user_id": 1, "limit": 2})

    assert response.status_code == 200
    assert len(captured) == 2
    assert captured[0].news_id == 21
    assert captured[0].rank == 1
    assert captured[0].path == "onboarding"
