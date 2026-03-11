from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from app.api.recommend import get_recommend_service
from app.main import app
from app.repositories.news_repository import NewsCandidate, NewsRepository
from app.schemas.recommend import RecommendNewsRequest
import app.services.recommend_service as recommend_service_module
from app.services.context_builder import RecommendContextBuilder
from app.services.recommend_service import RecommendService
from app.services.retrieval_service import RetrievalService
from app.services.session_cache import InMemorySessionCache


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


def _client_with_ids(news_ids: list[int]) -> TestClient:
    repository = FakeNewsRepository(news_ids)
    return _client_with_repository(repository)


def _client_with_repository(repository: FakeNewsRepository) -> TestClient:
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
    assert [item["news_id"] for item in body2["items"]] == [8, 7]
    assert [item["path"] for item in body2["items"]] == ["A1", "A1"]
    assert body2["request_id"] == body1["request_id"]


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


def test_mix_guardrail_promotes_breaking_into_first_window() -> None:
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

    guarded = RecommendService._apply_mix_guardrails(entries)
    first_window_paths = [path for _, path in guarded[: recommend_service_module.settings.guardrail_first_page_window]]

    assert "breaking" in first_window_paths


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


def test_click_endpoint_logs_path_from_served_session(monkeypatch) -> None:
    client = _client_with_ids([21, 20, 19, 18])
    captured = []

    monkeypatch.setattr(recommend_service_module, "log_click", lambda payload: captured.append(payload))

    first = client.post("/recommend/news", json={"user_id": 1, "limit": 2})
    assert first.status_code == 200
    body = first.json()

    response = client.post(
        "/recommend/news/click",
        json={
            "request_id": body["request_id"],
            "user_id": 1,
            "news_id": body["items"][0]["news_id"],
        },
    )

    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}
    assert len(captured) == 1
    assert captured[0].news_id == body["items"][0]["news_id"]
    assert captured[0].rank == 1
    assert captured[0].path == "onboarding"
