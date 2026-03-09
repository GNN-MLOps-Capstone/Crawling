from __future__ import annotations

from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from app.core.config import settings
from app.api.recommend import get_recommend_service
from app.main import app
from app.repositories.news_repository import NewsCandidate
from app.services.context_builder import RecommendContextBuilder
from app.services.rerank_service import RerankService
from app.services.recommend_service import RecommendService
from app.services.retrieval_service import RetrievalService
from app.services.session_cache import InMemorySessionCache


class FakeNewsRepository:
    def __init__(self, news_ids: list[int]) -> None:
        self.news_ids = news_ids

    def fetch_latest_news_ids(self, *, limit: int, offset: int) -> list[int]:
        return self.news_ids[offset : offset + limit]

    def fetch_recent_candidates(
        self,
        *,
        limit: int,
        hours: int,
        exclude_ids: set[int],
        blocked_domains: tuple[str, ...] = (),
        ) -> list[NewsCandidate]:
        del hours, blocked_domains
        candidates: list[NewsCandidate] = []
        now = datetime.utcnow()
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


class PathBOnlyRepository(FakeNewsRepository):
    def fetch_recent_candidates(
        self,
        *,
        limit: int,
        hours: int,
        exclude_ids: set[int],
        blocked_domains: tuple[str, ...] = (),
    ) -> list[NewsCandidate]:
        if hours > 2:
            raise RuntimeError("path a failed")
        return super().fetch_recent_candidates(
            limit=limit,
            hours=hours,
            exclude_ids=exclude_ids,
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
            path_a_hours=72,
            path_b_hours=2,
            path_a_limit=50,
            path_b_limit=30,
        ),
        rerank_service=RerankService(),
    )
    app.dependency_overrides[get_recommend_service] = lambda: service
    return TestClient(app)


def test_request_validation_limit_required() -> None:
    client = _client_with_ids([100, 99, 98])
    response = client.post("/recommend/news", json={"user_id": "u1"})
    assert response.status_code == 422


def test_cursor_limit_mismatch_returns_400() -> None:
    client = _client_with_ids([100, 99, 98, 97])

    first = client.post(
        "/recommend/news",
        json={"user_id": "u1", "limit": 2, "request_id": "req-1"},
    )
    assert first.status_code == 200
    cursor = first.json()["next_cursor"]
    assert cursor is not None

    second = client.post(
        "/recommend/news",
        json={"user_id": "u1", "limit": 3, "cursor": cursor},
    )
    assert second.status_code == 400
    detail = second.json()["detail"]["error"]
    assert detail["code"] == "INVALID_CURSOR"


def test_pagination_consistency_with_cursor() -> None:
    client = _client_with_ids([10, 9, 8, 7, 6])

    first = client.post("/recommend/news", json={"user_id": "u1", "limit": 2})
    assert first.status_code == 200
    body1 = first.json()
    assert [item["news_id"] for item in body1["items"]] == [10, 9]
    assert body1["next_cursor"] is not None

    second = client.post(
        "/recommend/news",
        json={"user_id": "u1", "limit": 2, "cursor": body1["next_cursor"]},
    )
    assert second.status_code == 200
    body2 = second.json()
    assert [item["news_id"] for item in body2["items"]] == [8, 7]
    assert body2["request_id"] == body1["request_id"]


def test_response_contains_phase1a_meta_fields() -> None:
    client = _client_with_ids([21, 20, 19, 18])

    response = client.post("/recommend/news", json={"user_id": "u1", "limit": 2, "context": {}})

    assert response.status_code == 200
    meta = response.json()["meta"]
    assert meta["source"] == "phase1a_hybrid"
    assert meta["fallback_used"] is False
    assert meta["fallback_reason"] is None


def test_path_a_failure_falls_back_to_path_b() -> None:
    client = _client_with_repository(PathBOnlyRepository([21, 20, 19, 18]))

    response = client.post("/recommend/news", json={"user_id": "u1", "limit": 2, "context": {}})

    assert response.status_code == 200
    body = response.json()
    assert [item["news_id"] for item in body["items"]] == [21, 20]
    assert body["meta"]["fallback_used"] is True
    assert body["meta"]["fallback_reason"] == "path_a_failed_use_path_b"


def test_gemini_provider_without_api_key_falls_back() -> None:
    original_provider = settings.rerank_provider
    original_api_key = settings.gemini_api_key
    object.__setattr__(settings, "rerank_provider", "gemini")
    object.__setattr__(settings, "gemini_api_key", None)
    try:
        service = RerankService()
        result = service.rerank(
            context=RecommendContextBuilder().build({}),
            candidates=[
                NewsCandidate(
                    news_id=1,
                    title="title-1",
                    pub_date=datetime.utcnow(),
                    url="https://example.com/1",
                    category="unknown",
                    domain="example.com",
                )
            ],
        )
        assert result.fallback_used is True
        assert result.fallback_reason == "llm_rerank_failed"
    finally:
        object.__setattr__(settings, "rerank_provider", original_provider)
        object.__setattr__(settings, "gemini_api_key", original_api_key)
