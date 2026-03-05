from __future__ import annotations

from fastapi.testclient import TestClient

from app.api.recommend import get_recommend_service
from app.main import app
from app.services.recommend_service import RecommendService


class FakeNewsRepository:
    def __init__(self, news_ids: list[int]) -> None:
        self.news_ids = news_ids

    def fetch_latest_news_ids(self, *, limit: int, offset: int) -> list[int]:
        return self.news_ids[offset : offset + limit]


def _client_with_ids(news_ids: list[int]) -> TestClient:
    app.dependency_overrides[get_recommend_service] = lambda: RecommendService(
        repository=FakeNewsRepository(news_ids)
    )
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
