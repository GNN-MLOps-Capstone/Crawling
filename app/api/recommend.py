from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.config import settings
from app.repositories.news_repository import NewsRepository
from app.schemas.recommend import RecommendNewsRequest, RecommendNewsResponse
from app.services.context_builder import RecommendContextBuilder
from app.services.cursor_service import CursorError
from app.services.recommend_service import RecommendService
from app.services.retrieval_service import RetrievalService
from app.services.session_cache import build_session_cache

router = APIRouter()
_session_cache = build_session_cache(
    host=settings.redis_host,
    port=settings.redis_port,
    password=settings.redis_password,
)


def get_recommend_service() -> RecommendService:
    repository = NewsRepository()
    return RecommendService(
        repository=repository,
        session_cache=_session_cache,
        context_builder=RecommendContextBuilder(),
        retrieval_service=RetrievalService(
            repository=repository,
            base_pool_hours=settings.base_pool_hours,
            onboarding_hours=settings.onboarding_hours,
            behavior_hours=settings.behavior_hours,
            breaking_hours=settings.breaking_hours,
            breaking_stale_cutoff_minutes=settings.breaking_stale_cutoff_minutes,
            onboarding_limit=settings.onboarding_candidate_limit,
            behavior_limit=settings.behavior_candidate_limit,
            breaking_limit=settings.breaking_candidate_limit,
            blocked_domains=settings.blocked_domains,
        ),
    )


@router.post(
    "/recommend/news",
    response_model=RecommendNewsResponse,
    status_code=status.HTTP_200_OK,
)
def recommend_news(
    request: RecommendNewsRequest,
    service: RecommendService = Depends(get_recommend_service),
) -> RecommendNewsResponse:
    try:
        return service.recommend_news(request)
    except CursorError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": {
                    "code": "INVALID_CURSOR",
                    "message": str(exc),
                    "request_id": request.request_id,
                }
            },
        ) from exc
