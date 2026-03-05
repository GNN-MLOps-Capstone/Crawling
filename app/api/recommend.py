from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.repositories.news_repository import NewsRepository
from app.schemas.recommend import RecommendNewsRequest, RecommendNewsResponse
from app.services.cursor_service import CursorError
from app.services.recommend_service import RecommendService

router = APIRouter()


def get_recommend_service() -> RecommendService:
    return RecommendService(repository=NewsRepository())


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
