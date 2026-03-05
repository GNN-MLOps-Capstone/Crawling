from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from app.core.config import settings
from app.repositories.news_repository import NewsRepository
from app.schemas.recommend import (
    RecommendNewsItem,
    RecommendNewsMeta,
    RecommendNewsRequest,
    RecommendNewsResponse,
)
from app.services.cursor_service import CursorError, CursorPayload, decode_cursor, encode_cursor

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecommendService:
    repository: NewsRepository

    def recommend_news(self, request: RecommendNewsRequest) -> RecommendNewsResponse:
        request_id = request.request_id or str(uuid.uuid4())
        offset = 0

        if request.cursor:
            payload = decode_cursor(request.cursor)
            self._validate_cursor(payload=payload, request=request)
            request_id = payload.request_id
            offset = payload.offset

        # One extra row to detect whether next page exists.
        rows = self.repository.fetch_latest_news_ids(limit=request.limit + 1, offset=offset)
        has_more = len(rows) > request.limit
        selected_rows = rows[: request.limit]

        next_cursor = None
        if has_more:
            next_cursor = encode_cursor(
                CursorPayload(
                    v=settings.default_cursor_version,
                    limit=request.limit,
                    offset=offset + request.limit,
                    request_id=request_id,
                )
            )

        logger.info(
            "recommend_news user_id=%s request_id=%s limit=%s cursor_offset=%s items=%s has_more=%s",
            request.user_id,
            request_id,
            request.limit,
            offset,
            len(selected_rows),
            has_more,
        )

        return RecommendNewsResponse(
            request_id=request_id,
            items=[RecommendNewsItem(news_id=news_id) for news_id in selected_rows],
            next_cursor=next_cursor,
            meta=RecommendNewsMeta(source="mock_latest", fallback_used=False),
        )

    @staticmethod
    def _validate_cursor(*, payload: CursorPayload, request: RecommendNewsRequest) -> None:
        if payload.v != settings.default_cursor_version:
            raise CursorError("Unsupported cursor version")
        if payload.limit != request.limit:
            raise CursorError("cursor.limit must equal request.limit")
        if request.request_id and request.request_id != payload.request_id:
            raise CursorError("request_id does not match cursor.request_id")
