from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from time import perf_counter

from app.core.config import settings
from app.repositories.news_repository import NewsRepository
from app.schemas.recommend import (
    RecommendNewsItem,
    RecommendNewsMeta,
    RecommendNewsRequest,
    RecommendNewsResponse,
)
from app.services.context_builder import RecommendContextBuilder
from app.services.cursor_service import CursorError, CursorPayload, decode_cursor, encode_cursor
from app.services.recommend_logging import RequestLog, context_hash, log_request, now_ts
from app.services.rerank_service import RerankService
from app.services.retrieval_service import RetrievalService
from app.services.session_cache import RecommendationSession, SessionCache

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecommendService:
    repository: NewsRepository
    session_cache: SessionCache
    context_builder: RecommendContextBuilder
    retrieval_service: RetrievalService
    rerank_service: RerankService

    def recommend_news(self, request: RecommendNewsRequest) -> RecommendNewsResponse:
        started_at = perf_counter()
        request_id = request.request_id or str(uuid.uuid4())
        offset = 0
        cache_status = "miss"

        if request.cursor:
            payload = decode_cursor(request.cursor)
            self._validate_cursor(payload=payload, request=request)
            request_id = payload.request_id
            offset = payload.offset

        session = self.session_cache.get(request_id)
        if session is None:
            session = self._build_session(request=request, request_id=request_id)
        elif session.limit != request.limit:
            raise CursorError("session.limit must equal request.limit")
        else:
            cache_status = "hit"

        self._maybe_prefetch(session=session)
        session = self._ensure_page_window(session=session, request=request, offset=offset)

        total = len(session.timeline_ids)
        selected_ids = session.timeline_ids[offset : offset + request.limit]
        served_until = min(offset + len(selected_ids), total)
        session.served_ids = session.timeline_ids[:served_until]
        session.touch()
        self.session_cache.set(request_id, session, settings.session_cache_ttl_seconds)

        next_cursor = None
        if served_until < total:
            next_cursor = encode_cursor(
                CursorPayload(
                    v=settings.default_cursor_version,
                    limit=request.limit,
                    offset=served_until,
                    request_id=request_id,
                )
            )

        logger.info(
            "recommend_news user_id=%s request_id=%s limit=%s cursor_offset=%s items=%s total=%s fallback_used=%s fallback_reason=%s context_present=%s prefetch_triggered=%s prefetch_status=%s batch_generation_id=%s",
            request.user_id,
            request_id,
            request.limit,
            offset,
            len(selected_ids),
            total,
            session.fallback_used,
            session.fallback_reason,
            bool(request.context),
            session.prefetch_triggered,
            session.prefetch_status,
            session.batch_generation_id,
        )
        self._log_request(
            request=request,
            session=session,
            offset=offset,
            cache_status=cache_status,
            latency_ms=int((perf_counter() - started_at) * 1000),
        )

        return RecommendNewsResponse(
            request_id=request_id,
            items=[RecommendNewsItem(news_id=news_id) for news_id in selected_ids],
            next_cursor=next_cursor,
            meta=RecommendNewsMeta(
                source=session.source,
                fallback_used=session.fallback_used,
                fallback_reason=session.fallback_reason,
            ),
        )

    def _build_session(self, *, request: RecommendNewsRequest, request_id: str) -> RecommendationSession:
        context = self.context_builder.build(request.context)
        retrieval = self.retrieval_service.retrieve(context=context, exclude_ids=set())
        reranked_a = self.rerank_service.rerank(context=context, candidates=retrieval.path_a)

        timeline_ids = self._mix_paths(
            path_a_ids=[item.news_id for item in reranked_a.candidates],
            path_b_ids=[item.news_id for item in retrieval.path_b],
        )
        source = "phase1a_hybrid"
        fallback_used = retrieval.fallback_used or reranked_a.fallback_used or not timeline_ids
        fallback_reason = retrieval.fallback_reason or reranked_a.fallback_reason

        if not timeline_ids:
            timeline_ids = self.repository.fetch_latest_news_ids(limit=request.limit, offset=0)
            source = "phase1a_latest_fallback"
            fallback_used = True
            fallback_reason = fallback_reason or "empty_result"

        session = RecommendationSession(
            request_id=request_id,
            user_id=request.user_id,
            limit=request.limit,
            timeline_ids=timeline_ids,
            path_a_queue=[item.news_id for item in reranked_a.candidates],
            path_b_queue=[item.news_id for item in retrieval.path_b],
            current_mix_policy={"path_a": settings.path_a_mix_weight, "path_b": settings.path_b_mix_weight},
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            source=source,
            cache_key=self._cache_key(request_id),
        )
        self.session_cache.set(request_id, session, settings.session_cache_ttl_seconds)
        return session

    def _maybe_prefetch(self, *, session: RecommendationSession) -> None:
        path_a_set = set(session.path_a_queue)
        remaining_path_a = max(
            len(session.path_a_queue) - len([news_id for news_id in session.served_ids if news_id in path_a_set]),
            0,
        )
        if session.prefetch_triggered or remaining_path_a > settings.prefetch_path_a_low_watermark:
            return

        session.prefetch_triggered = True
        session.prefetch_status = "triggered"
        context = self.context_builder.build({})
        retrieval = self.retrieval_service.retrieve(context=context, exclude_ids=set(session.timeline_ids))
        reranked_a = self.rerank_service.rerank(context=context, candidates=retrieval.path_a)
        appended = self._mix_paths(
            path_a_ids=[item.news_id for item in reranked_a.candidates],
            path_b_ids=[item.news_id for item in retrieval.path_b],
        )
        session.prefetched_timeline_ids = appended
        session.prefetch_status = "ready" if appended else "empty"
        session.timeline_ids.extend([news_id for news_id in appended if news_id not in set(session.timeline_ids)])
        session.path_a_queue.extend(
            [item.news_id for item in reranked_a.candidates if item.news_id not in set(session.path_a_queue)]
        )
        session.path_b_queue.extend(
            [item.news_id for item in retrieval.path_b if item.news_id not in set(session.path_b_queue)]
        )
        session.batch_generation_id += 1
        session.touch()

    def _ensure_page_window(
        self,
        *,
        session: RecommendationSession,
        request: RecommendNewsRequest,
        offset: int,
    ) -> RecommendationSession:
        if offset < len(session.timeline_ids):
            return session

        if session.prefetched_timeline_ids:
            session.timeline_ids.extend(
                [news_id for news_id in session.prefetched_timeline_ids if news_id not in set(session.timeline_ids)]
            )
            session.prefetched_timeline_ids = []
            session.prefetch_status = "rolled_over"
            return session

        if settings.prefetch_policy == "path_a_replenish_first":
            rebuilt = self._build_session(request=request, request_id=session.request_id)
            rebuilt.served_ids = session.served_ids[:]
            rebuilt.batch_generation_id = session.batch_generation_id + 1
            rebuilt.prefetch_status = "rebuilt"
            rebuilt.fallback_used = rebuilt.fallback_used or True
            rebuilt.fallback_reason = rebuilt.fallback_reason or "batch_rollover_rebuild"
            return rebuilt

        return session

    def _log_request(
        self,
        *,
        request: RecommendNewsRequest,
        session: RecommendationSession,
        offset: int,
        cache_status: str,
        latency_ms: int,
    ) -> None:
        path_a_set = set(session.path_a_queue)
        path_b_set = set(session.path_b_queue)
        served_set = set(session.served_ids)
        log_request(
            RequestLog(
                timestamp=now_ts(),
                request_id=session.request_id,
                session_id=session.request_id,
                user_id=request.user_id,
                limit=request.limit,
                offset=offset,
                latency_ms=latency_ms,
                cache_status=cache_status,
                context_hash=context_hash(request.context),
                context_present=bool(request.context),
                fallback_used=session.fallback_used,
                fallback_reason=session.fallback_reason,
                prefetch_triggered=session.prefetch_triggered,
                prefetch_status=session.prefetch_status,
                batch_generation_id=session.batch_generation_id,
                path_a_remaining=len([news_id for news_id in session.path_a_queue if news_id not in served_set]),
                path_b_remaining=len([news_id for news_id in session.path_b_queue if news_id not in served_set]),
                path_c_remaining=len([news_id for news_id in session.path_c_queue if news_id not in served_set]),
                mix_ratio=dict(session.current_mix_policy),
                source=session.source,
            )
        )

    @staticmethod
    def _cache_key(request_id: str) -> str:
        return f"recommend:session:{request_id}"

    @staticmethod
    def _mix_paths(*, path_a_ids: list[int], path_b_ids: list[int]) -> list[int]:
        mixed: list[int] = []
        seen: set[int] = set()
        index_a = 0
        index_b = 0

        while index_a < len(path_a_ids) or index_b < len(path_b_ids):
            for _ in range(settings.path_a_mix_weight):
                if index_a >= len(path_a_ids):
                    break
                news_id = path_a_ids[index_a]
                index_a += 1
                if news_id not in seen:
                    mixed.append(news_id)
                    seen.add(news_id)

            for _ in range(settings.path_b_mix_weight):
                if index_b >= len(path_b_ids):
                    break
                news_id = path_b_ids[index_b]
                index_b += 1
                if news_id not in seen:
                    mixed.append(news_id)
                    seen.add(news_id)

            if index_a >= len(path_a_ids) and index_b >= len(path_b_ids):
                break

        return mixed

    @staticmethod
    def _validate_cursor(*, payload: CursorPayload, request: RecommendNewsRequest) -> None:
        if payload.v != settings.default_cursor_version:
            raise CursorError("Unsupported cursor version")
        if payload.limit != request.limit:
            raise CursorError("cursor.limit must equal request.limit")
        if request.request_id and request.request_id != payload.request_id:
            raise CursorError("request_id does not match cursor.request_id")
