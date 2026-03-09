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
from app.services.retrieval_service import RetrievalService
from app.services.session_cache import RecommendationSession, SessionCache

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecommendService:
    repository: NewsRepository
    session_cache: SessionCache
    context_builder: RecommendContextBuilder
    retrieval_service: RetrievalService

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
        if session is None and request.cursor:
            raise CursorError("session not found for cursor")
        if session is None:
            session = self._build_session(request=request, request_id=request_id)
        elif session.limit != request.limit:
            raise CursorError("session.limit must equal request.limit")
        else:
            cache_status = "hit"

        self._maybe_prefetch(session=session)
        session = self._ensure_page_window(session=session, request=request, offset=offset)

        total = len(session.timeline_ids) + len(
            [news_id for news_id in session.prefetched_timeline_ids if news_id not in set(session.timeline_ids)]
        )
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
        context = self.context_builder.build(
            user_id=request.user_id,
            raw_context=request.context,
            repository=self.repository,
        )
        retrieval = self.retrieval_service.retrieve(context=context, exclude_ids=set())

        timeline_ids = self._mix_paths(
            onboarding_ids=[item.news_id for item in retrieval.onboarding],
            behavior_ids=[item.news_id for item in retrieval.behavior],
            breaking_ids=[item.news_id for item in retrieval.breaking],
        )
        source = f"multipath_{context.user_state}"
        fallback_used = retrieval.fallback_used or not timeline_ids
        fallback_reason = retrieval.fallback_reason
        if context.lookup_errors and fallback_reason is None:
            fallback_reason = "user_signal_lookup_failed"

        if not timeline_ids:
            timeline_ids = self.repository.fetch_latest_news_ids(limit=request.limit, offset=0)
            source = "latest_fallback"
            fallback_used = True
            fallback_reason = fallback_reason or "empty_result"

        session = RecommendationSession(
            request_id=request_id,
            user_id=request.user_id,
            limit=request.limit,
            timeline_ids=timeline_ids,
            onboarding_queue=[item.news_id for item in retrieval.onboarding],
            behavior_queue=[item.news_id for item in retrieval.behavior],
            breaking_queue=[item.news_id for item in retrieval.breaking],
            current_mix_policy={
                "onboarding": settings.onboarding_mix_weight,
                "behavior": settings.behavior_mix_weight,
                "breaking": settings.breaking_mix_weight,
            },
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            source=source,
            debug_context=request.context,
            cache_key=self._cache_key(request_id),
        )
        self.session_cache.set(request_id, session, settings.session_cache_ttl_seconds)
        return session

    def _maybe_prefetch(self, *, session: RecommendationSession) -> None:
        primary_set = set(session.onboarding_queue) | set(session.behavior_queue)
        remaining_primary = max(
            len(primary_set) - len([news_id for news_id in session.served_ids if news_id in primary_set]),
            0,
        )
        projected_remaining_primary = max(remaining_primary - session.limit, 0)
        if (
            session.prefetch_triggered
            or projected_remaining_primary > settings.prefetch_primary_low_watermark
        ):
            return

        session.prefetch_triggered = True
        session.prefetch_status = "triggered"
        session.prefetch_trigger_path = "primary"
        context = self.context_builder.build(
            user_id=session.user_id,
            raw_context=session.debug_context,
            repository=self.repository,
        )
        retrieval = self.retrieval_service.retrieve(context=context, exclude_ids=set(session.timeline_ids))
        appended = self._mix_paths(
            onboarding_ids=[item.news_id for item in retrieval.onboarding],
            behavior_ids=[item.news_id for item in retrieval.behavior],
            breaking_ids=[item.news_id for item in retrieval.breaking],
        )
        session.prefetched_timeline_ids = appended
        session.prefetched_onboarding_queue = [item.news_id for item in retrieval.onboarding]
        session.prefetched_behavior_queue = [item.news_id for item in retrieval.behavior]
        session.prefetched_breaking_queue = [item.news_id for item in retrieval.breaking]
        session.prefetch_status = "ready" if appended else "empty"
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
            session.onboarding_queue.extend(
                [
                    news_id
                    for news_id in session.prefetched_onboarding_queue
                    if news_id not in set(session.onboarding_queue)
                ]
            )
            session.behavior_queue.extend(
                [
                    news_id
                    for news_id in session.prefetched_behavior_queue
                    if news_id not in set(session.behavior_queue)
                ]
            )
            session.breaking_queue.extend(
                [news_id for news_id in session.prefetched_breaking_queue if news_id not in set(session.breaking_queue)]
            )
            session.prefetched_timeline_ids = []
            session.prefetched_onboarding_queue = []
            session.prefetched_behavior_queue = []
            session.prefetched_breaking_queue = []
            session.prefetch_status = "rolled_over"
            return session

        if settings.prefetch_policy == "primary_replenish_first":
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
                prefetch_trigger_path=session.prefetch_trigger_path,
                batch_generation_id=session.batch_generation_id,
                onboarding_remaining=len(
                    [news_id for news_id in session.onboarding_queue if news_id not in served_set]
                ),
                behavior_remaining=len([news_id for news_id in session.behavior_queue if news_id not in served_set]),
                breaking_remaining=len([news_id for news_id in session.breaking_queue if news_id not in served_set]),
                mix_ratio=dict(session.current_mix_policy),
                source=session.source,
            )
        )

    @staticmethod
    def _cache_key(request_id: str) -> str:
        return f"recommend:session:{request_id}"

    @staticmethod
    def _mix_paths(*, onboarding_ids: list[int], behavior_ids: list[int], breaking_ids: list[int]) -> list[int]:
        mixed: list[int] = []
        seen: set[int] = set()
        index_onboarding = 0
        index_behavior = 0
        index_breaking = 0

        while (
            index_onboarding < len(onboarding_ids)
            or index_behavior < len(behavior_ids)
            or index_breaking < len(breaking_ids)
        ):
            for _ in range(settings.onboarding_mix_weight):
                if index_onboarding >= len(onboarding_ids):
                    break
                news_id = onboarding_ids[index_onboarding]
                index_onboarding += 1
                if news_id not in seen:
                    mixed.append(news_id)
                    seen.add(news_id)

            for _ in range(settings.behavior_mix_weight):
                if index_behavior >= len(behavior_ids):
                    break
                news_id = behavior_ids[index_behavior]
                index_behavior += 1
                if news_id not in seen:
                    mixed.append(news_id)
                    seen.add(news_id)

            for _ in range(settings.breaking_mix_weight):
                if index_breaking >= len(breaking_ids):
                    break
                news_id = breaking_ids[index_breaking]
                index_breaking += 1
                if news_id not in seen:
                    mixed.append(news_id)
                    seen.add(news_id)

            if (
                index_onboarding >= len(onboarding_ids)
                and index_behavior >= len(behavior_ids)
                and index_breaking >= len(breaking_ids)
            ):
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
