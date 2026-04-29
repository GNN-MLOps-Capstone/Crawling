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
from app.services.bandit_service import BanditService
from app.services.recommend_logging import (
    ImpressionLog,
    RequestLog,
    context_hash,
    log_impression,
    log_request,
    now_ts,
)
from app.services.retrieval_service import RetrievalService
from app.services.session_cache import RecommendationSession, SessionCache

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecommendService:
    repository: NewsRepository
    session_cache: SessionCache
    context_builder: RecommendContextBuilder
    retrieval_service: RetrievalService
    bandit_service: BanditService

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
        elif self._is_stale_session(session):
            session = self._restore_stale_session(
                request=request,
                request_id=request_id,
                stale_session=session,
            )
            cache_status = "stale_restored"
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
            bool(self._request_context_payload(request)),
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
            user_state=self._user_state_from_source(session.source),
        )
        self._log_impressions(
            session=session,
            user_id=request.user_id,
            offset=offset,
            selected_ids=selected_ids,
        )

        return RecommendNewsResponse(
            request_id=request_id,
            items=[
                RecommendNewsItem(
                    news_id=news_id,
                    path=self._public_path(session.timeline_path_map.get(news_id, "unknown")),
                )
                for news_id in selected_ids
            ],
            next_cursor=next_cursor,
            meta=RecommendNewsMeta(
                source=session.source,
                fallback_used=session.fallback_used,
                fallback_reason=session.fallback_reason,
            ),
        )

    def _build_session(
        self,
        *,
        request: RecommendNewsRequest,
        request_id: str,
        exclude_ids: set[int] | None = None,
        served_prefix_ids: list[int] | None = None,
        served_prefix_path_map: dict[int, str] | None = None,
    ) -> RecommendationSession:
        context = self.context_builder.build(
            user_id=request.user_id,
            raw_context=request.context,
            repository=self.repository,
        )
        retrieval = self.retrieval_service.retrieve(context=context, exclude_ids=exclude_ids or set())

        mix_plan = self.bandit_service.build_mix_plan(
            path_candidates={
                "onboarding": [item.news_id for item in retrieval.onboarding],
                "behavior": [item.news_id for item in retrieval.behavior],
                "breaking": [item.news_id for item in retrieval.breaking],
                "popular": [item.news_id for item in retrieval.popular],
            },
            user_id=request.user_id,
        )
        prefix_ids = served_prefix_ids or []
        prefix_path_map = served_prefix_path_map or {}
        timeline_ids = prefix_ids + [news_id for news_id, _ in mix_plan.timeline_entries]
        timeline_path_map = {
            **prefix_path_map,
            **{news_id: path for news_id, path in mix_plan.timeline_entries},
        }
        source = f"multipath_{context.user_state}"
        fallback_used = retrieval.fallback_used or not timeline_ids
        fallback_reason = self._resolve_fallback_reason(context=context, retrieval_fallback_reason=retrieval.fallback_reason)

        if not timeline_ids:
            timeline_ids = self.repository.fetch_latest_news_ids(limit=request.limit, offset=0)
            source = "latest_fallback"
            fallback_used = True
            fallback_reason = fallback_reason or "empty_result"
            timeline_path_map = {news_id: "latest" for news_id in timeline_ids}

        session = RecommendationSession(
            request_id=request_id,
            user_id=request.user_id,
            limit=request.limit,
            timeline_ids=timeline_ids,
            onboarding_queue=[item.news_id for item in retrieval.onboarding],
            behavior_queue=[item.news_id for item in retrieval.behavior],
            breaking_queue=[item.news_id for item in retrieval.breaking],
            popular_queue=[item.news_id for item in retrieval.popular],
            current_mix_policy=mix_plan.mix_policy,
            mix_allocator=mix_plan.allocator,
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            source=source,
            timeline_path_map=timeline_path_map,
            debug_context=self._request_context_payload(request),
            cache_key=self._cache_key(request_id),
        )
        self.session_cache.set(request_id, session, settings.session_cache_ttl_seconds)
        return session

    def _restore_stale_session(
        self,
        *,
        request: RecommendNewsRequest,
        request_id: str,
        stale_session: RecommendationSession,
    ) -> RecommendationSession:
        restored = self._build_session(
            request=request,
            request_id=request_id,
            exclude_ids=set(stale_session.served_ids),
            served_prefix_ids=stale_session.served_ids[:],
            served_prefix_path_map={
                news_id: stale_session.timeline_path_map.get(news_id, "unknown")
                for news_id in stale_session.served_ids
            },
        )
        restored.batch_generation_id = stale_session.batch_generation_id + 1
        restored.prefetch_status = "stale_restored"
        restored.prefetch_triggered = False
        restored.prefetch_trigger_path = None
        restored.fallback_used = True
        restored.fallback_reason = "stale_session_restored"
        restored.served_ids = stale_session.served_ids[:]
        self.session_cache.set(request_id, restored, settings.session_cache_ttl_seconds)
        return restored

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
        mix_plan = self.bandit_service.build_mix_plan(
            path_candidates={
                "onboarding": [item.news_id for item in retrieval.onboarding],
                "behavior": [item.news_id for item in retrieval.behavior],
                "breaking": [item.news_id for item in retrieval.breaking],
                "popular": [item.news_id for item in retrieval.popular],
            },
            user_id=session.user_id,
        )
        appended = [news_id for news_id, _ in mix_plan.timeline_entries]
        session.prefetched_timeline_ids = appended
        session.prefetched_timeline_path_map = {
            news_id: path for news_id, path in mix_plan.timeline_entries
        }
        session.prefetched_onboarding_queue = [item.news_id for item in retrieval.onboarding]
        session.prefetched_behavior_queue = [item.news_id for item in retrieval.behavior]
        session.prefetched_breaking_queue = [item.news_id for item in retrieval.breaking]
        session.prefetched_popular_queue = [item.news_id for item in retrieval.popular]
        session.current_mix_policy = mix_plan.mix_policy
        session.mix_allocator = mix_plan.allocator
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
            session.popular_queue.extend(
                [news_id for news_id in session.prefetched_popular_queue if news_id not in set(session.popular_queue)]
            )
            session.prefetched_timeline_ids = []
            session.prefetched_onboarding_queue = []
            session.prefetched_behavior_queue = []
            session.prefetched_breaking_queue = []
            session.prefetched_popular_queue = []
            session.timeline_path_map.update(session.prefetched_timeline_path_map)
            session.prefetched_timeline_path_map = {}
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
        user_state: str,
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
                context_hash=context_hash(self._request_context_payload(request)),
                context_present=bool(self._request_context_payload(request)),
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
                popular_remaining=len([news_id for news_id in session.popular_queue if news_id not in served_set]),
                mix_ratio=dict(session.current_mix_policy),
                source=session.source,
                user_state=user_state,
            )
        )

    def _log_impressions(
        self,
        *,
        session: RecommendationSession,
        user_id: int,
        offset: int,
        selected_ids: list[int],
    ) -> None:
        for index, news_id in enumerate(selected_ids, start=1):
            event_ts = now_ts()
            path = session.timeline_path_map.get(news_id, "unknown")
            log_impression(
                ImpressionLog(
                    timestamp=event_ts,
                    request_id=session.request_id,
                    session_id=session.request_id,
                    user_id=user_id,
                    news_id=news_id,
                    rank=offset + index,
                    path=path,
                    batch_generation_id=session.batch_generation_id,
                )
            )

    @staticmethod
    def _cache_key(request_id: str) -> str:
        return f"recommend:session:{request_id}"

    @staticmethod
    def _public_path(path: str) -> str:
        path_map = {
            "onboarding": "A1",
            "behavior": "A2",
            "breaking": "B",
            "popular": "C",
            "latest": "LATEST",
        }
        return path_map.get(path, path.upper())

    @staticmethod
    def _resolve_fallback_reason(*, context, retrieval_fallback_reason: str | None) -> str | None:
        if retrieval_fallback_reason is not None:
            return retrieval_fallback_reason
        if context.lookup_errors:
            return "user_signal_lookup_failed"
        if not context.has_onboarding_signals and not context.has_behavior_signals:
            return "profile_missing"
        if not context.has_onboarding_signals:
            return "profile_missing"
        return None

    @staticmethod
    def _user_state_from_source(source: str) -> str:
        if source.endswith("_warm"):
            return "warm"
        if source.endswith("_cold"):
            return "cold"
        return "fallback"

    @staticmethod
    def _request_context_payload(request: RecommendNewsRequest) -> dict[str, object]:
        return request.context.model_dump(exclude_defaults=True, exclude_none=True)

    @staticmethod
    def _is_stale_session(session: RecommendationSession) -> bool:
        max_age = max(settings.session_stale_after_seconds, 1)
        return (now_ts() - session.last_served_at) >= max_age

    @staticmethod
    def _validate_cursor(*, payload: CursorPayload, request: RecommendNewsRequest) -> None:
        if payload.v != settings.default_cursor_version:
            raise CursorError("Unsupported cursor version")
        if payload.limit != request.limit:
            raise CursorError("cursor.limit must equal request.limit")
        if request.request_id and request.request_id != payload.request_id:
            raise CursorError("request_id does not match cursor.request_id")
