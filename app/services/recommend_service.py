from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
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
from app.services.experiment_service import ExperimentAssignment, ExperimentService
from app.services.metrics import record_recommend_success, set_experiment_labels
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
    experiment_service: ExperimentService = field(default_factory=ExperimentService)

    def recommend_news(self, request: RecommendNewsRequest) -> RecommendNewsResponse:
        started_at = perf_counter()
        request_id = request.request_id or str(uuid.uuid4())
        offset = 0
        cache_status = "miss"
        assignment: ExperimentAssignment | None = None

        if request.force_refresh and request.cursor:
            raise CursorError("force_refresh cannot be used with cursor")

        if request.cursor:
            payload = decode_cursor(request.cursor)
            self._validate_cursor(payload=payload, request=request)
            request_id = payload.request_id
            offset = payload.offset

        session = None if request.force_refresh else self.session_cache.get(request_id)
        if session is None and request.cursor:
            raise CursorError("session not found for cursor")
        if session is None:
            assignment = self.experiment_service.assign(
                user_id=request.user_id,
                eligible=self._is_experiment_eligible(user_id=request.user_id),
            )
            set_experiment_labels(experiment_id=assignment.experiment_id, variant=assignment.variant)
            session = self._build_session(request=request, request_id=request_id, assignment=assignment)
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

        set_experiment_labels(experiment_id=session.experiment_id, variant=session.variant)
        self._maybe_prefetch(session=session)
        session = self._ensure_page_window(session=session, request=request, offset=offset)
        set_experiment_labels(experiment_id=session.experiment_id, variant=session.variant)

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

        public_paths = [
            self._public_path(session.timeline_path_map.get(news_id, "unknown"))
            for news_id in selected_ids
        ]
        response = RecommendNewsResponse(
            request_id=request_id,
            items=[
                RecommendNewsItem(news_id=news_id, path=path)
                for news_id, path in zip(selected_ids, public_paths, strict=False)
            ],
            next_cursor=next_cursor,
            meta=RecommendNewsMeta(
                source=session.source,
                fallback_used=session.fallback_used,
                fallback_reason=session.fallback_reason,
                experiment_id=session.experiment_id,
                variant=session.variant,
            ),
        )
        record_recommend_success(
            user_state=self._user_state_from_source(session.source),
            cache_status=cache_status,
            latency_seconds=perf_counter() - started_at,
            item_count=len(selected_ids),
            fallback_used=session.fallback_used,
            fallback_reason=session.fallback_reason,
            item_paths=public_paths,
            prefetch_status=session.prefetch_status,
            prefetch_trigger_path=session.prefetch_trigger_path,
            session_event=self._session_event(cache_status=cache_status, prefetch_status=session.prefetch_status),
        )
        return response

    def _build_session(
        self,
        *,
        request: RecommendNewsRequest,
        request_id: str,
        exclude_ids: set[int] | None = None,
        served_prefix_ids: list[int] | None = None,
        served_prefix_path_map: dict[int, str] | None = None,
        assignment: ExperimentAssignment | None = None,
    ) -> RecommendationSession:
        assignment = assignment or self.experiment_service.assign(
            user_id=request.user_id,
            eligible=self._is_experiment_eligible(user_id=request.user_id),
        )
        context = self.context_builder.build(
            user_id=request.user_id,
            raw_context=request.context,
            repository=self.repository,
        )
        suppression = self._build_suppression_policy(
            user_id=request.user_id,
            context=context,
            exclude_ids=exclude_ids or set(),
        )
        if assignment.strategy == "latest":
            return self._build_latest_session(
                request=request,
                request_id=request_id,
                context=context,
                exclude_ids=suppression["exclude_ids"],
                assignment=assignment,
                served_prefix_ids=served_prefix_ids,
                served_prefix_path_map=served_prefix_path_map,
            )
        if assignment.strategy == "popular":
            return self._build_popular_session(
                request=request,
                request_id=request_id,
                context=context,
                exclude_ids=suppression["exclude_ids"],
                assignment=assignment,
                served_prefix_ids=served_prefix_ids,
                served_prefix_path_map=served_prefix_path_map,
            )
        if assignment.strategy == "random":
            return self._build_random_session(
                request=request,
                request_id=request_id,
                context=context,
                exclude_ids=suppression["exclude_ids"],
                assignment=assignment,
                served_prefix_ids=served_prefix_ids,
                served_prefix_path_map=served_prefix_path_map,
            )

        retrieval = self.retrieval_service.retrieve(
            context=context,
            exclude_ids=suppression["exclude_ids"],
            exposure_penalties=suppression["exposure_penalties"],
        )

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
            timeline_ids = self.repository.fetch_latest_news_ids(
                limit=request.limit,
                offset=0,
                exclude_ids=suppression["exclude_ids"],
            )
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
            experiment_id=assignment.experiment_id,
            variant=assignment.variant,
            timeline_path_map=timeline_path_map,
            debug_context=self._request_context_payload(request),
            cache_key=self._cache_key(request_id),
        )
        self.session_cache.set(request_id, session, settings.session_cache_ttl_seconds)
        return session

    def _build_latest_session(
        self,
        *,
        request: RecommendNewsRequest,
        request_id: str,
        context,
        exclude_ids: set[int],
        assignment: ExperimentAssignment,
        served_prefix_ids: list[int] | None,
        served_prefix_path_map: dict[int, str] | None,
    ) -> RecommendationSession:
        prefix_ids = served_prefix_ids or []
        prefix_path_map = served_prefix_path_map or {}
        candidate_limit = max(settings.default_limit_max, request.limit)
        latest_ids = self.repository.fetch_latest_news_ids(
            limit=candidate_limit,
            offset=0,
            exclude_ids=exclude_ids | set(prefix_ids),
        )
        timeline_ids = prefix_ids + latest_ids
        timeline_path_map = {
            **prefix_path_map,
            **{news_id: "latest" for news_id in latest_ids},
        }
        return RecommendationSession(
            request_id=request_id,
            user_id=request.user_id,
            limit=request.limit,
            timeline_ids=timeline_ids,
            onboarding_queue=[],
            behavior_queue=[],
            breaking_queue=[],
            popular_queue=[],
            current_mix_policy={"latest": len(latest_ids)},
            mix_allocator="ab_baseline",
            fallback_used=False,
            fallback_reason=None,
            source=f"ab_latest_{context.user_state}",
            experiment_id=assignment.experiment_id,
            variant=assignment.variant,
            timeline_path_map=timeline_path_map,
            debug_context=self._request_context_payload(request),
            cache_key=self._cache_key(request_id),
        )

    def _build_popular_session(
        self,
        *,
        request: RecommendNewsRequest,
        request_id: str,
        context,
        exclude_ids: set[int],
        assignment: ExperimentAssignment,
        served_prefix_ids: list[int] | None,
        served_prefix_path_map: dict[int, str] | None,
    ) -> RecommendationSession:
        prefix_ids = served_prefix_ids or []
        prefix_path_map = served_prefix_path_map or {}
        candidates = self.repository.fetch_popular_candidates(
            limit=max(settings.popular_candidate_limit, request.limit),
            exclude_ids=exclude_ids | set(prefix_ids),
            blocked_domains=settings.blocked_domains,
        )
        candidate_ids = [candidate.news_id for candidate in candidates]
        fallback_used = False
        fallback_reason = None
        path = "popular"
        if not candidate_ids:
            candidate_ids = self.repository.fetch_latest_news_ids(
                limit=request.limit,
                offset=0,
                exclude_ids=exclude_ids | set(prefix_ids),
            )
            fallback_used = True
            fallback_reason = "popular_pool_empty"
            path = "latest"
        timeline_ids = prefix_ids + candidate_ids
        timeline_path_map = {
            **prefix_path_map,
            **{news_id: path for news_id in candidate_ids},
        }
        return RecommendationSession(
            request_id=request_id,
            user_id=request.user_id,
            limit=request.limit,
            timeline_ids=timeline_ids,
            onboarding_queue=[],
            behavior_queue=[],
            breaking_queue=[],
            popular_queue=candidate_ids if path == "popular" else [],
            current_mix_policy={path: len(candidate_ids)},
            mix_allocator="ab_baseline",
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            source=f"ab_popular_{context.user_state}",
            experiment_id=assignment.experiment_id,
            variant=assignment.variant,
            timeline_path_map=timeline_path_map,
            debug_context=self._request_context_payload(request),
            cache_key=self._cache_key(request_id),
        )

    def _build_random_session(
        self,
        *,
        request: RecommendNewsRequest,
        request_id: str,
        context,
        exclude_ids: set[int],
        assignment: ExperimentAssignment,
        served_prefix_ids: list[int] | None,
        served_prefix_path_map: dict[int, str] | None,
    ) -> RecommendationSession:
        prefix_ids = served_prefix_ids or []
        prefix_path_map = served_prefix_path_map or {}
        candidate_limit = max(settings.default_limit_max, request.limit * settings.retrieval_candidate_pool_multiplier)
        candidates = self.repository.fetch_recent_candidates(
            limit=candidate_limit,
            hours=settings.base_pool_hours,
            exclude_ids=exclude_ids | set(prefix_ids),
            stale_cutoff_minutes=None,
            blocked_domains=settings.blocked_domains,
        )
        candidate_ids = [candidate.news_id for candidate in candidates]
        rng = self.experiment_service.stable_random(
            assignment=assignment,
            user_id=request.user_id,
            request_id=request_id,
        )
        rng.shuffle(candidate_ids)
        fallback_used = False
        fallback_reason = None
        path = "random"
        if not candidate_ids:
            candidate_ids = self.repository.fetch_latest_news_ids(
                limit=request.limit,
                offset=0,
                exclude_ids=exclude_ids | set(prefix_ids),
            )
            fallback_used = True
            fallback_reason = "random_pool_empty"
            path = "latest"
        timeline_ids = prefix_ids + candidate_ids
        timeline_path_map = {
            **prefix_path_map,
            **{news_id: path for news_id in candidate_ids},
        }
        return RecommendationSession(
            request_id=request_id,
            user_id=request.user_id,
            limit=request.limit,
            timeline_ids=timeline_ids,
            onboarding_queue=[],
            behavior_queue=[],
            breaking_queue=[],
            popular_queue=[],
            current_mix_policy={path: len(candidate_ids)},
            mix_allocator="ab_baseline",
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            source=f"ab_random_{context.user_state}",
            experiment_id=assignment.experiment_id,
            variant=assignment.variant,
            timeline_path_map=timeline_path_map,
            debug_context=self._request_context_payload(request),
            cache_key=self._cache_key(request_id),
        )

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
            assignment=ExperimentAssignment(
                experiment_id=stale_session.experiment_id,
                variant=stale_session.variant,
            ),
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
        if session.variant != "recommend":
            return
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
        suppression = self._build_suppression_policy(
            user_id=session.user_id,
            context=context,
            exclude_ids=set(session.timeline_ids),
        )
        retrieval = self.retrieval_service.retrieve(
            context=context,
            exclude_ids=suppression["exclude_ids"],
            exposure_penalties=suppression["exposure_penalties"],
        )
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
            rebuilt = self._build_session(
                request=request,
                request_id=session.request_id,
                assignment=ExperimentAssignment(
                    experiment_id=session.experiment_id,
                    variant=session.variant,
                ),
            )
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
                experiment_id=session.experiment_id,
                variant=session.variant,
            )
        )

    def _build_suppression_policy(
        self,
        *,
        user_id: int,
        context,
        exclude_ids: set[int],
    ) -> dict[str, object]:
        hard_exclude_ids = set(exclude_ids)
        hard_exclude_ids.update(self._read_news_ids(user_id=user_id, context=context))
        exposure_penalties: dict[int, float] = {}

        served_stats = self._recent_served_news(user_id=user_id)
        now = datetime.now(UTC)
        for news_id, stats in served_stats.items():
            last_served_at = self._aware_datetime(getattr(stats, "last_served_at", None))
            serve_count = int(getattr(stats, "serve_count", 0) or 0)
            if last_served_at is None or serve_count <= 0:
                continue
            age_seconds = (now - last_served_at).total_seconds()
            if age_seconds <= max(settings.exposure_cooldown_seconds, 0):
                hard_exclude_ids.add(news_id)
                continue
            exposure_penalties[news_id] = max(
                settings.exposure_penalty_min_multiplier,
                1.0 - (serve_count * settings.exposure_penalty_per_impression),
            )

        return {
            "exclude_ids": hard_exclude_ids,
            "exposure_penalties": exposure_penalties,
        }

    def _read_news_ids(self, *, user_id: int, context) -> set[int]:
        read_ids = {
            int(action["news_id"])
            for action in context.recent_actions
            if isinstance(action, dict) and isinstance(action.get("news_id"), int)
        }
        if settings.read_exclude_days <= 0 or settings.read_exclude_limit <= 0:
            return read_ids

        try:
            read_ids.update(
                self.repository.fetch_read_news_ids(
                    user_id=user_id,
                    days=settings.read_exclude_days,
                    limit=settings.read_exclude_limit,
                    dwell_threshold_seconds=settings.valid_read_dwell_seconds,
                )
            )
        except Exception as exc:
            logger.warning("read news suppression lookup failed for user_id=%s: %s", user_id, exc)
        return read_ids

    def _recent_served_news(self, *, user_id: int) -> dict[int, object]:
        if settings.exposure_penalty_window_hours <= 0:
            return {}
        try:
            return self.repository.fetch_recent_served_news(
                user_id=user_id,
                window_hours=settings.exposure_penalty_window_hours,
            )
        except Exception as exc:
            logger.warning("served news suppression lookup failed for user_id=%s: %s", user_id, exc)
            return {}

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
                    experiment_id=session.experiment_id,
                    variant=session.variant,
                )
            )

    def _is_experiment_eligible(self, *, user_id: int) -> bool:
        min_requests = max(settings.recommend_experiment_min_requests, 0)
        if min_requests <= 0:
            return True
        try:
            return self.repository.fetch_recommend_request_count(user_id=user_id) >= min_requests
        except Exception as exc:
            logger.warning("recommend experiment eligibility lookup failed for user_id=%s: %s", user_id, exc)
            return False

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
            "random": "RANDOM",
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
    def _session_event(*, cache_status: str, prefetch_status: str) -> str | None:
        if cache_status == "stale_restored":
            return "stale_restored"
        if prefetch_status in {"rebuilt", "rolled_over"}:
            return prefetch_status
        return None

    @staticmethod
    def _aware_datetime(value: object) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

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
