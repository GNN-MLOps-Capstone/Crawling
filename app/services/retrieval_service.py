from __future__ import annotations

import math
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from app.repositories.news_repository import NewsCandidate, NewsRepository
from app.services.context_builder import NormalizedRecommendContext
from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WeightedEntity:
    entity_id: str
    entity_type: str
    weight: float
    frequency: int = 1


@dataclass(frozen=True)
class RetrievalResult:
    onboarding: list[NewsCandidate]
    behavior: list[NewsCandidate]
    breaking: list[NewsCandidate]
    popular: list[NewsCandidate]
    fallback_used: bool = False
    fallback_reason: str | None = None


@dataclass(frozen=True)
class RetrievalService:
    repository: NewsRepository
    base_pool_hours: int
    onboarding_hours: int
    behavior_hours: int
    breaking_hours: int
    breaking_stale_cutoff_minutes: int
    onboarding_limit: int
    behavior_limit: int
    breaking_limit: int
    popular_limit: int
    blocked_domains: tuple[str, ...] = ()

    def retrieve(
        self,
        *,
        context: NormalizedRecommendContext,
        exclude_ids: set[int],
    ) -> RetrievalResult:
        base_pool_limit = self._base_pool_limit()
        breaking_hours = min(self.breaking_hours, self.base_pool_hours)
        onboarding: list[NewsCandidate] = []
        behavior: list[NewsCandidate] = []
        breaking: list[NewsCandidate] = []
        popular: list[NewsCandidate] = []
        onboarding_failed = False
        behavior_failed = False
        breaking_failed = False
        popular_failed = False
        behavior_insufficient = False
        base_pool: list[NewsCandidate] = []

        try:
            base_pool = self.repository.fetch_recent_candidates(
                limit=base_pool_limit,
                hours=self.base_pool_hours,
                exclude_ids=exclude_ids,
                stale_cutoff_minutes=None,
                blocked_domains=self.blocked_domains,
            )
        except Exception as exc:
            logger.warning("base pool retrieval failed: %s", exc)
            onboarding_failed = True
            if context.has_behavior_signals:
                behavior_failed = True

        if not onboarding_failed:
            try:
                onboarding = self._score_onboarding_candidates(
                    candidates=base_pool,
                    context=context,
                    limit=self.onboarding_limit,
                )
            except Exception as exc:
                logger.warning("onboarding scoring failed: %s", exc)
                onboarding_failed = True

        if context.has_behavior_signals:
            if not behavior_failed:
                try:
                    behavior_pool = [
                        candidate
                        for candidate in base_pool
                        if candidate.news_id not in {item.news_id for item in onboarding}
                    ]
                    behavior, behavior_insufficient = self._score_behavior_candidates(
                        candidates=behavior_pool,
                        context=context,
                        limit=self.behavior_limit,
                    )
                except Exception as exc:
                    logger.warning("behavior scoring failed: %s", exc)
                    behavior_failed = True

        try:
            breaking = self.repository.fetch_recent_candidates(
                limit=self.breaking_limit,
                hours=breaking_hours,
                exclude_ids=exclude_ids | {item.news_id for item in onboarding} | {item.news_id for item in behavior},
                stale_cutoff_minutes=self.breaking_stale_cutoff_minutes,
                blocked_domains=self.blocked_domains,
            )
        except Exception as exc:
            logger.warning("breaking retrieval failed: %s", exc)
            breaking_failed = True

        try:
            popular = self.repository.fetch_popular_candidates(
                limit=self.popular_limit,
                exclude_ids=exclude_ids
                | {item.news_id for item in onboarding}
                | {item.news_id for item in behavior}
                | {item.news_id for item in breaking},
                blocked_domains=self.blocked_domains,
            )
        except Exception as exc:
            logger.warning("popular retrieval failed: %s", exc)
            popular_failed = True

        if settings.breaking_replenish_min_count > len(breaking):
            try:
                onboarding, breaking = self._replenish_breaking_candidates(
                    context=context,
                    exclude_ids=exclude_ids,
                    onboarding=onboarding,
                    behavior=behavior,
                    breaking=breaking,
                )
            except Exception as exc:
                logger.warning("breaking replenish failed: %s", exc)

        if onboarding or behavior:
            return RetrievalResult(
                onboarding=onboarding,
                behavior=behavior,
                breaking=breaking,
                popular=popular,
                fallback_used=onboarding_failed or behavior_failed or breaking_failed or popular_failed,
                fallback_reason=self._fallback_reason(
                    onboarding_failed=onboarding_failed,
                    behavior_failed=behavior_failed,
                    breaking_failed=breaking_failed,
                    popular_failed=popular_failed,
                    behavior_insufficient=behavior_insufficient,
                ),
            )

        if breaking or popular:
            if (
                not context.has_onboarding_signals
                and not context.has_behavior_signals
                and not (onboarding_failed or behavior_failed or breaking_failed or popular_failed)
            ):
                return RetrievalResult(
                    onboarding=[],
                    behavior=[],
                    breaking=breaking,
                    popular=popular,
                    fallback_used=False,
                    fallback_reason=None,
                )
            return RetrievalResult(
                onboarding=[],
                behavior=[],
                breaking=breaking,
                popular=popular,
                fallback_used=True,
                fallback_reason="primary_failed_use_exploration",
            )

        try:
            latest = self.repository.fetch_latest_news_ids(
                limit=self.onboarding_limit + self.behavior_limit + self.breaking_limit,
                offset=0,
            )
            onboarding = [
                NewsCandidate(
                    news_id=news_id,
                    title="",
                    pub_date=None,
                    url="",
                    category="unknown",
                    domain="unknown",
                )
                for news_id in latest
            ]
            return RetrievalResult(
                onboarding=onboarding,
                behavior=[],
                breaking=[],
                popular=[],
                fallback_used=True,
                fallback_reason="retrieval_failed",
            )
        except Exception as exc:
            logger.warning("latest fallback retrieval failed: %s", exc)
            return RetrievalResult(
                onboarding=[],
                behavior=[],
                breaking=[],
                popular=[],
                fallback_used=True,
                fallback_reason="db_failed",
            )

    @staticmethod
    def _fallback_reason(
        *,
        onboarding_failed: bool,
        behavior_failed: bool,
        breaking_failed: bool,
        popular_failed: bool,
        behavior_insufficient: bool = False,
    ) -> str | None:
        if onboarding_failed:
            return "onboarding_failed"
        if behavior_failed:
            return "behavior_failed"
        if breaking_failed:
            return "breaking_failed"
        if popular_failed:
            return "popular_failed"
        if behavior_insufficient:
            return "behavior_insufficient"
        return None

    def _score_onboarding_candidates(
        self,
        *,
        candidates: list[NewsCandidate],
        context: NormalizedRecommendContext,
        limit: int,
    ) -> list[NewsCandidate]:
        query_entities = self._build_onboarding_query_entities(context)
        if not query_entities:
            return []
        return self._rank_candidates(
            candidates=candidates,
            query_entities=query_entities,
            stock_boost_entity_ids={
                entity.entity_id for entity in query_entities if entity.entity_type == "stock"
            },
            stock_match_boost=settings.onboarding_stock_match_boost,
            limit=limit,
        )

    def _score_behavior_candidates(
        self,
        *,
        candidates: list[NewsCandidate],
        context: NormalizedRecommendContext,
        limit: int,
    ) -> tuple[list[NewsCandidate], bool]:
        query_entities = self._build_behavior_query_entities(context)
        seen_recent_ids = {
            int(action["news_id"])
            for action in context.recent_actions
            if isinstance(action, dict) and isinstance(action.get("news_id"), int)
        }
        filtered_candidates = [
            candidate for candidate in candidates if candidate.news_id not in seen_recent_ids
        ]
        if not query_entities:
            return filtered_candidates[:limit], True
        repeated_stock_ids = {
            entity.entity_id
            for entity in query_entities
            if entity.entity_type == "stock" and entity.frequency > 1
        }
        ranked_candidates = self._rank_candidates(
            candidates=filtered_candidates,
            query_entities=query_entities,
            stock_boost_entity_ids=repeated_stock_ids,
            stock_match_boost=settings.behavior_repeat_stock_match_boost,
            limit=limit,
        )
        ranked_candidates = [
            candidate
            for candidate in ranked_candidates
            if (candidate.score or 0.0) >= settings.behavior_min_candidate_score
        ]
        return ranked_candidates[:limit], False

    def _replenish_breaking_candidates(
        self,
        *,
        context: NormalizedRecommendContext,
        exclude_ids: set[int],
        onboarding: list[NewsCandidate],
        behavior: list[NewsCandidate],
        breaking: list[NewsCandidate],
    ) -> tuple[list[NewsCandidate], list[NewsCandidate]]:
        target_count = min(max(settings.breaking_replenish_min_count, 0), self.breaking_limit)
        if target_count <= len(breaking):
            return onboarding, breaking

        breaking_hours = min(self.breaking_hours, self.base_pool_hours)
        supplemental_candidates = self.repository.fetch_recent_candidates(
            limit=self.breaking_limit,
            hours=breaking_hours,
            exclude_ids=exclude_ids | {item.news_id for item in behavior} | {item.news_id for item in breaking},
            stale_cutoff_minutes=self.breaking_stale_cutoff_minutes,
            blocked_domains=self.blocked_domains,
        )

        replenished_breaking_by_id = {item.news_id: item for item in breaking}
        for candidate in reversed(supplemental_candidates):
            if candidate.news_id in replenished_breaking_by_id:
                continue
            replenished_breaking_by_id[candidate.news_id] = candidate
            if len(replenished_breaking_by_id) >= target_count:
                break

        if len(replenished_breaking_by_id) == len(breaking):
            return onboarding, breaking

        replenished_ids = set(replenished_breaking_by_id)
        updated_onboarding = [
            candidate for candidate in onboarding if candidate.news_id not in replenished_ids
        ]
        replenished_breaking = [
            replenished_breaking_by_id[news_id]
            for news_id in [item.news_id for item in breaking]
            if news_id in replenished_breaking_by_id
        ]
        replenished_breaking.extend(
            candidate
            for candidate in supplemental_candidates
            if candidate.news_id in replenished_ids and candidate.news_id not in {item.news_id for item in breaking}
        )
        return updated_onboarding, replenished_breaking[: self.breaking_limit]

    def _rank_candidates(
        self,
        *,
        candidates: list[NewsCandidate],
        query_entities: list[WeightedEntity],
        stock_boost_entity_ids: set[str],
        stock_match_boost: float,
        limit: int,
    ) -> list[NewsCandidate]:
        if not candidates or not query_entities:
            return candidates[:limit]

        news_entities = self.repository.fetch_news_entities(news_ids=[candidate.news_id for candidate in candidates])
        entity_refs = self._collect_entity_refs(query_entities=query_entities, news_entities=news_entities)
        embeddings = self.repository.fetch_entity_embeddings(entity_refs=entity_refs)
        if not embeddings:
            return candidates[:limit]

        scored_candidates: list[NewsCandidate] = []
        for candidate in candidates:
            entity_map = news_entities.get(candidate.news_id, {"keyword_ids": [], "stock_ids": []})
            score = self._candidate_score(
                candidate_entities=entity_map,
                query_entities=query_entities,
                embeddings=embeddings,
                stock_boost_entity_ids=stock_boost_entity_ids,
                stock_match_boost=stock_match_boost,
            )
            scored_candidates.append(
                NewsCandidate(
                    news_id=candidate.news_id,
                    title=candidate.title,
                    pub_date=candidate.pub_date,
                    url=candidate.url,
                    category=candidate.category,
                    domain=candidate.domain,
                    score=score,
                )
            )

        scored_candidates.sort(
            key=lambda item: (
                item.score if item.score is not None else float("-inf"),
                item.pub_date or datetime.min.replace(tzinfo=None),
                item.news_id,
            ),
            reverse=True,
        )
        return scored_candidates[:limit]

    def _candidate_score(
        self,
        *,
        candidate_entities: dict[str, list[str]],
        query_entities: list[WeightedEntity],
        embeddings: dict[tuple[str, str], list[float]],
        stock_boost_entity_ids: set[str],
        stock_match_boost: float,
    ) -> float:
        candidate_refs = [
            ("keyword", entity_id) for entity_id in candidate_entities.get("keyword_ids", [])
        ] + [
            ("stock", entity_id) for entity_id in candidate_entities.get("stock_ids", [])
        ]
        if not candidate_refs:
            return 0.0

        total_score = 0.0
        for query_entity in query_entities:
            query_vector = embeddings.get((query_entity.entity_type, query_entity.entity_id))
            if not query_vector:
                continue

            best_similarity = 0.0
            for candidate_type, candidate_entity_id in candidate_refs:
                candidate_vector = embeddings.get((candidate_type, candidate_entity_id))
                if not candidate_vector:
                    continue
                similarity = self._cosine_similarity(query_vector, candidate_vector)
                similarity = max(similarity, 0.0)
                similarity *= self._pair_weight(query_entity.entity_type, candidate_type)
                if similarity > best_similarity:
                    best_similarity = similarity

            total_score += query_entity.weight * best_similarity

        stock_matches = stock_boost_entity_ids & set(candidate_entities.get("stock_ids", []))
        if stock_matches:
            total_score += stock_match_boost * len(stock_matches)
        return total_score

    @staticmethod
    def _collect_entity_refs(
        *,
        query_entities: list[WeightedEntity],
        news_entities: dict[int, dict[str, list[str]]],
    ) -> list[tuple[str, str]]:
        refs = {
            (entity.entity_type, entity.entity_id)
            for entity in query_entities
        }
        for entity_map in news_entities.values():
            refs.update(("keyword", entity_id) for entity_id in entity_map.get("keyword_ids", []))
            refs.update(("stock", entity_id) for entity_id in entity_map.get("stock_ids", []))
        return sorted(refs)

    @staticmethod
    def _build_onboarding_query_entities(context: NormalizedRecommendContext) -> list[WeightedEntity]:
        profile = context.profile or {}
        entities: list[WeightedEntity] = []
        entities.extend(
            WeightedEntity(entity_id=entity_id, entity_type="stock", weight=1.0)
            for entity_id in profile.get("stock_ids", [])
        )
        entities.extend(
            WeightedEntity(entity_id=entity_id, entity_type="keyword", weight=1.0)
            for entity_id in profile.get("keyword_ids", [])
        )
        return entities

    @staticmethod
    def _build_behavior_query_entities(context: NormalizedRecommendContext) -> list[WeightedEntity]:
        aggregates: dict[tuple[str, str], dict[str, object]] = {}
        now = datetime.now(UTC)
        ordered_actions = sorted(
            context.recent_actions,
            key=lambda action: RetrievalService._parse_timestamp(action.get("timestamp")) or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )

        for action in ordered_actions:
            action_ts = RetrievalService._parse_timestamp(action.get("timestamp"))
            decay = RetrievalService._time_decay(action_ts=action_ts, now=now)
            for entity_type, key in (("keyword", "keyword_ids"), ("stock", "stock_ids")):
                for entity_id in action.get(key, []):
                    bucket = aggregates.setdefault(
                        (entity_type, str(entity_id)),
                        {"count": 0, "best_decay": 0.0, "decayed_sum": 0.0},
                    )
                    bucket["count"] = int(bucket["count"]) + 1
                    bucket["best_decay"] = max(float(bucket["best_decay"]), decay)
                    bucket["decayed_sum"] = float(bucket["decayed_sum"]) + decay

        if len(ordered_actions) < settings.behavior_min_recent_actions:
            return []
        if len(aggregates) < settings.behavior_min_unique_entities:
            return []

        query_entities: list[WeightedEntity] = []
        for (entity_type, entity_id), payload in aggregates.items():
            count = int(payload["count"])
            decay = float(payload["best_decay"])
            decayed_sum = float(payload["decayed_sum"])
            weight = math.log1p(count) + (0.7 * decayed_sum) + (0.3 * decay)
            query_entities.append(
                WeightedEntity(
                    entity_id=entity_id,
                    entity_type=entity_type,
                    weight=weight,
                    frequency=count,
                )
            )
        return query_entities

    @staticmethod
    def _parse_timestamp(raw_value: object) -> datetime | None:
        if not isinstance(raw_value, str) or not raw_value:
            return None
        try:
            normalized = raw_value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _time_decay(*, action_ts: datetime | None, now: datetime) -> float:
        if action_ts is None:
            return 1.0
        age_hours = max((now - action_ts).total_seconds() / 3600, 0.0)
        half_life = max(settings.behavior_decay_half_life_hours, 1.0)
        return math.exp(-math.log(2) * age_hours / half_life)

    @staticmethod
    def _cosine_similarity(left: list[float], right: list[float]) -> float:
        if len(left) != len(right) or not left:
            return 0.0
        dot = sum(a * b for a, b in zip(left, right))
        left_norm = math.sqrt(sum(value * value for value in left))
        right_norm = math.sqrt(sum(value * value for value in right))
        if left_norm == 0 or right_norm == 0:
            return 0.0
        return dot / (left_norm * right_norm)

    @staticmethod
    def _pair_weight(left_type: str, right_type: str) -> float:
        if left_type == "keyword" and right_type == "keyword":
            return settings.similarity_weight_keyword_keyword
        if left_type == "stock" and right_type == "stock":
            return settings.similarity_weight_stock_stock
        return settings.similarity_weight_keyword_stock

    @staticmethod
    def _candidate_pool_limit(limit: int) -> int:
        return max(limit * settings.retrieval_candidate_pool_multiplier, limit)

    def _base_pool_limit(self) -> int:
        primary_limit = max(self.onboarding_limit, self.behavior_limit)
        return self._candidate_pool_limit(primary_limit)
