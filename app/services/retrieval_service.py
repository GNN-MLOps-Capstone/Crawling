from __future__ import annotations

import logging
from dataclasses import dataclass

from app.repositories.news_repository import NewsCandidate, NewsRepository
from app.services.context_builder import NormalizedRecommendContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetrievalResult:
    path_a: list[NewsCandidate]
    path_b: list[NewsCandidate]
    fallback_used: bool = False
    fallback_reason: str | None = None


@dataclass(frozen=True)
class RetrievalService:
    repository: NewsRepository
    base_pool_hours: int
    path_a_hours: int
    path_b_hours: int
    path_a_limit: int
    path_b_limit: int
    blocked_domains: tuple[str, ...] = ()

    def retrieve(
        self,
        *,
        context: NormalizedRecommendContext,
        exclude_ids: set[int],
    ) -> RetrievalResult:
        del context
        path_a_hours = min(self.path_a_hours, self.base_pool_hours)
        path_b_hours = min(self.path_b_hours, path_a_hours)
        path_a: list[NewsCandidate] = []
        path_b: list[NewsCandidate] = []
        try:
            path_a = self.repository.fetch_recent_candidates(
                limit=self.path_a_limit,
                hours=path_a_hours,
                exclude_ids=exclude_ids,
                blocked_domains=self.blocked_domains,
            )
        except Exception as exc:
            logger.warning("path A retrieval failed: %s", exc)

        path_b_failed = False
        try:
            path_b = self.repository.fetch_recent_candidates(
                limit=self.path_b_limit,
                hours=path_b_hours,
                exclude_ids=exclude_ids | {item.news_id for item in path_a},
                blocked_domains=self.blocked_domains,
            )
        except Exception as exc:
            logger.warning("path B retrieval failed: %s", exc)
            path_b_failed = True

        if path_a:
            return RetrievalResult(
                path_a=path_a,
                path_b=path_b,
                fallback_used=path_b_failed,
                fallback_reason="path_b_failed" if path_b_failed else None,
            )

        if path_b:
            return RetrievalResult(
                path_a=[],
                path_b=path_b,
                fallback_used=True,
                fallback_reason="path_a_failed_use_path_b",
            )

        try:
            latest = self.repository.fetch_latest_news_ids(limit=self.path_a_limit + self.path_b_limit, offset=0)
            path_a = [
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
                path_a=path_a,
                path_b=[],
                fallback_used=True,
                fallback_reason="retrieval_failed",
            )
        except Exception as exc:
            logger.warning("latest fallback retrieval failed: %s", exc)
            return RetrievalResult(
                path_a=[],
                path_b=[],
                fallback_used=True,
                fallback_reason="db_failed",
            )
