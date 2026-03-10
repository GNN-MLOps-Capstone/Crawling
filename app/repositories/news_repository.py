from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import psycopg2

from app.core.config import settings


@dataclass(frozen=True)
class NewsCandidate:
    news_id: int
    title: str
    pub_date: datetime | None
    url: str
    category: str
    domain: str
    score: float | None = None
    snapshot_at: datetime | None = None


class NewsRepository:
    def fetch_popular_candidates(
        self,
        *,
        limit: int,
        hours: int,
        exclude_ids: set[int],
        blocked_domains: tuple[str, ...] = (),
    ) -> list[NewsCandidate]:
        query = """
            WITH latest_snapshot AS (
                SELECT MAX(snapshot_at) AS snapshot_at
                FROM recommendation_popular_snapshot
            )
            SELECT
                rps.snapshot_at,
                nn.news_id,
                nn.title,
                nn.pub_date,
                nn.url,
                COALESCE(rps.category, 'unknown') AS category,
                COALESCE(rps.domain, 'unknown') AS domain,
                rps.score
            FROM recommendation_popular_snapshot rps
            JOIN latest_snapshot ls
              ON ls.snapshot_at = rps.snapshot_at
            JOIN filtered_news fn
              ON fn.news_id = rps.news_id
            JOIN naver_news nn
              ON nn.news_id = rps.news_id
            WHERE nn.pub_date >= %s
            ORDER BY rps.rank ASC, rps.score DESC, rps.news_id DESC
            LIMIT %s
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (datetime.now(UTC) - timedelta(hours=hours), limit * 3))
                rows = cur.fetchall()

        candidates: list[NewsCandidate] = []
        latest_snapshot_at: datetime | None = None
        for snapshot_at, news_id, title, pub_date, url, category, domain, score in rows:
            if latest_snapshot_at is None:
                latest_snapshot_at = snapshot_at
            normalized_news_id = int(news_id)
            if normalized_news_id in exclude_ids:
                continue
            normalized_url = str(url or "")
            normalized_domain = str(domain or "").strip() or urlparse(normalized_url).netloc.replace("www.", "") or "unknown"
            if normalized_domain in blocked_domains:
                continue
            candidates.append(
                NewsCandidate(
                    news_id=normalized_news_id,
                    title=str(title or ""),
                    pub_date=pub_date,
                    url=normalized_url,
                    category=str(category or "unknown"),
                    domain=normalized_domain,
                    score=float(score) if score is not None else None,
                    snapshot_at=snapshot_at,
                )
            )
            if len(candidates) >= limit:
                break

        if not candidates or latest_snapshot_at is None:
            return []
        if self._is_stale_popular_snapshot(latest_snapshot_at):
            return []
        return candidates

    def fetch_user_onboarding_profile(self, *, user_id: int) -> dict[str, list[str]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT stock_id::varchar
                    FROM watchlist
                    WHERE user_id = %s
                    ORDER BY stock_id
                    """,
                    (user_id,),
                )
                stock_rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT keyword_id::varchar
                    FROM user_onboarding_keywords
                    WHERE user_id = %s
                    ORDER BY keyword_id
                    """,
                    (user_id,),
                )
                keyword_rows = cur.fetchall()

        stock_ids = [str(row[0]) for row in stock_rows]
        keyword_ids = [str(row[0]) for row in keyword_rows]
        if not stock_ids and not keyword_ids:
            return {}
        return {
            "stock_ids": stock_ids,
            "keyword_ids": keyword_ids,
        }

    def fetch_recent_actions(
        self,
        *,
        user_id: int,
        limit: int,
        dwell_threshold_seconds: int,
    ) -> list[dict[str, object]]:
        query = """
            WITH session_events AS (
                SELECT
                    news_id,
                    content_session_id,
                    MIN(CASE WHEN event_type = 'content_open' THEN event_ts_client END) AS view_ts,
                    MAX(CASE WHEN event_type = 'content_leave' THEN event_ts_client END) AS leave_ts,
                    MAX(event_ts_client) AS latest_event_ts
                FROM interaction_events
                WHERE user_id = %s::varchar
                  AND event_type IN ('content_open', 'content_leave')
                  AND news_id IS NOT NULL
                GROUP BY news_id, content_session_id
            ),
            qualified_sessions AS (
                SELECT
                    news_id,
                    COALESCE(leave_ts, latest_event_ts) AS action_ts,
                    EXTRACT(EPOCH FROM (COALESCE(leave_ts, latest_event_ts) - view_ts)) AS dwell_seconds
                FROM session_events
                WHERE view_ts IS NOT NULL
            )
            SELECT
                qs.news_id,
                qs.action_ts,
                qs.dwell_seconds,
                COALESCE(
                    ARRAY_AGG(DISTINCT nk.keyword_id::varchar)
                    FILTER (WHERE nk.keyword_id IS NOT NULL),
                    '{}'
                ) AS keyword_ids,
                COALESCE(
                    ARRAY_AGG(DISTINCT ns.stock_id::varchar)
                    FILTER (WHERE ns.stock_id IS NOT NULL),
                    '{}'
                ) AS stock_ids
            FROM qualified_sessions qs
            LEFT JOIN news_keyword_mapping nk ON nk.news_id = qs.news_id
            LEFT JOIN news_stock_mapping ns ON ns.news_id = qs.news_id
            WHERE qs.dwell_seconds >= %s
            GROUP BY qs.news_id, qs.action_ts, qs.dwell_seconds
            ORDER BY qs.action_ts DESC, qs.news_id DESC
            LIMIT %s
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (user_id, dwell_threshold_seconds, limit))
                rows = cur.fetchall()

        actions: list[dict[str, object]] = []
        for news_id, action_ts, dwell_seconds, keyword_ids, stock_ids in rows:
            actions.append(
                {
                    "news_id": int(news_id),
                    "timestamp": action_ts.isoformat() if action_ts is not None else None,
                    "dwell_seconds": float(dwell_seconds) if dwell_seconds is not None else None,
                    "keyword_ids": [str(item) for item in (keyword_ids or [])],
                    "stock_ids": [str(item) for item in (stock_ids or [])],
                }
            )
        return actions

    def fetch_latest_news_ids(self, *, limit: int, offset: int) -> list[int]:
        query = """
            SELECT fn.news_id
            FROM filtered_news fn
            JOIN naver_news nn ON nn.news_id = fn.news_id
            ORDER BY nn.pub_date DESC, fn.news_id DESC
            LIMIT %s OFFSET %s
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit, offset))
                rows = cur.fetchall()
        return [int(row[0]) for row in rows]

    def fetch_news_entities(self, *, news_ids: list[int]) -> dict[int, dict[str, list[str]]]:
        if not news_ids:
            return {}

        keyword_query = """
            SELECT news_id, keyword_id::varchar
            FROM news_keyword_mapping
            WHERE news_id = ANY(%s)
        """
        stock_query = """
            SELECT news_id, stock_id::varchar
            FROM news_stock_mapping
            WHERE news_id = ANY(%s)
        """

        entities: dict[int, dict[str, list[str]]] = {
            int(news_id): {"keyword_ids": [], "stock_ids": []}
            for news_id in news_ids
        }

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(keyword_query, (news_ids,))
                for news_id, keyword_id in cur.fetchall():
                    entities[int(news_id)]["keyword_ids"].append(str(keyword_id))
                cur.execute(stock_query, (news_ids,))
                for news_id, stock_id in cur.fetchall():
                    entities[int(news_id)]["stock_ids"].append(str(stock_id))

        return entities

    def fetch_entity_embeddings(
        self,
        *,
        entity_refs: list[tuple[str, str]],
    ) -> dict[tuple[str, str], list[float]]:
        if not entity_refs:
            return {}

        params = [(entity_id, entity_type) for entity_type, entity_id in entity_refs]
        query = """
            SELECT entity_id, LOWER(entity_type), gnn_embedding::text
            FROM test_service_embeddings
            WHERE (entity_id, LOWER(entity_type)) IN (
                SELECT entity_id, LOWER(entity_type)
                FROM UNNEST(%s::text[], %s::text[]) AS t(entity_id, entity_type)
            )
        """
        entity_ids = [entity_id for entity_id, _ in params]
        entity_types = [entity_type for _, entity_type in params]

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (entity_ids, entity_types))
                rows = cur.fetchall()

        embeddings: dict[tuple[str, str], list[float]] = {}
        for entity_id, entity_type, raw_embedding in rows:
            vector = self._parse_embedding(raw_embedding)
            if vector:
                embeddings[(str(entity_type), str(entity_id))] = vector
        return embeddings

    def fetch_recent_candidates(
        self,
        *,
        limit: int,
        hours: int,
        exclude_ids: set[int],
        stale_cutoff_minutes: int | None = None,
        blocked_domains: tuple[str, ...] = (),
    ) -> list[NewsCandidate]:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        stale_cutoff = None
        if stale_cutoff_minutes is not None:
            stale_cutoff = datetime.utcnow() - timedelta(minutes=stale_cutoff_minutes)
        query = """
            SELECT nn.news_id, nn.title, nn.pub_date, nn.url
            FROM filtered_news fn
            JOIN naver_news nn ON nn.news_id = fn.news_id
            WHERE nn.pub_date >= %s
            ORDER BY nn.pub_date DESC, nn.news_id DESC
            LIMIT %s
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (cutoff, limit * 4))
                rows = cur.fetchall()

        candidates: list[NewsCandidate] = []
        for row in rows:
            news_id = int(row[0])
            if news_id in exclude_ids:
                continue
            url = str(row[3] or "")
            domain = urlparse(url).netloc.replace("www.", "") if url else "unknown"
            if domain and domain in blocked_domains:
                continue
            if stale_cutoff is not None and row[2] is not None and row[2] < stale_cutoff:
                continue
            candidates.append(
                NewsCandidate(
                    news_id=news_id,
                    title=str(row[1] or ""),
                    pub_date=row[2],
                    url=url,
                    category="unknown",
                    domain=domain or "unknown",
                )
            )
            if len(candidates) >= limit:
                break
        return candidates

    @staticmethod
    def _connect():
        return psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )

    @staticmethod
    def _is_stale_popular_snapshot(snapshot_at: datetime) -> bool:
        aware_snapshot_at = snapshot_at if snapshot_at.tzinfo is not None else snapshot_at.replace(tzinfo=UTC)
        max_age = timedelta(minutes=settings.popular_snapshot_max_age_minutes)
        return datetime.now(UTC) - aware_snapshot_at.astimezone(UTC) > max_age

    @staticmethod
    def _parse_embedding(raw_embedding: object) -> list[float]:
        if raw_embedding is None:
            return []
        if isinstance(raw_embedding, (list, tuple)):
            return [float(value) for value in raw_embedding]

        text = str(raw_embedding).strip()
        if not text:
            return []
        text = text.strip("[]")
        if not text:
            return []
        return [float(value.strip()) for value in text.split(",") if value.strip()]
