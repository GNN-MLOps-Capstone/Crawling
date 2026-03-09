from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
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


class NewsRepository:
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

    def fetch_recent_candidates(
        self,
        *,
        limit: int,
        hours: int,
        exclude_ids: set[int],
        blocked_domains: tuple[str, ...] = (),
    ) -> list[NewsCandidate]:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
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
