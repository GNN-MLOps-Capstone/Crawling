from __future__ import annotations

import psycopg2

from app.core.config import settings


class NewsRepository:
    def fetch_latest_news_ids(self, *, limit: int, offset: int) -> list[int]:
        query = """
            SELECT fn.news_id
            FROM filtered_news fn
            JOIN naver_news nn ON nn.news_id = fn.news_id
            ORDER BY nn.pub_date DESC, fn.news_id DESC
            LIMIT %s OFFSET %s
        """

        with psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit, offset))
                rows = cur.fetchall()
        return [int(row[0]) for row in rows]
