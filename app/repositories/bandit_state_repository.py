from __future__ import annotations

from dataclasses import dataclass

import psycopg2

from app.core.config import settings


@dataclass(frozen=True)
class BanditStateRow:
    path: str
    alpha: float
    beta: float


class BanditStateRepository:
    def fetch_global_posteriors(self, *, paths: list[str]) -> dict[str, BanditStateRow]:
        if not paths:
            return {}

        query = """
            SELECT path, alpha, beta
            FROM public.recommendation_bandit_state
            WHERE scope = 'global'
              AND user_id IS NULL
              AND path = ANY(%s)
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (paths,))
                rows = cur.fetchall()

        return {
            str(path): BanditStateRow(
                path=str(path),
                alpha=float(alpha or 0.0),
                beta=float(beta or 0.0),
            )
            for path, alpha, beta in rows
        }

    def fetch_user_posteriors(self, *, user_id: int, paths: list[str]) -> dict[str, BanditStateRow]:
        if not paths:
            return {}

        query = """
            SELECT path, alpha, beta
            FROM public.recommendation_bandit_state
            WHERE scope = 'user'
              AND user_id = %s
              AND path = ANY(%s)
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (user_id, paths))
                rows = cur.fetchall()

        return {
            str(path): BanditStateRow(
                path=str(path),
                alpha=float(alpha or 0.0),
                beta=float(beta or 0.0),
            )
            for path, alpha, beta in rows
        }

    @staticmethod
    def _connect():
        return psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
