from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

logger = logging.getLogger(__name__)

PATH_CODE_TO_KEY = {
    "A1": "onboarding",
    "A2": "behavior",
    "B": "breaking",
    "C": "popular",
}


@dataclass(frozen=True)
class PosteriorRecord:
    scope: str
    path: str
    alpha: float
    beta: float
    user_id: int | None = None
    window_start: str | None = None
    window_end: str | None = None
    reward_count: int = 0
    impression_count: int = 0


GLOBAL_POSTERIORS_SQL = """
WITH params AS (
    SELECT
        %(window_end)s::timestamptz AS window_end,
        %(window_end)s::timestamptz - (%(lookback_hours)s::text || ' hours')::interval AS window_start
),
path_totals AS (
    SELECT
        m.path,
        SUM(m.dwell_5s_count)::bigint AS reward_count,
        SUM(m.impression_count)::bigint AS impression_count
    FROM public.recommendation_news_path_metrics m
    CROSS JOIN params p
    WHERE m.path IN ('A1', 'A2', 'B', 'C')
      AND m.bucket_start >= p.window_start::timestamp
      AND m.bucket_end <= p.window_end::timestamp
    GROUP BY m.path
)
SELECT
    pt.path,
    COALESCE(pt.reward_count, 0)::bigint AS reward_count,
    COALESCE(pt.impression_count, 0)::bigint AS impression_count
FROM path_totals pt
"""


USER_POSTERIORS_SQL = """
WITH params AS (
    SELECT
        %(window_end)s::timestamptz AS window_end,
        %(window_end)s::timestamptz - (%(lookback_hours)s::text || ' hours')::interval AS window_start
),
served_impressions AS (
    SELECT
        rs.request_id,
        rs.user_id::integer AS user_id,
        (item ->> 'news_id')::bigint AS news_id,
        item ->> 'path' AS path,
        ROW_NUMBER() OVER (
            PARTITION BY rs.request_id, rs.user_id::integer, (item ->> 'news_id')::bigint
            ORDER BY
                COALESCE(NULLIF(item ->> 'position', '')::integer, 2147483647),
                rs.id
        ) AS rn
    FROM public.recommendation_serves rs
    CROSS JOIN params p
    CROSS JOIN LATERAL jsonb_array_elements(rs.served_items) AS item
    WHERE rs.created_at >= p.window_start
      AND rs.created_at < p.window_end
      AND rs.request_id IS NOT NULL
      AND rs.user_id IS NOT NULL
      AND item ? 'news_id'
      AND item ? 'path'
      AND (item ->> 'news_id') ~ '^[0-9]+$'
      AND item ->> 'path' IN ('A1', 'A2', 'B', 'C')
),
dedup_impressions AS (
    SELECT
        request_id,
        user_id,
        news_id,
        path
    FROM served_impressions
    WHERE rn = 1
),
open_events AS (
    SELECT
        ie.request_id,
        ie.user_id::integer AS user_id,
        ie.news_id,
        MIN(COALESCE(ie.event_ts_client, ie.event_ts_server)) AS open_ts,
        ie.content_session_id
    FROM public.interaction_events ie
    CROSS JOIN params p
    WHERE ie.event_type = 'content_open'
      AND ie.event_ts_server >= p.window_start
      AND ie.event_ts_server < p.window_end
      AND ie.request_id IS NOT NULL
      AND ie.user_id IS NOT NULL
      AND ie.news_id IS NOT NULL
    GROUP BY
        ie.request_id,
        ie.user_id::integer,
        ie.news_id,
        ie.content_session_id
),
leave_events AS (
    SELECT
        ie.content_session_id,
        MIN(COALESCE(ie.event_ts_client, ie.event_ts_server)) AS leave_ts
    FROM public.interaction_events ie
    CROSS JOIN params p
    WHERE ie.event_type = 'content_leave'
      AND ie.event_ts_server >= p.window_start
      AND ie.event_ts_server < p.window_end + INTERVAL '1 hour'
      AND ie.content_session_id IS NOT NULL
    GROUP BY ie.content_session_id
),
dwell_rewards AS (
    SELECT DISTINCT
        oe.request_id,
        oe.user_id,
        oe.news_id
    FROM open_events oe
    JOIN leave_events le
      ON le.content_session_id = oe.content_session_id
    WHERE oe.content_session_id IS NOT NULL
      AND le.leave_ts >= oe.open_ts
      AND EXTRACT(EPOCH FROM (le.leave_ts - oe.open_ts)) >= %(dwell_threshold_seconds)s
),
user_path_totals AS (
    SELECT
        di.user_id,
        di.path,
        COUNT(*)::bigint AS impression_count,
        COUNT(dr.news_id)::bigint AS reward_count
    FROM dedup_impressions di
    LEFT JOIN dwell_rewards dr
      ON dr.request_id = di.request_id
     AND dr.user_id = di.user_id
     AND dr.news_id = di.news_id
    GROUP BY
        di.user_id,
        di.path
)
SELECT
    upt.user_id,
    upt.path,
    COALESCE(upt.reward_count, 0)::bigint AS reward_count,
    COALESCE(upt.impression_count, 0)::bigint AS impression_count
FROM user_path_totals upt
"""

UPSERT_GLOBAL_BANDIT_STATE_SQL = """
INSERT INTO public.recommendation_bandit_state (
    scope,
    user_id,
    path,
    alpha,
    beta,
    reward_count,
    impression_count,
    window_end,
    updated_at
)
VALUES (
    %(scope)s,
    %(user_id)s,
    %(path)s,
    %(alpha)s,
    %(beta)s,
    %(reward_count)s,
    %(impression_count)s,
    %(window_end)s::timestamptz,
    NOW()
)
ON CONFLICT (path) WHERE scope = 'global'
DO UPDATE SET
    alpha = EXCLUDED.alpha,
    beta = EXCLUDED.beta,
    reward_count = EXCLUDED.reward_count,
    impression_count = EXCLUDED.impression_count,
    window_end = EXCLUDED.window_end,
    updated_at = NOW()
"""

UPSERT_USER_BANDIT_STATE_SQL = """
INSERT INTO public.recommendation_bandit_state (
    scope,
    user_id,
    path,
    alpha,
    beta,
    reward_count,
    impression_count,
    window_end,
    updated_at
)
VALUES (
    %(scope)s,
    %(user_id)s,
    %(path)s,
    %(alpha)s,
    %(beta)s,
    %(reward_count)s,
    %(impression_count)s,
    %(window_end)s::timestamptz,
    NOW()
)
ON CONFLICT (user_id, path) WHERE scope = 'user'
DO UPDATE SET
    alpha = EXCLUDED.alpha,
    beta = EXCLUDED.beta,
    reward_count = EXCLUDED.reward_count,
    impression_count = EXCLUDED.impression_count,
    window_end = EXCLUDED.window_end,
    updated_at = NOW()
"""


def build_bandit_state_key(*, key_prefix: str, scope: str, path: str, user_id: int | None = None) -> str:
    if scope == "global":
        return f"{key_prefix}:global:{path}"
    if scope == "user" and user_id is not None:
        return f"{key_prefix}:user:{user_id}:{path}"
    raise ValueError(f"invalid bandit key scope={scope} user_id={user_id}")


def build_bandit_state_payload(record: PosteriorRecord) -> str:
    return json.dumps(
        {
            "alpha": record.alpha,
            "beta": record.beta,
            "scope": record.scope,
            "path": record.path,
            "user_id": record.user_id,
            "window_start": record.window_start,
            "window_end": record.window_end,
            "reward_count": record.reward_count,
            "impression_count": record.impression_count,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def rows_to_global_posteriors(
    rows: Iterable[tuple[str, int, int]],
    *,
    window_start: str,
    window_end: str,
) -> list[PosteriorRecord]:
    records: list[PosteriorRecord] = []
    for raw_path, reward_count, impression_count in rows:
        path = PATH_CODE_TO_KEY.get(raw_path)
        if path is None:
            continue
        reward = int(reward_count or 0)
        impressions = int(impression_count or 0)
        records.append(
            PosteriorRecord(
                scope="global",
                path=path,
                alpha=float(reward),
                beta=float(max(impressions - reward, 0)),
                window_start=window_start,
                window_end=window_end,
                reward_count=reward,
                impression_count=impressions,
            )
        )
    return records


def rows_to_user_posteriors(
    rows: Iterable[tuple[int, str, int, int]],
    *,
    window_start: str,
    window_end: str,
) -> list[PosteriorRecord]:
    records: list[PosteriorRecord] = []
    for user_id, raw_path, reward_count, impression_count in rows:
        path = PATH_CODE_TO_KEY.get(raw_path)
        if path is None:
            continue
        reward = int(reward_count or 0)
        impressions = int(impression_count or 0)
        records.append(
            PosteriorRecord(
                scope="user",
                user_id=int(user_id),
                path=path,
                alpha=float(reward),
                beta=float(max(impressions - reward, 0)),
                window_start=window_start,
                window_end=window_end,
                reward_count=reward,
                impression_count=impressions,
            )
        )
    return records


def update_bandit_posteriors(
    *,
    postgres_conn_id: str,
    window_end: str,
    lookback_hours: int,
    dwell_threshold_seconds: int,
) -> dict[str, int]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    window_start = _compute_window_start(window_end=window_end, lookback_hours=lookback_hours)
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)

    query_params = {
        "window_end": window_end,
        "lookback_hours": lookback_hours,
        "dwell_threshold_seconds": dwell_threshold_seconds,
    }
    global_rows = postgres.get_records(GLOBAL_POSTERIORS_SQL, parameters=query_params)
    user_rows = postgres.get_records(USER_POSTERIORS_SQL, parameters=query_params)

    global_records = rows_to_global_posteriors(
        global_rows,
        window_start=window_start,
        window_end=window_end,
    )
    user_records = rows_to_user_posteriors(
        user_rows,
        window_start=window_start,
        window_end=window_end,
    )

    global_upsert_rows = [
        {
            "scope": record.scope,
            "user_id": record.user_id,
            "path": record.path,
            "alpha": record.alpha,
            "beta": record.beta,
            "reward_count": record.reward_count,
            "impression_count": record.impression_count,
            "window_end": record.window_end,
        }
        for record in global_records
    ]
    user_upsert_rows = [
        {
            "scope": record.scope,
            "user_id": record.user_id,
            "path": record.path,
            "alpha": record.alpha,
            "beta": record.beta,
            "reward_count": record.reward_count,
            "impression_count": record.impression_count,
            "window_end": record.window_end,
        }
        for record in user_records
    ]
    if global_upsert_rows or user_upsert_rows:
        with postgres.get_conn() as conn:
            with conn.cursor() as cur:
                if global_upsert_rows:
                    cur.executemany(UPSERT_GLOBAL_BANDIT_STATE_SQL, global_upsert_rows)
                if user_upsert_rows:
                    cur.executemany(UPSERT_USER_BANDIT_STATE_SQL, user_upsert_rows)
            conn.commit()

    logger.info(
        "bandit posterior update completed to_db window_start=%s window_end=%s global=%s user=%s",
        window_start,
        window_end,
        len(global_records),
        len(user_records),
    )
    return {
        "global_records": len(global_records),
        "user_records": len(user_records),
    }


def _compute_window_start(*, window_end: str, lookback_hours: int) -> str:
    parsed = datetime.fromisoformat(window_end.replace("Z", "+00:00"))
    shifted = parsed.astimezone(timezone.utc) - timedelta(hours=lookback_hours)
    return shifted.isoformat()
