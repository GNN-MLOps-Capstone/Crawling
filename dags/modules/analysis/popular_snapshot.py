from __future__ import annotations

from datetime import UTC, datetime

import psycopg2


def refresh_popular_snapshot(
    *,
    db_info: dict,
    snapshot_at: datetime | None = None,
    lookback_hours: int = 72,
    candidate_limit: int = 500,
    max_per_domain: int = 2,
    keep_hours: int = 72,
) -> dict[str, object]:
    effective_snapshot_at = snapshot_at or datetime.now(UTC)
    refresh_query = """
        DELETE FROM public.recommendation_popular_snapshot
        WHERE snapshot_at < %(snapshot_at)s - (%(keep_hours)s * INTERVAL '1 hour');

        WITH scored_candidates AS (
            SELECT
                %(snapshot_at)s::timestamptz AS snapshot_at,
                nn.news_id,
                COALESCE(
                    NULLIF(
                        regexp_replace(split_part(split_part(COALESCE(nn.url, ''), '//', 2), '/', 1), '^www\\.', ''),
                        ''
                    ),
                    'unknown'
                ) AS domain,
                COALESCE(
                    NULLIF(
                        regexp_replace(split_part(split_part(COALESCE(nn.url, ''), '//', 2), '/', 1), '^www\\.', ''),
                        ''
                    ),
                    'unknown'
                ) AS category,
                COUNT(*) FILTER (WHERE ie.event_type = 'content_open')::int AS open_count,
                (
                    LN(1 + COUNT(*) FILTER (WHERE ie.event_type = 'content_open'))
                    + 0.15 * (
                        1.0 / (
                            1.0 + GREATEST(EXTRACT(EPOCH FROM (%(snapshot_at)s::timestamptz - nn.pub_date)) / 3600.0, 0.0)
                        )
                    )
                )::double precision AS score
            FROM public.filtered_news fn
            JOIN public.naver_news nn
              ON nn.news_id = fn.news_id
            LEFT JOIN public.interaction_events ie
              ON ie.news_id = nn.news_id
             AND ie.event_type = 'content_open'
             AND COALESCE(ie.event_ts_client, ie.event_ts_server) >= %(snapshot_at)s::timestamptz - (%(lookback_hours)s * INTERVAL '1 hour')
            WHERE nn.pub_date >= %(snapshot_at)s::timestamptz - (%(lookback_hours)s * INTERVAL '1 hour')
            GROUP BY nn.news_id, nn.pub_date, nn.url
        ),
        ranked AS (
            SELECT
                snapshot_at,
                news_id,
                score,
                domain,
                category,
                open_count,
                ROW_NUMBER() OVER (
                    PARTITION BY domain
                    ORDER BY score DESC, news_id DESC
                ) AS domain_rank,
                ROW_NUMBER() OVER (
                    ORDER BY score DESC, news_id DESC
                ) AS global_rank
            FROM scored_candidates
            WHERE open_count > 0
        ),
        selected AS (
            SELECT
                snapshot_at,
                ROW_NUMBER() OVER (ORDER BY score DESC, news_id DESC) AS rank,
                news_id,
                score,
                domain,
                category,
                open_count,
                %(snapshot_at)s::timestamptz - (%(lookback_hours)s * INTERVAL '1 hour') AS window_start,
                %(snapshot_at)s::timestamptz AS window_end
            FROM ranked
            WHERE domain_rank <= %(max_per_domain)s
            ORDER BY score DESC, news_id DESC
            LIMIT %(candidate_limit)s
        )
        INSERT INTO public.recommendation_popular_snapshot (
            snapshot_at,
            rank,
            news_id,
            score,
            domain,
            category,
            open_count,
            window_start,
            window_end
        )
        SELECT
            snapshot_at,
            rank,
            news_id,
            score,
            domain,
            category,
            open_count,
            window_start,
            window_end
        FROM selected;
    """

    params = {
        "snapshot_at": effective_snapshot_at,
        "lookback_hours": lookback_hours,
        "candidate_limit": candidate_limit,
        "max_per_domain": max_per_domain,
        "keep_hours": keep_hours,
    }

    with psycopg2.connect(**db_info) as conn:
        with conn.cursor() as cur:
            cur.execute(refresh_query, params)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM public.recommendation_popular_snapshot
                WHERE snapshot_at = %s
                """,
                (effective_snapshot_at,),
            )
            inserted_count = int(cur.fetchone()[0])
        conn.commit()

    return {
        "snapshot_at": effective_snapshot_at.isoformat(),
        "inserted_count": inserted_count,
        "lookback_hours": lookback_hours,
        "candidate_limit": candidate_limit,
        "max_per_domain": max_per_domain,
    }
