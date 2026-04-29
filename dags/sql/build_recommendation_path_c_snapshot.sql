{% set dag_conf = dag_run.conf if dag_run and dag_run.conf else {} %}
WITH params AS (
    SELECT
        '{{ dag_conf.get("window_end") or params.get("window_end") or data_interval_end.to_iso8601_string() }}'::timestamptz AS snapshot_at,
        '{{ dag_conf.get("window_end") or params.get("window_end") or data_interval_end.to_iso8601_string() }}'::timestamptz - INTERVAL '72 hours' AS window_start,
        500::integer AS candidate_limit,
        20.0::double precision AS prior_impressions
),
aggregated AS (
    SELECT
        m.news_id,
        SUM(m.impression_count)::bigint AS impression_count,
        SUM(m.click_count)::bigint AS click_count
    FROM public.recommendation_news_path_metrics m
    CROSS JOIN params p
    WHERE m.path IN ('A1', 'A2', 'B')
      AND m.bucket_start >= p.window_start::timestamp
      AND m.bucket_end <= p.snapshot_at::timestamp
    GROUP BY m.news_id
    HAVING SUM(m.impression_count) > 0
),
global_prior AS (
    SELECT
        COALESCE(SUM(click_count)::double precision / NULLIF(SUM(impression_count), 0), 0.0) AS prior_ctr
    FROM aggregated
),
scored AS (
    SELECT
        a.news_id,
        (
            a.click_count + (gp.prior_ctr * p.prior_impressions)
        ) / NULLIF(a.impression_count + p.prior_impressions, 0.0) AS score,
        a.click_count,
        a.impression_count
    FROM aggregated a
    CROSS JOIN global_prior gp
    CROSS JOIN params p
),
ranked AS (
    SELECT s.news_id
    FROM scored s
    ORDER BY
        s.score DESC,
        s.click_count DESC,
        s.impression_count DESC,
        s.news_id DESC
    LIMIT (SELECT candidate_limit FROM params)
),
cleanup AS (
    DELETE FROM public.recommendation_path_c_snapshot
    WHERE snapshot_at < (SELECT snapshot_at - INTERVAL '7 days' FROM params)
    RETURNING 1
)
INSERT INTO public.recommendation_path_c_snapshot (
    snapshot_at,
    news_ids
)
SELECT
    p.snapshot_at,
    COALESCE(ARRAY(SELECT r.news_id FROM ranked r), ARRAY[]::bigint[])
FROM params p
ON CONFLICT (snapshot_at)
DO UPDATE SET
    news_ids = EXCLUDED.news_ids,
    created_at = CURRENT_TIMESTAMP;
