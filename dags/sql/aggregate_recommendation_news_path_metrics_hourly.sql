{% set dag_conf = dag_run.conf if dag_run and dag_run.conf else {} %}
WITH params AS (
    SELECT
        '{{ dag_conf.get("window_start") or params.get("window_start") or data_interval_start.to_iso8601_string() }}'::timestamptz AS window_start,
        '{{ dag_conf.get("window_end") or params.get("window_end") or data_interval_end.to_iso8601_string() }}'::timestamptz AS window_end
),
served_impressions AS (
    SELECT
        p.window_start AS bucket_start,
        p.window_end AS bucket_end,
        rs.request_id,
        rs.user_id,
        (item ->> 'news_id')::bigint AS news_id,
        item ->> 'path' AS path,
        ROW_NUMBER() OVER (
            PARTITION BY rs.request_id, (item ->> 'news_id')::bigint
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
      AND item ? 'news_id'
      AND item ? 'path'
      AND (item ->> 'news_id') ~ '^[0-9]+$'
      AND item ->> 'path' IN ('A1', 'A2', 'B', 'C')
),
dedup_impressions AS (
    SELECT
        bucket_start,
        bucket_end,
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
        ie.user_id,
        ie.news_id,
        MIN(COALESCE(ie.event_ts_client, ie.event_ts_server)) AS open_ts,
        ie.content_session_id
    FROM public.interaction_events ie
    CROSS JOIN params p
    WHERE ie.event_type = 'content_open'
      AND ie.event_ts_server >= p.window_start
      AND ie.event_ts_server < p.window_end
      AND ie.request_id IS NOT NULL
      AND ie.news_id IS NOT NULL
    GROUP BY
        ie.request_id,
        ie.user_id,
        ie.news_id,
        ie.content_session_id
),
click_metrics AS (
    SELECT
        oe.request_id,
        oe.user_id,
        oe.news_id,
        1::bigint AS click_count
    FROM open_events oe
    GROUP BY
        oe.request_id,
        oe.user_id,
        oe.news_id
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
dwell_sessions AS (
    SELECT
        oe.request_id,
        oe.user_id,
        oe.news_id,
        oe.content_session_id,
        GREATEST(
            EXTRACT(EPOCH FROM (le.leave_ts - oe.open_ts)),
            0
        )::double precision AS dwell_seconds
    FROM open_events oe
    JOIN leave_events le
      ON le.content_session_id = oe.content_session_id
    WHERE oe.content_session_id IS NOT NULL
      AND le.leave_ts >= oe.open_ts
),
dwell_metrics AS (
    SELECT
        ds.request_id,
        ds.user_id,
        ds.news_id,
        COUNT(*)::bigint AS dwell_event_count,
        COUNT(CASE WHEN ds.dwell_seconds >= 5 THEN 1 END)::bigint AS dwell_5s_count,
        COUNT(CASE WHEN ds.dwell_seconds >= 30 THEN 1 END)::bigint AS dwell_30s_count,
        COALESCE(SUM(LN(1 + ds.dwell_seconds)), 0)::double precision AS sum_log_dwell_time
    FROM dwell_sessions ds
    GROUP BY
        ds.request_id,
        ds.user_id,
        ds.news_id
),
path_metrics AS (
    SELECT
        di.bucket_start,
        di.bucket_end,
        di.news_id,
        di.path,
        COUNT(*)::bigint AS impression_count,
        COALESCE(SUM(cm.click_count), 0)::bigint AS click_count,
        COALESCE(SUM(dm.dwell_event_count), 0)::bigint AS dwell_event_count,
        COALESCE(SUM(dm.dwell_5s_count), 0)::bigint AS dwell_5s_count,
        COALESCE(SUM(dm.dwell_30s_count), 0)::bigint AS dwell_30s_count,
        COALESCE(SUM(dm.sum_log_dwell_time), 0)::double precision AS sum_log_dwell_time
    FROM dedup_impressions di
    LEFT JOIN click_metrics cm
      ON cm.request_id = di.request_id
     AND cm.user_id = di.user_id
     AND cm.news_id = di.news_id
    LEFT JOIN dwell_metrics dm
      ON dm.request_id = di.request_id
     AND dm.user_id = di.user_id
     AND dm.news_id = di.news_id
    GROUP BY
        di.bucket_start,
        di.bucket_end,
        di.news_id,
        di.path
),
all_metrics AS (
    SELECT
        bucket_start,
        bucket_end,
        news_id,
        path,
        impression_count,
        click_count,
        dwell_5s_count,
        dwell_30s_count,
        dwell_event_count,
        sum_log_dwell_time
    FROM path_metrics

    UNION ALL

    SELECT
        bucket_start,
        bucket_end,
        news_id,
        'TOTAL' AS path,
        SUM(impression_count)::bigint AS impression_count,
        SUM(click_count)::bigint AS click_count,
        SUM(dwell_5s_count)::bigint AS dwell_5s_count,
        SUM(dwell_30s_count)::bigint AS dwell_30s_count,
        SUM(dwell_event_count)::bigint AS dwell_event_count,
        SUM(sum_log_dwell_time)::double precision AS sum_log_dwell_time
    FROM path_metrics
    GROUP BY
        bucket_start,
        bucket_end,
        news_id
)
INSERT INTO public.recommendation_news_path_metrics (
    bucket_start,
    bucket_end,
    news_id,
    path,
    impression_count,
    click_count,
    dwell_5s_count,
    dwell_30s_count,
    dwell_event_count,
    sum_log_dwell_time,
    updated_at
)
SELECT
    bucket_start,
    bucket_end,
    news_id,
    path,
    impression_count,
    click_count,
    dwell_5s_count,
    dwell_30s_count,
    dwell_event_count,
    sum_log_dwell_time,
    CURRENT_TIMESTAMP
FROM all_metrics
ON CONFLICT (bucket_start, bucket_end, news_id, path)
DO UPDATE SET
    impression_count = EXCLUDED.impression_count,
    click_count = EXCLUDED.click_count,
    dwell_5s_count = EXCLUDED.dwell_5s_count,
    dwell_30s_count = EXCLUDED.dwell_30s_count,
    dwell_event_count = EXCLUDED.dwell_event_count,
    sum_log_dwell_time = EXCLUDED.sum_log_dwell_time,
    updated_at = CURRENT_TIMESTAMP;
