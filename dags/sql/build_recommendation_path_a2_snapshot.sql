{% set dag_conf = dag_run.conf if dag_run and dag_run.conf else {} %}
WITH params AS (
    SELECT
        '{{ dag_conf.get("window_start") or params.get("window_start") or data_interval_start.to_iso8601_string() }}'::timestamptz AS window_start,
        '{{ dag_conf.get("window_end") or params.get("window_end") or data_interval_end.to_iso8601_string() }}'::timestamptz AS snapshot_at,
        20::integer AS per_user_limit,
        10::double precision AS dwell_threshold_seconds
),
changed_users AS (
    SELECT DISTINCT
        ie.user_id::integer AS user_id
    FROM public.interaction_events ie
    CROSS JOIN params p
    WHERE ie.event_ts_server >= p.window_start
      AND ie.event_ts_server < p.snapshot_at
      AND ie.event_type IN ('content_open', 'content_leave', 'content_heartbeat')
      AND ie.user_id IS NOT NULL
),
open_events AS (
    SELECT
        ie.user_id::integer AS user_id,
        ie.news_id,
        ie.content_session_id,
        MIN(COALESCE(ie.event_ts_client, ie.event_ts_server)) AS view_ts
    FROM public.interaction_events ie
    JOIN changed_users cu
      ON cu.user_id = ie.user_id::integer
    WHERE ie.event_type = 'content_open'
      AND ie.news_id IS NOT NULL
      AND ie.content_session_id IS NOT NULL
      AND ie.user_id IS NOT NULL
    GROUP BY
        ie.user_id::integer,
        ie.news_id,
        ie.content_session_id
),
session_activity AS (
    SELECT
        ie.user_id::integer AS user_id,
        ie.content_session_id,
        MAX(COALESCE(ie.event_ts_client, ie.event_ts_server)) AS latest_event_ts
    FROM public.interaction_events ie
    JOIN changed_users cu
      ON cu.user_id = ie.user_id::integer
    WHERE ie.event_type IN ('content_open', 'content_leave', 'content_heartbeat')
      AND ie.content_session_id IS NOT NULL
      AND ie.user_id IS NOT NULL
      AND COALESCE(ie.event_ts_client, ie.event_ts_server) <= (SELECT snapshot_at FROM params)
    GROUP BY
        ie.user_id::integer,
        ie.content_session_id
),
qualified_sessions AS (
    SELECT
        oe.user_id,
        oe.news_id,
        sa.latest_event_ts AS action_ts,
        EXTRACT(EPOCH FROM (sa.latest_event_ts - oe.view_ts))::double precision AS dwell_seconds
    FROM open_events oe
    JOIN session_activity sa
      ON sa.user_id = oe.user_id
     AND sa.content_session_id = oe.content_session_id
    CROSS JOIN params p
    WHERE sa.latest_event_ts >= oe.view_ts
      AND EXTRACT(EPOCH FROM (sa.latest_event_ts - oe.view_ts)) >= p.dwell_threshold_seconds
),
session_entities AS (
    SELECT
        qs.user_id,
        qs.news_id,
        qs.action_ts,
        qs.dwell_seconds,
        COALESCE(
            ARRAY_AGG(DISTINCT nk.keyword_id::varchar)
            FILTER (WHERE nk.keyword_id IS NOT NULL),
            ARRAY[]::varchar[]
        ) AS keyword_ids,
        COALESCE(
            ARRAY_AGG(DISTINCT ns.stock_id::varchar)
            FILTER (WHERE ns.stock_id IS NOT NULL),
            ARRAY[]::varchar[]
        ) AS stock_ids
    FROM qualified_sessions qs
    LEFT JOIN public.news_keyword_mapping nk
      ON nk.news_id = qs.news_id
    LEFT JOIN public.news_stock_mapping ns
      ON ns.news_id = qs.news_id
    GROUP BY
        qs.user_id,
        qs.news_id,
        qs.action_ts,
        qs.dwell_seconds
),
ranked_actions AS (
    SELECT
        se.*,
        ROW_NUMBER() OVER (
            PARTITION BY se.user_id
            ORDER BY se.action_ts DESC, se.news_id DESC
        ) AS row_num
    FROM session_entities se
),
per_user_items AS (
    SELECT
        ra.user_id,
        jsonb_agg(
            jsonb_build_object(
                'news_id', ra.news_id,
                'timestamp', ra.action_ts,
                'dwell_seconds', ra.dwell_seconds,
                'keyword_ids', to_jsonb(ra.keyword_ids),
                'stock_ids', to_jsonb(ra.stock_ids)
            )
            ORDER BY ra.action_ts DESC, ra.news_id DESC
        ) AS items
    FROM ranked_actions ra
    CROSS JOIN params p
    WHERE ra.row_num <= p.per_user_limit
    GROUP BY ra.user_id
)
INSERT INTO public.recommendation_path_a2_snapshot (
    user_id,
    items,
    snapshot_at
)
SELECT
    cu.user_id,
    COALESCE(pui.items, '[]'::jsonb) AS items,
    p.snapshot_at
FROM changed_users cu
CROSS JOIN params p
LEFT JOIN per_user_items pui
  ON pui.user_id = cu.user_id
ON CONFLICT (user_id)
DO UPDATE SET
    items = EXCLUDED.items,
    snapshot_at = EXCLUDED.snapshot_at;
