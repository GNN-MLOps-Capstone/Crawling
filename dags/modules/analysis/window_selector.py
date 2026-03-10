import pandas as pd
import psycopg2


DEFAULT_TARGET_STATUSES = (
    "pending",
    "failed_retryable",
    "processing",
)


def resolve_target_news_by_window(
    window_start: str,
    window_end: str,
    db_info: dict,
    statuses=DEFAULT_TARGET_STATUSES,
) -> pd.DataFrame:
    """
    created_at 기준 KST time window 내 후속 처리 대상 뉴스를 조회한다.
    hourly DAG에서는 이 결과의 news_id 목록만 XCom으로 전달하는 것을 권장한다.
    """
    conn_str = (
        f"host={db_info['host']} port={db_info['port']} dbname={db_info['dbname']} "
        f"user={db_info['user']} password={db_info['password']}"
    )
    query = """
        SELECT
            cn.news_id,
            cn.text,
            nn.pub_date,
            cn.filter_status::text AS filter_status,
            cn.filter_version,
            cn.created_at
        FROM public.crawled_news cn
        JOIN public.naver_news nn ON cn.news_id = nn.news_id
        WHERE cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' >= %s
          AND cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' < %s
          AND cn.filter_status::text = ANY(%s)
        ORDER BY cn.created_at ASC, cn.news_id ASC
    """

    params = (window_start, window_end, list(statuses))
    try:
        with psycopg2.connect(conn_str) as conn:
            df = pd.read_sql(query, conn, params=params)
    except Exception as e:
        print(f"❌ [WindowSelector] DB Read Error: {e}")
        raise e

    if df.empty:
        print(
            f"⚠️ [WindowSelector] No target news for window: {window_start} ~ {window_end}",
        )
        return pd.DataFrame()

    print(
        f"✅ [WindowSelector] Resolved {len(df)} rows for window: {window_start} ~ {window_end}",
    )
    return df


def resolve_target_news_ids_by_window(
    window_start: str,
    window_end: str,
    db_info: dict,
    statuses=DEFAULT_TARGET_STATUSES,
) -> list[int]:
    df = resolve_target_news_by_window(window_start, window_end, db_info, statuses=statuses)
    if df.empty:
        return []
    return df["news_id"].dropna().astype("int64").tolist()
