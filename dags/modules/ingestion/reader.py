import pandas as pd
import psycopg2


def _build_conn_str(db_info: dict) -> str:
    return (
        f"host={db_info['host']} port={db_info['port']} dbname={db_info['dbname']} "
        f"user={db_info['user']} password={db_info['password']}"
    )


def _read_news(query: str, params, db_info: dict) -> pd.DataFrame:
    conn_str = _build_conn_str(db_info)

    try:
        with psycopg2.connect(conn_str) as conn:
            df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        print(f"❌ [Reader] DB Read Error: {e}")
        raise e


def read_daily_news(target_date: str, db_info: dict) -> pd.DataFrame:
    """
    [Source: Postgres] 순수 psycopg2 사용 (Airflow 의존성 제거)
    """
    # KST 기준 하루 범위 설정
    search_start = f"{target_date} 00:00:00"
    search_end = f"{target_date} 23:59:59"

    query = """
        SELECT 
            cn.news_id, 
            cn.text, 
            nn.pub_date 
        FROM public.crawled_news cn
        JOIN public.naver_news nn ON cn.news_id = nn.news_id
        WHERE cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' >= %s
          AND cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' <= %s
    """

    df = _read_news(query, (search_start, search_end), db_info)

    if df.empty:
        print(f"⚠️ [Reader] No data found for date: {target_date}")
        return pd.DataFrame()

    print(f"✅ [Reader] Fetched {len(df)} rows for {target_date}")
    return df


def read_news_by_time_window(window_start: str, window_end: str, db_info: dict) -> pd.DataFrame:
    """
    [Source: Postgres] created_at 기준 KST time window 조회
    """
    query = """
        SELECT
            cn.news_id,
            cn.text,
            nn.pub_date,
            cn.created_at
        FROM public.crawled_news cn
        JOIN public.naver_news nn ON cn.news_id = nn.news_id
        WHERE cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' >= %s
          AND cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' < %s
        ORDER BY cn.created_at ASC, cn.news_id ASC
    """

    df = _read_news(query, (window_start, window_end), db_info)

    if df.empty:
        print(f"⚠️ [Reader] No data found for window: {window_start} ~ {window_end}")
        return pd.DataFrame()

    print(f"✅ [Reader] Fetched {len(df)} rows for window: {window_start} ~ {window_end}")
    return df
