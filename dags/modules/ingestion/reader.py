import pandas as pd
import psycopg2


def read_daily_news(target_date: str, db_info: dict) -> pd.DataFrame:
    """
    [Source: Postgres] 순수 psycopg2 사용 (Airflow 의존성 제거)
    """
    conn_str = f"host={db_info['host']} port={db_info['port']} dbname={db_info['dbname']} user={db_info['user']} password={db_info['password']}"

    # KST 기준 하루 범위 설정
    search_start = f"{target_date} 00:00:00"
    search_end = f"{target_date} 23:59:59"

    query = f"""
        SELECT 
            cn.news_id, 
            cn.text, 
            nn.pub_date 
        FROM public.crawled_news cn
        JOIN public.naver_news nn ON cn.news_id = nn.news_id
        WHERE cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' >= '{search_start}'
          AND cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' <= '{search_end}'
    """

    try:
        with psycopg2.connect(conn_str) as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            print(f"⚠️ [Reader] No data found for date: {target_date}")
            return pd.DataFrame()

        print(f"✅ [Reader] Fetched {len(df)} rows for {target_date}")
        return df

    except Exception as e:
        print(f"❌ [Reader] DB Read Error: {e}")
        raise e