import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def read_daily_news(target_date: str, conn_id: str) -> pd.DataFrame:
    """
    [Source: Postgres]
    지정된 날짜(created_at)의 뉴스 데이터를 조회하여 DataFrame으로 반환합니다.
    """
    pg_hook = PostgresHook(postgres_conn_id=conn_id)

    search_start = f"{target_date} 00:00:00"
    search_end = f"{target_date} 23:59:59"

    # 필요한 컬럼만 명시적으로 조회
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
        df = pg_hook.get_pandas_df(query)
        if df.empty:
            print(f"⚠️ [Reader] No data found for date: {target_date}")
            return pd.DataFrame()

        print(f"✅ [Reader] Fetched {len(df)} rows from DB.")
        return df

    except Exception as e:
        print(f"❌ [Reader] DB Read Error: {e}")
        raise e