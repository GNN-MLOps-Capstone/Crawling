import io
import json
import pandas as pd
import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

# [환경 설정]
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
POSTGRES_CONN_ID = 'news_data_db'
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='embedding_initialization',
    schedule_interval=None,  # On-Demand (Manual)
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['initialization', 'vector', 'postgres', 'stock', 'keyword']
)
def initialization_pipeline():
    # ==============================================================================
    # 🟢 Section 1: Stock Summary Embedding
    # ==============================================================================

    @task
    def fetch_target_stocks():
        """(Main Env) 요약문은 있으나 임베딩이 없는 주식 데이터 조회"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # summary_text가 있고, summary_embedding이 비어있는 경우만 타겟
        sql = """
              SELECT stock_id, summary_text
              FROM public.stocks
              WHERE summary_text IS NOT NULL
                AND (summary_embedding IS NULL) \
              """
        df = pg_hook.get_pandas_df(sql)
        print(f"📉 Target Stocks: {len(df)}")
        return df.to_json(orient='records')

    @task.external_python(python=PYTHON_VENV_PATH)
    def make_stock_embedding(stock_json, aws_conn_info):
        """(NLP Env) Stock Summary 임베딩 생성 -> Parquet 저장"""
        import pandas as pd
        import boto3
        import io
        import json
        from ollama import Client
        from tqdm import tqdm
        from datetime import datetime

        if not stock_json: return None

        df = pd.DataFrame(json.loads(stock_json))
        if df.empty: return None

        # Setup
        OLLAMA_HOST = "http://ollama:11434"
        EMBEDDING_MODEL = "bge-m3"
        client = Client(host=OLLAMA_HOST, timeout=300)

        s3_client = boto3.client('s3',
                                 aws_access_key_id=aws_conn_info['access_key'],
                                 aws_secret_access_key=aws_conn_info['secret_key'],
                                 endpoint_url=aws_conn_info['endpoint_url'])

        # Embedding
        print("🧠 Generating Stock Embeddings...")
        embeddings = []
        for text in tqdm(df['summary_text']):
            try:
                response = client.embeddings(model=EMBEDDING_MODEL, prompt=text)
                embeddings.append(response['embedding'])
            except Exception as e:
                print(f"Error: {e}")
                embeddings.append(None)

        df['vector'] = embeddings  # Output Schema: vector
        df = df.dropna(subset=['vector'])

        # Output Schema: stock_id, vector
        df_save = df[['stock_id', 'vector']].copy()

        # Save Parquet (Spec v2.0)
        today = datetime.now().strftime("%Y%m%d")
        file_key = f"stock_embeddings/date={today}/stock_embeddings.parquet"

        buf = io.BytesIO()
        df_save.to_parquet(buf, index=False)

        s3_client.put_object(Bucket='silver', Key=file_key, Body=buf.getvalue())
        print(f"✅ Saved Stock Parquet: silver/{file_key}")

        return file_key

    @task
    def update_stock_db(file_key, aws_conn_info):
        """(Main Env) Parquet 로드 후 DB Update"""
        if not file_key: return

        import boto3
        import pandas as pd
        from psycopg2.extras import execute_batch
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Load Parquet
        s3 = boto3.client('s3',
                          aws_access_key_id=aws_conn_info['access_key'],
                          aws_secret_access_key=aws_conn_info['secret_key'],
                          endpoint_url=aws_conn_info['endpoint_url'])

        obj = s3.get_object(Bucket='silver', Key=file_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        # DB Update
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # stock_id 기준 vector 업데이트
        query = "UPDATE public.stocks SET summary_embedding = %s WHERE stock_id = %s"
        data = [(row['vector'], row['stock_id']) for _, row in df.iterrows()]

        try:
            print(f"Writing {len(data)} stock embeddings to DB...")
            execute_batch(cursor, query, data, page_size=500)
            conn.commit()
            print("✅ Stock DB Update Complete.")
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    # ==============================================================================
    # 🔵 Section 2: Keyword Embedding (Task [1])
    # ==============================================================================

    @task
    def fetch_target_keywords():
        """(Main Env) 임베딩이 없는 키워드 데이터 조회"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
              SELECT word
              FROM public.keywords
              WHERE embedding_vector IS NULL \
              """
        try:
            df = pg_hook.get_pandas_df(sql)
            print(f"📉 Target Keywords: {len(df)}")
            return df.to_json(orient='records')
        except Exception as e:
            print(f"⚠️ Keyword table access error (Maybe empty?): {e}")
            return "[]"

    @task.external_python(python=PYTHON_VENV_PATH)
    def make_keyword_embedding_init(keyword_json, aws_conn_info):
        """(NLP Env) Keyword 임베딩 생성 -> Parquet 저장"""
        import pandas as pd
        import boto3
        import io
        import json
        from ollama import Client
        from tqdm import tqdm
        from datetime import datetime

        if not keyword_json: return None
        df = pd.DataFrame(json.loads(keyword_json))
        if df.empty: return None

        # Setup
        OLLAMA_HOST = "http://ollama:11434"
        EMBEDDING_MODEL = "bge-m3"
        client = Client(host=OLLAMA_HOST, timeout=300)

        s3_client = boto3.client('s3',
                                 aws_access_key_id=aws_conn_info['access_key'],
                                 aws_secret_access_key=aws_conn_info['secret_key'],
                                 endpoint_url=aws_conn_info['endpoint_url'])

        # Embedding
        print("🧠 Generating Keyword Embeddings...")
        embeddings = []
        for kw in tqdm(df['keyword']):
            try:
                # 키워드는 짧으므로 별도 전처리 없이 바로 임베딩
                response = client.embeddings(model=EMBEDDING_MODEL, prompt=kw)
                embeddings.append(response['embedding'])
            except Exception as e:
                embeddings.append(None)

        df['vector'] = embeddings
        df = df.dropna(subset=['vector'])

        # Save Parquet (Spec v2.0)
        today = datetime.now().strftime("%Y%m%d")
        # Task [1] Output Schema: | keyword | vector |
        file_key = f"keyword_embeddings/date={today}/keyword_embeddings.parquet"

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)

        s3_client.put_object(Bucket='silver', Key=file_key, Body=buf.getvalue())
        print(f"✅ Saved Keyword Parquet: silver/{file_key}")

        return file_key

    @task
    def update_keyword_db(file_key, aws_conn_info):
        """(Main Env) Parquet 로드 후 DB Update (Keyword)"""
        if not file_key: return

        import boto3
        import pandas as pd
        from psycopg2.extras import execute_batch
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        s3 = boto3.client('s3',
                          aws_access_key_id=aws_conn_info['access_key'],
                          aws_secret_access_key=aws_conn_info['secret_key'],
                          endpoint_url=aws_conn_info['endpoint_url'])

        obj = s3.get_object(Bucket='silver', Key=file_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # keyword 기준 vector 업데이트
        query = "UPDATE public.keywords SET embedding_vector = %s WHERE word = %s"
        data = [(row['vector'], row['keyword']) for _, row in df.iterrows()]

        try:
            print(f"Writing {len(data)} keyword embeddings to DB...")
            execute_batch(cursor, query, data, page_size=1000)
            conn.commit()
            print("✅ Keyword DB Update Complete.")
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    # ==============================================================================
    # 🚀 Execution Flow
    # ==============================================================================

    # Common Connection Info
    conn = BaseHook.get_connection('MINIO_S3')
    aws_info = {
        "access_key": conn.login,
        "secret_key": conn.password,
        "endpoint_url": conn.extra_dejson.get('endpoint_url')
    }

    # 1. Stocks Flow
    raw_stocks = fetch_target_stocks()
    stock_parquet = make_stock_embedding(raw_stocks, aws_info)
    update_stock_db(stock_parquet, aws_info)

    # 2. Keywords Flow (Parallel)
    raw_keywords = fetch_target_keywords()
    keyword_parquet = make_keyword_embedding_init(raw_keywords, aws_info)
    update_keyword_db(keyword_parquet, aws_info)


embedding_init_dag = initialization_pipeline()