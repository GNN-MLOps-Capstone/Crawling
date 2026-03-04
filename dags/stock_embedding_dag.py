import pendulum
from datetime import datetime
import sys

# DAG 레벨 경로 추가
sys.path.append('/opt/airflow/dags')
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
POSTGRES_CONN_ID = 'news_data_db'
local_tz = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id='stock_summary_embedding_init',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['initialization', 'vector', 'postgres', 'venv_all']
)
def embedding_pipeline():
    @task
    def fetch_target_stocks():
        """(Main Env) 대상 조회"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = "SELECT stock_id, summary_text FROM public.stocks WHERE summary_text IS NOT NULL"
        df = pg_hook.get_pandas_df(sql)
        print(f"Target count: {len(df)}")
        return df.to_json(orient='records')

    # [Task 1] 임베딩 생성 (VENV)
    @task.external_python(python=PYTHON_VENV_PATH)
    def task_generate_embeddings(stock_json, aws_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.stock_embedding import generate_embeddings_and_save_parquet
        return generate_embeddings_and_save_parquet(stock_json, aws_conn_info)

    # [Task 2] DB 업로드
    @task.external_python(python=PYTHON_VENV_PATH)
    def task_update_db(file_key, aws_conn_info, db_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        # 모듈 불러오기
        from modules.analysis.stock_embedding import upload_parquet_to_postgres

        # 실행
        upload_parquet_to_postgres(file_key, aws_conn_info, db_conn_info)

    # --- 실행 흐름 ---
    # 1. AWS 정보
    aws_info = {
        "access_key": BaseHook.get_connection('MINIO_S3').login,
        "secret_key": BaseHook.get_connection('MINIO_S3').password,
        "endpoint_url": BaseHook.get_connection('MINIO_S3').extra_dejson.get('endpoint_url')
    }

    # 2. DB 접속 정보 추출 (Main Env에서 미리 꺼내서 전달)
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    db_info = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password
    }

    target_data = fetch_target_stocks()
    parquet_key = task_generate_embeddings(target_data, aws_info)

    # DB 정보(db_info)를 인자로 넘겨줍니다
    task_update_db(parquet_key, aws_info, db_info)


embedding_dag = embedding_pipeline()