import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook

# [설정]
local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = 'news_data_db'
MINIO_CONN_ID = 'MINIO_S3'
TARGET_BUCKET = 'bronze'
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'

BRONZE_DATASET = Dataset(f"s3://{TARGET_BUCKET}/crawled_news")

default_args = {
    'owner': 'dongbin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='news2minio_daily',
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['bronze', 'ingestion', 'external_python']
)
def news_ingestion_pipeline():
    @task.external_python(python=PYTHON_VENV_PATH, outlets=[BRONZE_DATASET])
    def ingest_process(target_date, db_info, aws_info, bucket_name):
        import sys
        # 모듈 경로를 인식시키기 위해 추가
        sys.path.append('/opt/airflow/dags')

        from modules.ingestion.reader import read_daily_news
        from modules.ingestion.writer import write_news_to_minio

        print(f"🚀 [Ingestion] Processing Target Date: {target_date}")

        # 1. Read (DB)
        news_df = read_daily_news(target_date, db_info)

        if news_df.empty:
            print("💤 No data to process.")
            return []

        # 2. Write (MinIO)
        saved_files = write_news_to_minio(news_df, bucket_name, aws_info)
        return saved_files

    # --- [데이터 준비] ---
    # 1. 날짜 계산
    target_date = "{{ data_interval_start.in_timezone('Asia/Seoul').to_date_string() }}"

    # 2. DB 정보 추출
    pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    db_info = {
        "host": pg_conn.host, "port": pg_conn.port,
        "user": pg_conn.login, "password": pg_conn.password,
        "dbname": pg_conn.schema
    }

    # 3. AWS 정보 추출
    aws_conn = BaseHook.get_connection(MINIO_CONN_ID)
    aws_info = {
        "access_key": aws_conn.login,
        "secret_key": aws_conn.password,
        "endpoint_url": aws_conn.extra_dejson.get('endpoint_url')
    }

    # Task 실행
    ingest_process(target_date, db_info, aws_info, TARGET_BUCKET)


dag_instance = news_ingestion_pipeline()