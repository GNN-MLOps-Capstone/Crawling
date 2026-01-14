import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset

# 🟢 [Module Import] 구조화된 모듈 경로 사용
from modules.ingestion.reader import read_daily_news
from modules.ingestion.writer import write_news_to_minio

# [설정]
local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = 'news_data_db'
MINIO_CONN_ID = 'MINIO_S3'
TARGET_BUCKET = 'bronze'

# [Dataset] 이 파이프라인이 완료되면 downstream DAG가 이를 감지함
BRONZE_DATASET = Dataset(f"s3://{TARGET_BUCKET}/crawled_news")

default_args = {
    'owner': 'dongbin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='news2minio_daily_v2',  # ID 업데이트
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=local_tz),
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['bronze', 'ingestion', 'structured']
)
def news_ingestion_pipeline():
    @task(outlets=[BRONZE_DATASET])
    def ingest_process(**context):
        # 1. Target Date 계산 (Yesterday)
        exec_date = context['logical_date'].in_timezone(local_tz)
        target_date = exec_date.subtract(days=1).to_date_string()
        print(f"🚀 [Ingestion] Start processing for: {target_date}")

        # 2. Extract (From Postgres)
        news_df = read_daily_news(target_date, POSTGRES_CONN_ID)

        if news_df.empty:
            print("💤 No data to process. Skipping upload.")
            return []

        # 3. Load (To MinIO)
        saved_files = write_news_to_minio(news_df, TARGET_BUCKET, MINIO_CONN_ID)

        return saved_files

    ingest_process()


dag_instance = news_ingestion_pipeline()