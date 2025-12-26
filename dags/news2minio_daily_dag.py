import io
import pandas as pd
from datetime import datetime, timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dagrun_operator import TriggerDagRunOperator

pg_hook = PostgresHook(postgres_conn_id='news_data_db')
conn_obj = pg_hook.get_connection(conn_id='news_data_db')

local_tz = pendulum.timezone("Asia/Seoul")

TARGET_MINIO_BUCKET = 'bronze'
POSTGRES_CONN_ID = 'news_data_db'
MINIO_CONN_ID = 'MINIO_S3'

default_args = {
    'owner': 'dongbin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='news_db_to_minio_daily',
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=local_tz),
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['bronze', 'news', 'taskflow']
)
def crawled_news_to_minio_sync():
    @task(task_id='upload_to_minio_parquet')
    def upload_to_minio_parquet(ds=None, **context):
        """
        PostgreSQL에서 업데이트된 뉴스 데이터를 추출하여
        MinIO 저장소에 Parquet 포맷으로 동기화합니다.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        exec_date = context['logical_date'].in_timezone(local_tz)
        target_date = exec_date.subtract(days=1).to_date_string()

        print(f"Target Date: {target_date}")

        search_start, search_end = f"{target_date} 00:00:00", f"{target_date} 23:59:59"
        print(f"DB 조회 범위: {search_start} ~ {search_end}")

        query = f"""
                    SELECT cn.*, nn.pub_date 
                    FROM public.crawled_news cn
                    JOIN public.naver_news nn ON cn.news_id = nn.news_id
                    WHERE cn.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' >= '{search_start}'
                      AND cn.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' <= '{search_end}'
                """
        new_df = pg_hook.get_pandas_df(query)

        if new_df.empty:
            return f"No updated data to sync for {target_date}"

        # 2. 뉴스 발행일(pub_date) 기반 파티셔닝 준비
        new_df['pub_date'] = pd.to_datetime(new_df['pub_date'])
        new_df['date_key'] = new_df['pub_date'].dt.date

        processed_dates = set()

        for target_date in new_df['date_key'].unique():
            daily_new_df = new_df[new_df['date_key'] == target_date].copy()

            # MinIO 저장 경로 (Hive 스타일 파티셔닝)
            year, month, day = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
            object_key = f'crawled-news/year={year}/month={month}/day={day}/data.parquet'

            # 3. 기존 MinIO 데이터와 병합 (Overwrite 방지 및 멱등성 유지)
            if s3_hook.check_for_key(object_key, TARGET_MINIO_BUCKET):
                print(f"Updating existing MinIO object: {object_key}")
                existing_obj = s3_hook.get_key(object_key, TARGET_MINIO_BUCKET)
                existing_df = pd.read_parquet(io.BytesIO(existing_obj.get()['Body'].read()))

                # 중복 제거: news_id 기준, 더 최신 updated_at 데이터 유지
                combined_df = pd.concat([existing_df, daily_new_df])
                final_df = combined_df.sort_values('updated_at').drop_duplicates('news_id', keep='last')
            else:
                final_df = daily_new_df

            # 4. MinIO로 최종 업로드
            buffer = io.BytesIO()
            final_df.to_parquet(buffer, engine='pyarrow', index=False)

            s3_hook.load_file_obj(
                file_obj=io.BytesIO(buffer.getvalue()),
                key=object_key,
                bucket_name=TARGET_MINIO_BUCKET,
                replace=True
            )

            processed_dates.add(target_date.strftime('%Y-%m-%d'))
            print(f"Sync complete: {len(final_df)} rows in {TARGET_MINIO_BUCKET}/{object_key}")

        return list(processed_dates)


    trigger_refinement = TriggerDagRunOperator(
        task_id='trigger_news_refinement',
        trigger_dag_id='news_refinement',
        conf={'updated_dates': "{{ task_instance.xcom_pull(task_ids='upload_to_minio_parquet') }}"},
        wait_for_completion=False
    )

    upload_to_minio_parquet() >> trigger_refinement

# DAG 인스턴스 생성
crawled_news_to_minio_sync_dag = crawled_news_to_minio_sync()