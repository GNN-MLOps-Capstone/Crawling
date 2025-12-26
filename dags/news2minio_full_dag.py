import io
import pandas as pd
from datetime import datetime, timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 타임존 설정
local_tz = pendulum.timezone("Asia/Seoul")

# 상수 설정
TARGET_MINIO_BUCKET = 'bronze'
POSTGRES_CONN_ID = 'news_data_db'
MINIO_CONN_ID = 'MINIO_S3'

default_args = {
    'owner': 'dongbin',
    'retries': 1,  # 전체 동기화는 실패 시 수동 개입이 나으므로 리트라이 축소
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='news_db_to_minio_full_sync',  # ID 변경으로 기존 데일리 DAG와 구분
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=local_tz),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=['bronze', 'news', 'full_sync']
)
def news_db_to_minio_full_sync():
    @task(task_id='upload_full_data_to_minio')
    def upload_full_data_to_minio():
        """
        PostgreSQL의 모든 뉴스 데이터를 가져와서
        발행일(pub_date) 기준으로 파티셔닝하여 MinIO에 업로드합니다.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        # 1. 전체 데이터 쿼리 (시간 제한 조건 제거)
        print("🚀 DB에서 전체 데이터를 조회합니다. 데이터 양에 따라 시간이 걸릴 수 있습니다...")
        query = """
                SELECT cn.*, nn.pub_date
                FROM public.crawled_news cn
                         JOIN public.naver_news nn ON cn.news_id = nn.news_id \
                """
        all_df = pg_hook.get_pandas_df(query)

        if all_df.empty:
            print("⚠️ DB에 데이터가 없습니다.")
            return "No data found"

        print(f"✅ 총 {len(all_df)} 건의 데이터를 로드했습니다. 파티셔닝을 시작합니다.")

        # 2. 날짜별 파티셔닝 준비
        all_df['pub_date'] = pd.to_datetime(all_df['pub_date'])
        all_df['date_key'] = all_df['pub_date'].dt.date

        # 3. 날짜별 루프 실행
        unique_dates = sorted(all_df['date_key'].unique())
        print(f"📅 총 {len(unique_dates)} 개의 날짜 파티션을 처리합니다.")

        for target_date in unique_dates:
            daily_df = all_df[all_df['date_key'] == target_date].copy()

            year = target_date.strftime('%Y')
            month = target_date.strftime('%m')
            day = target_date.strftime('%d')
            object_key = f'crawled-news/year={year}/month={month}/day={day}/data.parquet'

            # 기존 데이터가 있으면 병합, 없으면 신규 생성 (Idempotency 유지)
            if s3_hook.check_for_key(object_key, TARGET_MINIO_BUCKET):
                print(f"🔄 기존 데이터 병합 중: {object_key}")
                existing_obj = s3_hook.get_key(object_key, TARGET_MINIO_BUCKET)
                existing_df = pd.read_parquet(io.BytesIO(existing_obj.get()['Body'].read()))

                # 중복 제거 (가장 최신 수집본 유지)
                combined_df = pd.concat([existing_df, daily_df])
                final_df = combined_df.sort_values('updated_at').drop_duplicates('news_id', keep='last')
            else:
                final_df = daily_df

            # 4. Parquet 변환 및 업로드
            buffer = io.BytesIO()
            final_df.to_parquet(buffer, engine='pyarrow', index=False)

            s3_hook.load_file_obj(
                file_obj=io.BytesIO(buffer.getvalue()),
                key=object_key,
                bucket_name=TARGET_MINIO_BUCKET,
                replace=True
            )
            print(f"✔️ {target_date} 완료 ({len(final_df)} 건)")

        print("🎊 전체 데이터 동기화가 완료되었습니다!")

    upload_full_data_to_minio()


# DAG 실행
news_full_sync_dag = news_db_to_minio_full_sync()