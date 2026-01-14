import io
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def write_news_to_minio(df: pd.DataFrame, bucket_name: str, conn_id: str) -> list:
    """
    [Target: MinIO]
    DataFrame을 날짜별 파티션(year/month/day)으로 나누어 적재(Upsert)합니다.
    """
    if df.empty:
        return []

    s3_hook = S3Hook(aws_conn_id=conn_id)

    # 파티셔닝 키 생성
    df['pub_date'] = pd.to_datetime(df['pub_date'])
    df['date_key'] = df['pub_date'].dt.date

    uploaded_paths = []

    for date_key in df['date_key'].unique():
        partition_df = df[df['date_key'] == date_key].copy()

        # Hive Style Partition Path 생성
        y, m, d = date_key.strftime('%Y'), date_key.strftime('%m'), date_key.strftime('%d')
        object_key = f'crawled_news/year={y}/month={m}/day={d}/data.parquet'

        # [Idempotency] 기존 데이터 병합 로직
        if s3_hook.check_for_key(object_key, bucket_name):
            print(f"🔄 [Writer] Merging with existing: {object_key}")
            try:
                obj = s3_hook.get_key(object_key, bucket_name)
                existing_df = pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))

                # 병합 및 중복 제거
                combined_df = pd.concat([existing_df, partition_df])
                final_df = combined_df.drop_duplicates(subset=['news_id'], keep='last')
            except Exception as e:
                print(f"⚠️ [Writer] Read Error (Overwrite): {e}")
                final_df = partition_df
        else:
            final_df = partition_df

        # 임시 컬럼 정리
        if 'date_key' in final_df.columns:
            final_df = final_df.drop(columns=['date_key'])

        # Upload
        out_buffer = io.BytesIO()
        final_df.to_parquet(out_buffer, engine='pyarrow', index=False)

        s3_hook.load_file_obj(
            file_obj=io.BytesIO(out_buffer.getvalue()),
            key=object_key,
            bucket_name=bucket_name,
            replace=True
        )
        uploaded_paths.append(object_key)
        print(f"✅ [Writer] Saved: {object_key} ({len(final_df)} rows)")

    return uploaded_paths