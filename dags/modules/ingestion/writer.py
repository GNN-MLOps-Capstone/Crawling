import pandas as pd
import s3fs


def _build_filesystem(aws_info: dict) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=aws_info['access_key'],
        secret=aws_info['secret_key'],
        client_kwargs={'endpoint_url': aws_info['endpoint_url']}
    )


def write_news_to_minio(df: pd.DataFrame, bucket_name: str, aws_info: dict) -> list:
    """
    [Target: MinIO] 순수 s3fs 사용 (Airflow 의존성 제거)
    """
    if df.empty:
        return []

    fs = _build_filesystem(aws_info)

    df['pub_date'] = pd.to_datetime(df['pub_date'])
    df['date_key'] = df['pub_date'].dt.date

    uploaded_paths = []

    for date_key in df['date_key'].unique():
        partition_df = df[df['date_key'] == date_key].copy()

        y, m, d = date_key.strftime('%Y'), date_key.strftime('%m'), date_key.strftime('%d')
        object_key = f'{bucket_name}/crawled_news/year={y}/month={m}/day={d}/data.parquet'

        # 기존 파일 있으면 병합 (Idempotency)
        if fs.exists(object_key):
            print(f"🔄 [Writer] Merging with existing: {object_key}")
            try:
                with fs.open(object_key, 'rb') as f:
                    existing_df = pd.read_parquet(f)
                combined_df = pd.concat([existing_df, partition_df])
                final_df = combined_df.drop_duplicates(subset=['news_id'], keep='last')
            except:
                final_df = partition_df
        else:
            final_df = partition_df

        if 'date_key' in final_df.columns:
            final_df = final_df.drop(columns=['date_key'])

        # 저장
        with fs.open(object_key, 'wb') as f:
            final_df.to_parquet(f, engine='pyarrow', index=False)

        uploaded_paths.append(object_key)
        print(f"✅ [Writer] Saved: {object_key} ({len(final_df)} rows)")

    return uploaded_paths


def write_news_window_to_minio(df: pd.DataFrame, bucket_name: str, aws_info: dict, object_key: str) -> list:
    """
    hourly/incremental Bronze 적재용 단일 object 저장.
    object_key는 bucket prefix를 제외한 key여야 한다.
    """
    if df.empty:
        return []

    fs = _build_filesystem(aws_info)
    full_path = f'{bucket_name}/{object_key}'
    save_df = df.copy()

    with fs.open(full_path, 'wb') as f:
        save_df.to_parquet(f, engine='pyarrow', index=False)

    print(f"✅ [Writer] Saved incremental window: {full_path} ({len(save_df)} rows)")
    return [object_key]
