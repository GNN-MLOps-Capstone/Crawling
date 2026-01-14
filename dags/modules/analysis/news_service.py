import io
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# 같은 폴더 내 모듈 import
from modules.analysis.preprocessor import NewsPreProcessor
from modules.analysis.news_embedding import add_embeddings_to_df


def run_refinement_process(updated_dates: list, aws_info: dict):
    """
    [Service Layer]
    S3 연결 -> 데이터 로드 -> 전처리 -> 임베딩 -> S3 저장
    모든 절차를 총괄합니다.
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_info['access_key'],
                             aws_secret_access_key=aws_info['secret_key'],
                             endpoint_url=aws_info['endpoint_url'])

    OLLAMA_HOST = "http://ollama:11434"
    processed_files = []

    for date_str in updated_dates:
        print(f"🔎 Processing date: {date_str}")
        if not date_str or '-' not in date_str: continue

        y, m, d = date_str.split('-')
        input_key = f'crawled_news/year={y}/month={m}/day={d}/data.parquet'
        output_key = f'refined_news/year={y}/month={m}/day={d}/data.parquet'

        try:
            # 1. Load (Bronze)
            try:
                response = s3_client.get_object(Bucket='bronze', Key=input_key)
                df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            except s3_client.exceptions.NoSuchKey:
                print(f"⚠️ Input data not found: {input_key}")
                continue

            # 2. Preprocess
            processor = NewsPreProcessor()
            df = df.drop_duplicates(subset=['text']).dropna(subset=['text'])
            df['refined_text'] = df['text'].apply(processor.clean_text_basic)
            df['refined_text'] = df['refined_text'].apply(processor.is_english_only)

            mask_sports = df['refined_text'].apply(processor.is_sports_news)
            mask_short = df['refined_text'].str.len() <= 20
            df_final = df[~(mask_sports | mask_short)].copy()

            # 3. Embedding
            df_embedded = add_embeddings_to_df(df_final, model_name="bge-m3", host=OLLAMA_HOST)

            # 4. Save (Silver)
            # PyArrow Schema & Save Logic
            out_cols = ['news_id', 'refined_text', 'news_embedding', 'pub_date']
            df_save = df_embedded[out_cols].copy()
            df_save['news_id'] = df_save['news_id'].astype('int64')
            df_save['pub_date'] = df_save['pub_date'].astype(str)

            arrow_schema = pa.schema([
                ('news_id', pa.int64()),
                ('refined_text', pa.string()),
                ('news_embedding', pa.list_(pa.float32())),
                ('pub_date', pa.string())
            ])

            out_buf = io.BytesIO()
            table = pa.Table.from_pandas(df_save, schema=arrow_schema)
            pq.write_table(table, out_buf, compression='SNAPPY')

            s3_client.put_object(Bucket='silver', Key=output_key, Body=out_buf.getvalue())
            print(f"✅ Saved: {output_key} ({len(df_save)} rows)")
            processed_files.append(output_key)

        except Exception as e:
            print(f"❌ Error processing {date_str}: {e}")
            raise e

    return processed_files