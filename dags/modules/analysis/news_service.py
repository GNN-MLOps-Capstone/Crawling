import io
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import hashlib
import psycopg2
from psycopg2.extras import execute_values  # 필수

from modules.analysis.preprocessor import NewsPreProcessor
from modules.analysis.news_embedding import add_embeddings_to_df


def run_refinement_process(updated_dates: list, aws_info: dict, db_info: dict):
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_info['access_key'],
                             aws_secret_access_key=aws_info['secret_key'],
                             endpoint_url=aws_info['endpoint_url'])

    OLLAMA_HOST = "http://ollama:11434"
    processed_files = []

    def generate_content_hash(text):
        if not isinstance(text, str): return ''
        clean_content = text.replace(' ', '').replace('\n', '').replace('\r', '').replace('\t', '')
        return hashlib.md5(clean_content.encode('utf-8')).hexdigest()

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

            if df.empty: continue

            print(f"  - Raw count: {len(df)}")
            df = df.dropna(subset=['text'])
            df['content_hash'] = df['text'].apply(generate_content_hash)

            # 파일 내부 중복 제거
            df = df.drop_duplicates(subset=['content_hash'], keep='first')

            if df.empty: continue

            # -----------------------------------------------------------
            # [수정됨] 명시적 중복 검사 로직 (Select -> Insert)
            # -----------------------------------------------------------
            unique_hashes = df['content_hash'].unique().tolist()
            target_hashes = set(unique_hashes)

            try:
                conn = psycopg2.connect(**db_info)
                cur = conn.cursor()

                # [디버깅용] 실제 테이블에 데이터가 몇 개나 있는지 확인
                cur.execute("SELECT count(*) FROM processed_content_hashes")
                total_rows = cur.fetchone()[0]
                print(f"  👀 [Debug] Total rows in DB table: {total_rows}")

                # 1. DB에 이미 존재하는 해시 조회 (SELECT)
                #    WHERE IN 절을 사용하여 현재 배치의 해시 중 DB에 있는 것을 찾음
                if len(unique_hashes) > 0:
                    query = "SELECT content_hash FROM processed_content_hashes WHERE content_hash IN %s"
                    cur.execute(query, (tuple(unique_hashes),))
                    existing_rows = cur.fetchall()
                    existing_hashes = {row[0] for row in existing_rows}
                else:
                    existing_hashes = set()

                print(f"  - Found {len(existing_hashes)} duplicates in DB.")

                # 2. 새로운 해시만 필터링 (Python 연산)
                new_hashes_to_process = target_hashes - existing_hashes

                # 3. 새로운 해시를 DB에 등록 (INSERT)
                if new_hashes_to_process:
                    insert_query = """
                                   INSERT INTO processed_content_hashes (content_hash)
                                   VALUES %s ON CONFLICT (content_hash) DO NOTHING \
                                   """
                    values = [(h,) for h in new_hashes_to_process]
                    execute_values(cur, insert_query, values)
                    conn.commit()  # 커밋 필수
                    print(f"  - Registered {len(new_hashes_to_process)} new hashes to DB.")

                conn.close()

                # 4. DataFrame 필터링
                initial_count = len(df)
                df = df[df['content_hash'].isin(new_hashes_to_process)].copy()

                dropped_count = initial_count - len(df)
                if dropped_count > 0:
                    print(f"  🔥 {dropped_count} duplicates skipped (Already processed).")

            except Exception as e:
                print(f"  ❌ DB Deduplication Error: {e}")
                raise e

            if df.empty:
                print("  💤 All data in this batch has been processed before. Skipping.")
                continue
            # -----------------------------------------------------------

            # 3. Preprocess
            processor = NewsPreProcessor()
            df['refined_text'] = df['text'].apply(processor.clean_text_basic)
            df['refined_text'] = df['refined_text'].apply(processor.is_english_only)

            mask_sports = df['refined_text'].apply(processor.is_sports_news)
            mask_short = df['refined_text'].str.len() <= 20
            df_final = df[~(mask_sports | mask_short)].copy()

            if df_final.empty:
                print("  ⚠️ No valid data after filtering.")
                continue

            # 4. Embedding
            df_embedded = add_embeddings_to_df(df_final, model_name="bge-m3", host=OLLAMA_HOST)

            # 5. Save
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