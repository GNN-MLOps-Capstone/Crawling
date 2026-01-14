import io
import json
import pandas as pd
import boto3
from datetime import datetime


# tqdm은 함수 내부 import

def generate_embeddings_and_save_parquet(stock_json, aws_conn_info):
    """(NLP Env) 임베딩 생성 -> Parquet 저장"""
    from ollama import Client
    from tqdm import tqdm

    # ... (설정 부분 동일) ...
    OLLAMA_HOST = "http://ollama:11434"
    EMBEDDING_MODEL = "bge-m3"
    client = Client(host=OLLAMA_HOST)
    df = pd.DataFrame(json.loads(stock_json))

    if df.empty: return None

    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_conn_info['access_key'],
                             aws_secret_access_key=aws_conn_info['secret_key'],
                             endpoint_url=aws_conn_info['endpoint_url'])

    # 1. 임베딩 생성
    embeddings = []
    print(f"Embedding 생성 시작 ({len(df)}건)...")
    for text in tqdm(df['summary_text']):
        try:
            response = client.embeddings(model=EMBEDDING_MODEL, prompt=text)
            embeddings.append(response['embedding'])
        except Exception as e:
            embeddings.append(None)

    df['summary_embedding'] = embeddings
    df = df.dropna(subset=['summary_embedding'])

    # 2. Parquet 저장 (같은 환경에서 읽을 것이므로 최신 포맷 사용 가능)
    today = datetime.now().strftime("%Y%m%d")
    file_key = f"stock_embeddings/stock_embeddings_{today}.parquet"

    out_buffer = io.BytesIO()
    # engine='pyarrow' 필수
    df.to_parquet(out_buffer, index=False, engine='pyarrow')
    out_buffer.seek(0)

    s3_client.put_object(Bucket='silver', Key=file_key, Body=out_buffer.getvalue())
    print(f"✅ Parquet Saved: {file_key}")

    return file_key


def upload_parquet_to_postgres(file_key, aws_conn_info, db_conn_info):
    """
    (NLP Env 실행용)
    * 중요: PostgresHook 대신 psycopg2를 직접 사용합니다.
    """
    import psycopg2
    from psycopg2.extras import execute_batch

    if not file_key: return

    # 1. S3 로드
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_conn_info['access_key'],
                             aws_secret_access_key=aws_conn_info['secret_key'],
                             endpoint_url=aws_conn_info['endpoint_url'])

    print(f"Loading Parquet from {file_key}...")
    obj = s3_client.get_object(Bucket='silver', Key=file_key)

    # 여기서 읽는 환경 == 쓰는 환경 이므로 에러 없음
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

    # 2. DB 연결 (psycopg2 직접 사용)
    # db_conn_info 딕셔너리를 unpacking (**) 하여 연결
    conn = psycopg2.connect(**db_conn_info)
    cursor = conn.cursor()

    print(f"DB Update 시작 ({len(df)}건)...")

    query = "UPDATE public.stocks SET summary_embedding = %s WHERE stock_id = %s"
    data_tuples = []

    for _, row in df.iterrows():
        # numpy array -> list 변환
        embedding = row['summary_embedding'].tolist() if hasattr(row['summary_embedding'], 'tolist') else row[
            'summary_embedding']
        data_tuples.append((embedding, row['stock_id']))

    try:
        execute_batch(cursor, query, data_tuples, page_size=500)
        conn.commit()
        print(f"✅ 업데이트 완료")
    except Exception as e:
        conn.rollback()
        print(f"❌ DB Update Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()