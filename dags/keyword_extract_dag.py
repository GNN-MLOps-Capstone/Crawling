import io
import json
import logging
from datetime import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id='news_keyword_extraction',
    schedule_interval=None,
    start_date=datetime(2025, 12, 1, tzinfo=local_tz),
    catchup=False,
    render_template_as_native_obj=True,
    tags=['gold', 'llm']
)
def keyword_extraction_pipeline():
    @task.external_python(python=PYTHON_VENV_PATH)
    def extract_keywords_with_ollama(updated_dates, aws_conn_info):
        import io
        import json
        import ast
        import pandas as pd
        import boto3
        from ollama import Client

        # --- 1. 유틸리티: 문장 길이별 키워드 개수 결정 로직 ---
        def get_target_k_count(text_len):
            if text_len < 700:
                return 3
            elif text_len < 2300:
                return 4
            elif text_len < 4500:
                return 5
            else:
                return 6

        # --- 2. 유틸리티: 텍스트 분할 (Chunking) ---
        def chunk_text(text, max_chars=3000):
            if len(text) <= max_chars: return [text]
            chunks = []
            while text:
                if len(text) <= max_chars:
                    chunks.append(text);
                    break
                split_idx = text.rfind('.', 0, max_chars)
                if split_idx == -1: split_idx = max_chars
                chunks.append(text[:split_idx + 1])
                text = text[split_idx + 1:].strip()
            return chunks

        # --- 3. 유틸리티: 본문 존재 여부 검증 ---
        def verify_keywords(extracted_keywords, original_text):
            clean_text = "".join(original_text.split())
            return [k for k in extracted_keywords if "".join(k.split()) in clean_text]

        # Ollama 설정 (Docker Compose 서비스명 'ollama' 사용)
        OLLAMA_HOST = "http://ollama:11434"
        client = Client(host=OLLAMA_HOST, timeout=180)  # 긴 텍스트 대비 타임아웃 연장

        # [수정] 허깅페이스 Bllossom 모델 주소 적용
        MODEL = "hf.co/MLP-KTLim/llama-3-Korean-Bllossom-8B-gguf-Q4_K_M:Q4_K_M"

        s3_client = boto3.client('s3',
                                 aws_access_key_id=aws_conn_info['access_key'],
                                 aws_secret_access_key=aws_conn_info['secret_key'],
                                 endpoint_url=aws_conn_info['endpoint_url'])

        if isinstance(updated_dates, str):
            updated_dates = ast.literal_eval(updated_dates)

        for date_str in updated_dates:
            object_key = f'crawled-news/year={date_str[:4]}/month={date_str[5:7]}/day={date_str[8:10]}/data.parquet'

            try:
                response = s3_client.get_object(Bucket='silver', Key=object_key)
                df = pd.read_parquet(io.BytesIO(response['Body'].read()))

                final_results = []
                for _, row in df.iterrows():
                    full_text = str(row['text'])

                    # 1. 기사 길이에 따른 목표 키워드 수 산정
                    target_n = get_target_k_count(len(full_text))

                    # 2. 텍스트 분할 및 청크별 키워드 추출
                    chunks = chunk_text(full_text)
                    all_chunk_keywords = set()

                    for chunk in chunks:
                        try:
                            # [개선] JSON 포맷 강제 및 동적 개수 제한 프롬프트
                            res = client.chat(
                                model=MODEL,
                                format='json',
                                messages=[{
                                    'role': 'system',
                                    'content': f'당신은 뉴스 분석기입니다. 제공된 본문에서 핵심 명사 키워드를 최대 {target_n}개 뽑아 JSON 형식으로 응답하세요. 예: {{"keywords": ["단어1", "단어2"]}}'
                                }, {
                                    'role': 'user',
                                    'content': f"본문: {chunk}"
                                }],
                                options={'temperature': 0.1, 'num_ctx': 8192}
                            )
                            output_data = json.loads(res['message']['content'])
                            all_chunk_keywords.update(output_data.get('keywords', []))
                        except Exception as e:
                            print(f"⚠️ Chunk Error: {e}")

                    # 3. 사후 검증: 본문에 실제로 존재하는지 확인
                    verified = verify_keywords(list(all_chunk_keywords), full_text)

                    # 4. 중복 제거 및 최종 개수 제한 적용
                    unique_verified = list(dict.fromkeys(verified))
                    final_results.append(", ".join(unique_verified[:target_n]))

                df['keywords'] = final_results

                # Gold 버킷 저장
                out_buf = io.BytesIO()
                df.to_parquet(out_buf, index=False)
                s3_client.put_object(Bucket='gold', Key=object_key, Body=out_buf.getvalue())
                print(f"✅ {date_str} Gold 완료: {len(df)}건 처리")

            except Exception as e:
                print(f"❌ Date {date_str} Fail: {e}")

    @task(multiple_outputs=True)
    def prepare_params(**context):
        conf = context['dag_run'].conf
        return {
            "dates": conf.get('updated_dates', []),
            "aws": {
                "access_key": BaseHook.get_connection('MINIO_S3').login,
                "secret_key": BaseHook.get_connection('MINIO_S3').password,
                "endpoint_url": BaseHook.get_connection('MINIO_S3').extra_dejson.get('endpoint_url')
            }
        }

    params = prepare_params()
    extract_keywords_with_ollama(updated_dates=params['dates'], aws_conn_info=params['aws'])


keyword_extraction_dag = keyword_extraction_pipeline()