import io
from datetime import datetime
import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

# 가상환경 내의 파이썬 경로
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'

local_tz = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id='news_refinement',
    schedule_interval=None,
    start_date=datetime(2025, 12, 1, tzinfo=local_tz),
    catchup=False,
    render_template_as_native_obj=True,
    tags=['silver', 'refinement', 'external_python']
)
def news_refinement_pipeline():
    # 1. 실제 정제 로직 (가상환경 실행)
    @task.external_python(python=PYTHON_VENV_PATH)
    def refine_task_in_venv(updated_dates, aws_conn_info):
        import io
        import re
        import html
        import ast
        import pandas as pd
        import boto3
        try:
            import hanja
            from kiwipiepy import Kiwi
        except ImportError:
            return "Required libraries not found in venv"

        class NewsPreProcessor:
            def __init__(self):
                self.remove_patterns = [
                    r'\(.*?\)|\[.*?\]|\{.*?\}|<.*?>', r'\S*@\S*', r'http\S+|www\S+',
                    r'\S*=\S*', r'[가-힣]{2,4}\s?(기자|특파원)',
                    r'(연합뉴스|뉴스1|뉴시스|조선일보|중앙일보|동아일보|한겨레|한국일보|서울경제|매일경제|머니투데이|한국경제|경향신문|헤럴드경제|아시아경제|이데일리|데일리안|세계일보|국민일보|뉴스핌|파이낸셜뉴스)',
                    r'(무단전재\s*및\s*재배포\s*금지|저작권자[^.,\n]+|Copyright\s*ⓒ[^.,\n]+|끝\)|끝$)',
                    r'(사진\s*=\s*[^.,\n]+|관련기사|주요뉴스|이 시각 뉴스)[^.\n]*',
                    r'※.*|▶.*|★.*'
                ]

            def clean_text_basic(self, text):
                if not isinstance(text, str): return ""
                text = html.unescape(text)
                text = hanja.translate(text, 'substitution')
                for pat in self.remove_patterns:
                    text = re.sub(pat, '', text)
                text = re.sub(r'[^A-Za-z0-9가-힇.,\"\'\:\·\!\?\-\%\~\&]', ' ', text)
                text = re.sub(r'\s+', ' ', text).strip()
                return text

            def is_english_only(self, text):
                sentences = [s.strip() for s in text.split('.') if s.strip()]
                cleaned = [s for s in sentences if re.search(r'[가-힣]', s)]
                return '. '.join(cleaned) + ('.' if cleaned else '')

            def is_sports_news(self, title, text):
                content = (str(title) + " " + str(text)).lower()
                sports_kwd = ["야구", "농구", "축구", "골프", "e스포츠", "KBO"]
                context_kwd = ["경기", "시즌", "우승", "패배", "리그", "순위", "스코어"]
                has_sports = any(k in content for k in sports_kwd)
                return has_sports and sum(1 for k in context_kwd if k in content) >= 3

        processor = NewsPreProcessor()
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn_info['access_key'],
            aws_secret_access_key=aws_conn_info['secret_key'],
            endpoint_url=aws_conn_info['endpoint_url']
        )

        if isinstance(updated_dates, str):
            print(f"⚠️ updated_dates가 문자열로 들어왔습니다. 변환을 시도합니다: {updated_dates}")
            try:
                updated_dates = ast.literal_eval(updated_dates)
            except (ValueError, SyntaxError):
                # 만약 '[' 가 없는 단일 날짜 문자열인 경우 리스트로 감싸기
                updated_dates = [updated_dates]

        for date_str in updated_dates:
            print(f"🔎 Processing date string: '{date_str}' (type: {type(date_str)})")

            if not date_str or not isinstance(date_str, str) or '-' not in date_str:
                print(f"⚠️ Skip invalid date format: {date_str}")
                continue

            dt = date_str.split('-')

            if len(dt) < 3:
                print(f"⚠️ Unexpected date format (split count {len(dt)}): {date_str}")
                continue

            year, month, day = dt[0], dt[1], dt[2]
            object_key = f'crawled-news/year={year}/month={month}/day={day}/data.parquet'

            try:
                response = s3_client.get_object(Bucket='bronze', Key=object_key)
                df = pd.read_parquet(io.BytesIO(response['Body'].read()))

                trash_dfs = []
                df_initial = df.copy()
                df = df.drop_duplicates(subset=['text']).dropna(subset=['text'])

                # 중복 데이터 체크 로직 수정 (ID 기준)
                if len(df_initial) != len(df):
                    removed_mask = ~df_initial['crawled_news_id'].isin(df['crawled_news_id'])
                    df_trash_dup = df_initial[removed_mask].copy()
                    df_trash_dup['reason'] = 'duplicate_or_nan'
                    trash_dfs.append(df_trash_dup)

                df['text'] = df['text'].apply(processor.clean_text_basic)
                df['text'] = df['text'].apply(processor.is_english_only)

                mask_sports = df.apply(lambda r: processor.is_sports_news(r.get('title', ''), r['text']), axis=1)
                mask_short = df['text'].str.len() <= 20

                if mask_sports.any():
                    df_trash_sports = df[mask_sports].copy()
                    df_trash_sports['reason'] = 'sports_news'
                    trash_dfs.append(df_trash_sports)

                if mask_short.any():
                    df_trash_short = df[mask_short].copy()
                    df_trash_short['reason'] = 'short_text'
                    trash_dfs.append(df_trash_short)

                df_final = df[~(mask_sports | mask_short)].copy()

                # Silver 저장
                silver_buffer = io.BytesIO()
                df_final.to_parquet(silver_buffer, engine='pyarrow', index=False)
                s3_client.put_object(Bucket='silver', Key=object_key, Body=silver_buffer.getvalue())

                # Trash 저장
                if trash_dfs:
                    df_trash_total = pd.concat(trash_dfs, ignore_index=True)
                    trash_buffer = io.BytesIO()
                    df_trash_total.to_parquet(trash_buffer, engine='pyarrow', index=False)

                    # [수정 포인트] 경로 앞에 'trash/'를 붙여서 중복을 방지합니다.
                    trash_key = f"trash/{object_key}"

                    s3_client.put_object(
                        Bucket='silver',
                        Key=trash_key,  # 수정된 경로: trash/crawled-news/year=...
                        Body=trash_buffer.getvalue()
                    )
                    trash_count = len(df_trash_total)
                else:
                    trash_count = 0

                print(f"✅ {date_str}: Silver({len(df_final)}), Trash({len(df_trash_total) if trash_dfs else 0})")

            except Exception as e:
                print(f"❌ Error processing {date_str}: {e}")

        return f"Successfully processed: {updated_dates}"

    # 2. 파라미터를 준비하는 태스크 (Airflow 메인 환경 실행)
    @task(multiple_outputs=True)
    def prepare_params(**context):
        conf = context['dag_run'].conf
        dates = conf.get('updated_dates', [])

        # 날짜 리스트가 비어있으면 에러 방지를 위해 강제 종료하거나 로깅
        if not dates:
            logging.warning("⚠️ No updated_dates found in dag_run.conf")

        conn = BaseHook.get_connection('MINIO_S3')
        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": conn.extra_dejson.get('endpoint_url')
        }

        # 데이터를 딕셔너리로 반환 (다음 태스크로 전달됨)
        return {"dates": dates, "aws": aws_info}

    # --- DAG 구조 정의 (가장 중요한 부분!) ---
    params = prepare_params()

    # params에서 필요한 값을 꺼내어 refine_task_in_venv로 전달
    # refine_task_in_venv를 여기서 직접 호출하는 것이 TaskFlow API의 올바른 사용법입니다.
    refine_task_in_venv(
        updated_dates=params['dates'],
        aws_conn_info=params['aws']
    )


news_refinement_dag = news_refinement_pipeline()