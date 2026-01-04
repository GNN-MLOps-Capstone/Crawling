import io
import re
import html
import logging
from datetime import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 가상환경 내의 파이썬 경로
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id='news_refinement_full_refresh',
    schedule_interval=None,  # 수동 실행 전용
    start_date=datetime(2025, 12, 1, tzinfo=local_tz),
    catchup=False,
    tags=['silver', 'full-refresh', 'external_python']
)
def news_refinement_full_pipeline():
    # 1. Bronze 버킷의 모든 날짜 파티션을 리스트업하는 태스크
    @task
    def list_all_bronze_dates():
        s3_hook = S3Hook(aws_conn_id='MINIO_S3')
        bucket_name = 'bronze'
        prefix = 'crawled-news/'

        # 모든 parquet 파일 키 가져오기
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if not keys:
            return []

        # 경로에서 날짜 정보(YYYY-MM-DD) 추출
        # 예: crawled-news/year=2025/month=12/day=22/data.parquet
        date_pattern = re.compile(r'year=(\d{4})/month=(\d{2})/day=(\d{2})')
        dates = set()
        for key in keys:
            match = date_pattern.search(key)
            if match:
                dates.add(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")

        sorted_dates = sorted(list(dates))
        logging.info(f"🔎 총 {len(sorted_dates)}개의 날짜 파티션을 발견했습니다.")
        return sorted_dates

    # 2. AWS 연결 정보를 가져오는 태스크
    @task
    def get_aws_info():
        conn = BaseHook.get_connection('MINIO_S3')
        return {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": conn.extra_dejson.get('endpoint_url')
        }

    # 3. 개별 날짜에 대해 정제를 수행하는 태스크 (가상환경)
    @task.external_python(python=PYTHON_VENV_PATH)
    def refine_single_date_in_venv(date_str, aws_conn_info):
        import io
        import re
        import html
        import pandas as pd
        import boto3
        try:
            import hanja
            from kiwipiepy import Kiwi
        except ImportError:
            raise ImportError("Required libraries not found in venv")

        # --- 정제 로직 클래스 (기존과 동일) ---
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

        # 경로 설정
        dt = date_str.split('-')
        year, month, day = dt[0], dt[1], dt[2]
        object_key = f'crawled-news/year={year}/month={month}/day={day}/data.parquet'

        print(f"🚀 Processing: {object_key}")

        # 데이터 로드
        response = s3_client.get_object(Bucket='bronze', Key=object_key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))

        trash_dfs = []
        df_initial = df.copy()
        df = df.drop_duplicates(subset=['text']).dropna(subset=['text'])

        # 중복 제거 로그 및 Trash 수집
        if len(df_initial) != len(df):
            removed_mask = ~df_initial['crawled_news_id'].isin(df['crawled_news_id'])
            df_trash_dup = df_initial[removed_mask].copy()
            df_trash_dup['reason'] = 'duplicate_or_nan'
            trash_dfs.append(df_trash_dup)

        # 정제 수행
        df['text'] = df['text'].apply(processor.clean_text_basic)
        df['text'] = df['text'].apply(processor.is_english_only)

        mask_sports = df.apply(lambda r: processor.is_sports_news(r.get('title', ''), r['text']), axis=1)
        mask_short = df['text'].str.len() <= 20

        # Trash 분리
        if mask_sports.any():
            df_trash_sports = df[mask_sports].copy()
            df_trash_sports['reason'] = 'sports_news'
            trash_dfs.append(df_trash_sports)
        if mask_short.any():
            df_trash_short = df[mask_short].copy()
            df_trash_short['reason'] = 'short_text'
            trash_dfs.append(df_trash_short)

        df_final = df[~(mask_sports | mask_short)].copy()

        # [저장 1] Silver 데이터
        silver_buffer = io.BytesIO()
        df_final.to_parquet(silver_buffer, engine='pyarrow', index=False)
        s3_client.put_object(Bucket='silver', Key=object_key, Body=silver_buffer.getvalue())

        # [저장 2] Trash 데이터 (경로 분리)
        if trash_dfs:
            df_trash_total = pd.concat(trash_dfs, ignore_index=True)
            trash_buffer = io.BytesIO()
            df_trash_total.to_parquet(trash_buffer, engine='pyarrow', index=False)
            s3_client.put_object(
                Bucket='silver',
                Key=f"trash/{object_key}",
                Body=trash_buffer.getvalue()
            )
            trash_count = len(df_trash_total)
        else:
            trash_count = 0

        print(f"✅ {date_str} 처리 완료: Silver {len(df_final)}건, Trash {trash_count}건")
        return f"{date_str} success"

    # --- 실시간 태스크 매핑 실행 ---
    all_dates = list_all_bronze_dates()
    aws_info = get_aws_info()

    # .expand를 사용하면 all_dates의 리스트 개수만큼 태스크가 병렬로 자동 생성됩니다.
    refine_single_date_in_venv.expand(date_str=all_dates, aws_conn_info=[aws_info])


news_refinement_full_refresh_dag = news_refinement_full_pipeline()