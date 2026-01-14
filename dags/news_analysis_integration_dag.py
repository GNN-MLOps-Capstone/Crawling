import pandas as pd
import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param

# [환경 설정]
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = 'news_data_db'
MINIO_CONN_ID = 'MINIO_S3'
SILVER_BUCKET = 'silver'

# 🟢 DAG 1에서 정의한 Dataset URI와 동일해야 함
BRONZE_DATASET = Dataset("s3://bronze/crawled_news")

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='news_analysis_integration',
    default_args=default_args,
    schedule=[BRONZE_DATASET],  # 🟢 Bronze Dataset 업데이트 감지 시 자동 실행
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={
        "updated_dates": Param([], type="array", description="수동 실행 시 처리할 날짜 리스트 (YYYY-MM-DD)")
    },
    tags=['silver', 'etl', 'analysis', 'integration']
)
def analysis_integration_pipeline():
    # --- 1. Context & Setup ---
    @task(multiple_outputs=True)
    def prepare_common_context(**context):
        """
        Dataset 트리거로 실행되면 logical_date(어제)를 기준으로 날짜를 자동 계산하고,
        수동 실행(params)이면 입력된 날짜를 사용합니다.
        """
        params = context.get('params', {})
        manual_dates = params.get('updated_dates', [])

        if manual_dates:
            dates = manual_dates
        else:
            # DAG 1과 동일하게 '어제' 데이터를 처리한다고 가정
            exec_date = context['logical_date'].in_timezone(local_tz)
            target_date = exec_date.subtract(days=1).to_date_string()
            dates = [target_date]

        # MinIO 접속 정보 (공통 사용)
        conn = BaseHook.get_connection(MINIO_CONN_ID)
        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": conn.extra_dejson.get('endpoint_url')
        }
        return {"dates": dates, "aws": aws_info}

    @task
    def get_stock_mapping():
        """DB에서 주식 종목 정보를 조회하여 매핑 딕셔너리 생성"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        df = pg_hook.get_pandas_df(
            "SELECT s.stock_id, s.stock_name, a.alias_name FROM public.stocks s LEFT JOIN public.aliases a ON s.stock_id = a.stock_id"
        )
        mapping = {}
        for _, row in df.iterrows():
            info = {'id': row['stock_id'], 'name': row['stock_name']}
            mapping[row['stock_name'].lower()] = info
            if pd.notna(row['alias_name']):
                mapping[row['alias_name'].lower()] = info
        return mapping

    # --- 2. External Python Tasks (Services) ---

    # [Task 2] Refinement
    @task.external_python(python=PYTHON_VENV_PATH, task_id='refinement_and_embedding')
    def task_refine(updated_dates, aws_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.news_service import run_refinement_process
        return run_refinement_process(updated_dates, aws_conn_info)

    # [Task 3-1] Keyword Extraction
    @task.external_python(python=PYTHON_VENV_PATH, task_id='extraction_keyword')
    def task_extract_keywords(updated_dates, aws_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.extract_service import run_keyword_extraction
        return run_keyword_extraction(updated_dates, aws_conn_info)

    # [Task 3-2] Stock Extraction
    @task.external_python(python=PYTHON_VENV_PATH, task_id='extraction_stock')
    def task_extract_stocks(updated_dates, stock_map, aws_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.extract_service import run_stock_extraction
        return run_stock_extraction(updated_dates, stock_map, aws_conn_info)

    # [Task 4] Keyword Snapshot Update
    @task.external_python(python=PYTHON_VENV_PATH, task_id='create_keyword_embedding_snapshot')
    def task_snapshot_update(updated_dates, aws_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.keyword_embedding_service import run_embedding_update_service
        return run_embedding_update_service(updated_dates, aws_conn_info)

    # --- 3. Loading Preparation & Execution ---
    @task(multiple_outputs=True)
    def prepare_load_context(dates, aws_info):
        """DB 적재를 위한 S3 경로 계산 및 DB 접속 정보 준비"""
        targets = []
        for date_str in dates:
            y, m, d = date_str.split('-')
            targets.append({
                "date": date_str,
                "refined": f"refined_news/year={y}/month={m}/day={d}/data.parquet",
                "keywords": f"extracted_keywords/year={y}/month={m}/day={d}/data.parquet",
                "stocks": f"extracted_stocks/year={y}/month={m}/day={d}/data.parquet"
            })

        # MinIO에서 최신 Snapshot 경로 찾기
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        keys = s3_hook.list_keys(bucket_name=SILVER_BUCKET, prefix="keyword_embeddings/date=")
        latest_snapshot = None
        if keys:
            valid = [k for k in keys if "keyword_embeddings.parquet" in k]
            if valid:
                latest_snapshot = max(valid)

        pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        pg_info = {
            "host": pg_conn.host, "port": pg_conn.port,
            "user": pg_conn.login, "password": pg_conn.password,
            "dbname": pg_conn.schema
        }
        return {"targets": targets, "snapshot": latest_snapshot, "pg": pg_info}

    # [Task 5] Load to DB
    @task.external_python(python=PYTHON_VENV_PATH, task_id='load_to_db')
    def task_load_final(targets, snapshot_path, aws_info, pg_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.loading.load_service import run_db_loading
        return run_db_loading(targets, snapshot_path, aws_info, pg_info)

    # --- 4. Flow Definition ---

    # 1) 준비
    ctx = prepare_common_context()
    stock_map = get_stock_mapping()

    # 2) 전처리 (Refinement)
    refinement_done = task_refine(ctx['dates'], ctx['aws'])

    # 3) 추출 (Extraction) - Refinement 완료 후 병렬 실행
    keywords_done = task_extract_keywords(ctx['dates'], ctx['aws'])
    stocks_done = task_extract_stocks(ctx['dates'], stock_map, ctx['aws'])

    refinement_done >> keywords_done
    refinement_done >> stocks_done

    # 4) 스냅샷 업데이트 - Keyword Extraction 완료 후 실행
    snapshot_done = task_snapshot_update(ctx['dates'], ctx['aws'])
    keywords_done >> snapshot_done

    # 5) DB 적재 준비 및 실행 - 모든 데이터 처리(Stock, Snapshot) 완료 후
    load_ctx = prepare_load_context(ctx['dates'], ctx['aws'])

    final_load = task_load_final(
        load_ctx['targets'],
        load_ctx['snapshot'],
        ctx['aws'],
        load_ctx['pg']
    )

    # Stock 추출과 Snapshot 업데이트가 모두 끝나야 적재 시작
    [stocks_done, snapshot_done] >> final_load


dag_instance = analysis_integration_pipeline()