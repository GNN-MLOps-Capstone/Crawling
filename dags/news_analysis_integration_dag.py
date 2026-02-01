import pandas as pd
import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param
from airflow.models import Variable

# [환경 설정]
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = 'news_data_db'
MINIO_CONN_ID = 'MINIO_S3'
SILVER_BUCKET = 'silver'
CONFIG_PATH = '/opt/airflow/dags/config/analysis_config.yaml'  # 설정 파일 경로

BRONZE_DATASET = Dataset("s3://bronze/crawled_news")

default_args = {
    'owner': 'dongbin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


@dag(
    dag_id='news_analysis_integration_v2',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=True,
    max_active_runs=1,
    params={
        "updated_dates": Param([], type="array", description="수동 실행 날짜 (YYYY-MM-DD)")
    },
    tags=['silver', 'gemini', 'etl']
)
def analysis_integration_pipeline():

    @task
    def prepare_db_context():
        from airflow.hooks.base import BaseHook
        pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        return {
            "host": pg_conn.host,
            "user": pg_conn.login,
            "password": pg_conn.password,
            "dbname": pg_conn.schema,
            "port": pg_conn.port
        }

    # 1. Context 준비 (API Key 포함)
    @task(multiple_outputs=True)
    def prepare_context(**context):
        params = context.get('params', {})
        manual_dates = params.get('updated_dates', [])
        dates = manual_dates if manual_dates else [context['logical_date'].subtract(days=1).to_date_string()]

        conn = BaseHook.get_connection(MINIO_CONN_ID)
        gemini_key = Variable.get("GEMINI_API_KEY", default_var="")  # Airflow Variable 사용

        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": conn.extra_dejson.get('endpoint_url'),
            "gemini_api_key": gemini_key
        }
        return {"dates": dates, "aws": aws_info}

    @task
    def get_stock_mapping():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        df = pg_hook.get_pandas_df("SELECT stock_id, stock_name FROM public.stocks")
        # 검색 속도 향상을 위해 Lowercase Key Mapping
        return {row['stock_name'].lower(): {'id': row['stock_id'], 'name': row['stock_name']} for _, row in
                df.iterrows()}

    #2. Refinement
    @task.external_python(python=PYTHON_VENV_PATH, task_id='refinement')
    def task_refine(updated_dates, aws_info, pg_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.news_service import run_refinement_process
        return run_refinement_process(updated_dates, aws_info, pg_info)

    # 3. Gemini Analysis (신규 모듈 - 통합 분석)
    @task.external_python(python=PYTHON_VENV_PATH, task_id='gemini_analysis')
    def task_gemini(updated_dates, aws_info, stock_map):
        import sys
        sys.path.append('/opt/airflow/dags')
        config_file = '/opt/airflow/dags/config/analysis_config.yaml'
        from modules.analysis.gemini_service import run_gemini_service
        return run_gemini_service(updated_dates, aws_info, config_file, stock_map)

    # 4. Keyword Snapshot (기존 모듈 유지)
    @task.external_python(python=PYTHON_VENV_PATH, task_id='keyword_snapshot')
    def task_snapshot(updated_dates, aws_info):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.keyword_embedding_service import run_embedding_update_service
        return run_embedding_update_service(updated_dates, aws_info)

    # 5. Load
    @task.external_python(python=PYTHON_VENV_PATH, task_id='db_loading')
    def task_load(dates, aws_info, snapshot_input, pg_info):  # <--- pg_info 추가
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.loading.load_service import run_db_loading

        final_path = None

        if isinstance(snapshot_input, list) and snapshot_input:
            final_path = snapshot_input[-1]
        elif isinstance(snapshot_input, str):
            final_path = snapshot_input

        targets = []
        for d_str in dates:
            y, m, d = d_str.split('-')
            targets.append({
                "date": d_str,
                "refined": f"refined_news/year={y}/month={m}/day={d}/data.parquet",
                "stocks": f"extracted_stocks/year={y}/month={m}/day={d}/data.parquet",
                "keywords": f"extracted_keywords/year={y}/month={m}/day={d}/data.parquet",
                "analysis": f"analyzed_news/year={y}/month={m}/day={d}/data.parquet"
            })

        return run_db_loading(targets, final_path, aws_info, pg_info)

    # --- Flow ---
    ctx = prepare_context()
    pg_info = prepare_db_context()
    mapping = get_stock_mapping()

    # Refinement (Ollama Embedding) -> Gemini Analysis (Extraction)
    refined = task_refine(ctx['dates'], ctx['aws'], pg_info)
    analyzed = task_gemini(ctx['dates'], ctx['aws'], mapping)

    refined >> analyzed

    # Snapshot은 Keyword가 추출된 후 실행
    snapshot = task_snapshot(ctx['dates'], ctx['aws'])
    analyzed >> snapshot

    # Loading은 모든 분석 및 스냅샷 완료 후
    task_load(ctx['dates'], ctx['aws'], snapshot, pg_info)


dag_instance = analysis_integration_pipeline()