import pandas as pd
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = 'news_data_db'

default_args = {'owner': 'dongbin', 'retries': 0}


@dag(
    dag_id='news_stock_extraction_modular',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={"updated_dates": Param([], type="array")},
    tags=['silver', 'stock', 'modular']
)
def stock_pipeline():
    # 1. Main Worker에서 DB 조회 (PostgresHook 사용을 위해)
    @task
    def get_stock_mapping():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        df = pg_hook.get_pandas_df(
            "SELECT s.stock_id, s.stock_name, a.alias_name FROM public.stocks s LEFT JOIN public.aliases a ON s.stock_id = a.stock_id")

        mapping = {}
        for _, row in df.iterrows():
            info = {'id': row['stock_id'], 'name': row['stock_name']}
            mapping[row['stock_name'].lower()] = info
            if pd.notna(row['alias_name']): mapping[row['alias_name'].lower()] = info
        return mapping

    @task(multiple_outputs=True)
    def prepare_context(**context):
        params = context.get('params', {})
        conf = context['dag_run'].conf
        dates = conf.get('updated_dates') or params.get('updated_dates', [])

        conn = BaseHook.get_connection('MINIO_S3')
        aws_info = {"access_key": conn.login, "secret_key": conn.password,
                    "endpoint_url": conn.extra_dejson.get('endpoint_url')}
        return {"dates": dates, "aws": aws_info}

    # 2. External Python에서 Service 실행
    @task.external_python(python=PYTHON_VENV_PATH)
    def task_extract_stocks(updated_dates, stock_map, aws_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')

        # 🟢 Service 호출
        from modules.analysis.extract_service import run_stock_extraction
        return run_stock_extraction(updated_dates, stock_map, aws_conn_info)

    # Flow
    stock_map = get_stock_mapping()
    ctx = prepare_context()
    task_extract_stocks(ctx['dates'], stock_map, ctx['aws'])


stock_pipeline()