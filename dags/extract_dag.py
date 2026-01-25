import pandas as pd
import pendulum
from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = 'news_data_db'

default_args = {'owner': 'dongbin', 'retries': 0}

@dag(
    dag_id='news_gemini_stage1_extraction',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={"updated_dates": Param([], type="array")},
    tags=['silver', 'gemini', 'stage1']
)
def stage1_pipeline():
    @task
    def get_stock_mapping():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        df = pg_hook.get_pandas_df(
            "SELECT s.stock_id, s.stock_name, a.alias_name FROM public.stocks s LEFT JOIN public.aliases a ON s.stock_id = a.stock_id"
        )
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
        api_key = Variable.get("GEMINI_API_KEY")
        return {"dates": dates, "aws": aws_info, "api_key": api_key}

    @task.external_python(python=PYTHON_VENV_PATH)
    def task_extract_stage1(updated_dates, stock_map, aws_conn_info, api_key):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.extract_service import run_stage1_extraction
        return run_stage1_extraction(updated_dates, stock_map, aws_conn_info, api_key)

    ctx = prepare_context()
    stock_map = get_stock_mapping()
    extraction_done = task_extract_stage1(ctx['dates'], stock_map, ctx['aws'], ctx['api_key'])

    # 🔗 [핵심] 1단계가 성공하면 2단계 DAG를 호출하며 날짜 데이터를 넘김
    trigger_stage2 = TriggerDagRunOperator(
        task_id='trigger_stage2_sentiment',
        trigger_dag_id='news_gemini_stage2_sentiment',
        conf={"updated_dates": "{{ dag_run.conf.get('updated_dates', params.updated_dates) }}"},
        wait_for_completion=False # 2단계가 끝날 때까지 기다리지 않고 1단계는 종료
    )

    extraction_done >> trigger_stage2

stage1_pipeline()