import pendulum
from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.models import Variable

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {'owner': 'dongbin', 'retries': 0}

@dag(
    dag_id='news_gemini_stage2_sentiment',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={"updated_dates": Param([], type="array")},
    tags=['silver', 'gemini', 'stage2']
)
def stage2_pipeline():
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
    def task_sentiment_stage2(updated_dates, aws_conn_info, api_key):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.analysis.extract_service import run_stage2_sentiment
        return run_stage2_sentiment(updated_dates, aws_conn_info, api_key)

    ctx = prepare_context()
    task_sentiment_stage2(ctx['dates'], ctx['aws'], ctx['api_key'])

stage2_pipeline()