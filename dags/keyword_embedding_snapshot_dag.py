import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {'owner': 'dongbin', 'retries': 0}


@dag(
    dag_id='keyword_embedding_update_modular',  # ID 변경: Snapshot -> Embedding Update
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={"updated_dates": Param([], type="array")},
    tags=['silver', 'embedding', 'vector', 'update']  # 태그 변경
)
def embedding_update_pipeline():
    @task(multiple_outputs=True)
    def prepare_context(**context):
        params = context.get('params', {})
        conf = context['dag_run'].conf
        dates = conf.get('updated_dates') or params.get('updated_dates', [])

        conn = BaseHook.get_connection('MINIO_S3')
        aws_info = {"access_key": conn.login, "secret_key": conn.password,
                    "endpoint_url": conn.extra_dejson.get('endpoint_url')}
        return {"dates": dates, "aws": aws_info}

    @task.external_python(python=PYTHON_VENV_PATH)
    def task_generate_embeddings(updated_dates, aws_conn_info):
        import sys
        sys.path.append('/opt/airflow/dags')

        # 🟢 Embedding Service 호출
        from modules.analysis.keyword_embedding_service import run_embedding_update_service
        return run_embedding_update_service(updated_dates, aws_conn_info)

    # --- Flow ---
    ctx = prepare_context()
    task_generate_embeddings(ctx['dates'], ctx['aws'])


dag_instance = embedding_update_pipeline()