import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param

# [환경 설정]
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='news_refinement_single_task',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 12, 1, tzinfo=local_tz),
    catchup=False,
    params={
        "updated_dates": Param([], type="array", description="처리할 날짜 리스트")
    },
    render_template_as_native_obj=True,
    tags=['silver', 'refinement', 'clean']
)
def news_refinement_pipeline_final():
    @task(multiple_outputs=True)
    def prepare_context(**context):
        params = context.get('params', {})
        conf = context['dag_run'].conf
        dates = conf.get('updated_dates') or params.get('updated_dates', [])

        conn = BaseHook.get_connection('MINIO_S3')
        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": conn.extra_dejson.get('endpoint_url')
        }
        return {"dates": dates, "aws": aws_info}

    @task.external_python(python=PYTHON_VENV_PATH, task_id='refinement_and_embedding')
    def task_refinement_service(updated_dates, aws_conn_info):
        import sys
        # 모듈 경로 인식
        sys.path.append('/opt/airflow/dags')

        # 🟢 서비스 모듈 호출
        from modules.analysis.news_service import run_refinement_process

        # 복잡한 건 서비스가 알아서 처리함
        return run_refinement_process(updated_dates, aws_conn_info)

    # --- Flow ---
    ctx = prepare_context()
    task_refinement_service(updated_dates=ctx['dates'], aws_conn_info=ctx['aws'])


news_refinement_standalone_dag = news_refinement_pipeline_final()