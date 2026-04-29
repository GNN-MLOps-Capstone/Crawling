import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='naver_title_backfill',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={
        "limit": Param(0, type="integer", minimum=0, description="처리할 최대 건수 (0이면 전체)"),
        "offset": Param(0, type="integer", minimum=0, description="오프셋"),
        "days_back": Param(30, type="integer", minimum=1, description="최근 N일 대상"),
        "dry_run": Param(True, type="boolean", description="DB 업데이트 없이 결과만 확인"),
        "max_workers": Param(8, type="integer", minimum=1, description="병렬 크롤링 스레드"),
        "timeout_sec": Param(10, type="integer", minimum=1, description="요청 타임아웃(초)"),
    },
    tags=['news', 'maintenance', 'backfill', 'external_python'],
)
def naver_title_backfill_pipeline():
    @task(multiple_outputs=True)
    def prepare_context(**context):
        def to_bool(value):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
            return bool(value)

        params = context.get('params', {})
        dag_conf = context['dag_run'].conf or {}

        pg_conn = BaseHook.get_connection('news_data_db')
        db_config = {
            "host": pg_conn.host,
            "port": pg_conn.port,
            "user": pg_conn.login,
            "password": pg_conn.password,
            "dbname": pg_conn.schema,
        }

        return {
            "db": db_config,
            "limit": int(dag_conf.get("limit", params.get("limit", 0))),
            "offset": int(dag_conf.get("offset", params.get("offset", 0))),
            "days_back": int(dag_conf.get("days_back", params.get("days_back", 30))),
            "dry_run": to_bool(dag_conf.get("dry_run", params.get("dry_run", True))),
            "max_workers": int(dag_conf.get("max_workers", params.get("max_workers", 8))),
            "timeout_sec": int(dag_conf.get("timeout_sec", params.get("timeout_sec", 10))),
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id='backfill_titles')
    def backfill_titles(context):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.ingestion.title_backfill import run_title_backfill

        return run_title_backfill(
            db_config=context["db"],
            limit=context["limit"],
            offset=context["offset"],
            days_back=context["days_back"],
            dry_run=context["dry_run"],
            max_workers=context["max_workers"],
            timeout_sec=context["timeout_sec"],
        )

    ctx = prepare_context()
    backfill_titles(ctx)


dag_instance = naver_title_backfill_pipeline()
