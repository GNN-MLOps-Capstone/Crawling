from __future__ import annotations

import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook


PYTHON_VENV_PATH = "/opt/airflow/venv_nlp/bin/python"
POSTGRES_CONN_ID = "news_data_db"
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "dongbin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="recommendation_popular_snapshot",
    default_args=default_args,
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["recommendation", "snapshot", "popular"],
)
def recommendation_popular_snapshot_pipeline():
    @task(multiple_outputs=True)
    def prepare_context():
        pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        return {
            "db": {
                "host": pg_conn.host,
                "port": pg_conn.port,
                "user": pg_conn.login,
                "password": pg_conn.password,
                "dbname": pg_conn.schema,
            }
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id="refresh_popular_snapshot")
    def refresh_popular_snapshot(context):
        import sys

        sys.path.append("/opt/airflow/dags")

        from modules.analysis.popular_snapshot import refresh_popular_snapshot as run_refresh_popular_snapshot

        result = run_refresh_popular_snapshot(
            db_info=context["db"],
            lookback_hours=72,
            candidate_limit=500,
            max_per_domain=2,
            keep_hours=72,
        )
        print(f"✅ popular snapshot refreshed: {result}", flush=True)
        return result

    ctx = prepare_context()
    refresh_popular_snapshot(ctx)


dag_instance = recommendation_popular_snapshot_pipeline()
