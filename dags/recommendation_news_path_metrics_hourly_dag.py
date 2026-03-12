from __future__ import annotations

import os
import sys

import pendulum
from datetime import timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CURRENT_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DAG_DIR not in sys.path:
    sys.path.append(CURRENT_DAG_DIR)

from modules.serving.bandit_posterior import update_bandit_posteriors


default_args = {
    "owner": "dongbin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="recommendation_news_path_metrics_hourly",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    doc_md="추천 노출/클릭/체류시간 지표를 1시간 단위로 집계하는 DAG",
    tags=["recommendation", "aggregation", "hourly"],
    params={
        "window_start": Param(None, type=["null", "string"], description="UTC window start"),
        "window_end": Param(None, type=["null", "string"], description="UTC window end"),
    },
) as dag:
    aggregate_hourly_metrics = SQLExecuteQueryOperator(
        task_id="aggregate_recommendation_news_path_metrics_hourly",
        conn_id="news_data_db",
        sql="sql/aggregate_recommendation_news_path_metrics_hourly.sql",
    )

    build_path_c_snapshot = SQLExecuteQueryOperator(
        task_id="build_recommendation_path_c_snapshot",
        conn_id="news_data_db",
        sql="sql/build_recommendation_path_c_snapshot.sql",
    )

    build_path_a2_snapshot = SQLExecuteQueryOperator(
        task_id="build_recommendation_path_a2_snapshot",
        conn_id="news_data_db",
        sql="sql/build_recommendation_path_a2_snapshot.sql",
    )

    @task(task_id="update_bandit_posteriors")
    def task_update_bandit_posteriors(**context) -> dict[str, int]:
        data_interval_end = context["data_interval_end"]
        dag_conf = context["dag_run"].conf if context.get("dag_run") and context["dag_run"].conf else {}
        window_end = dag_conf.get("window_end") or data_interval_end.to_iso8601_string()
        return update_bandit_posteriors(
            postgres_conn_id="news_data_db",
            redis_host=os.getenv("REDIS_HOST", "redis"),
            redis_port=int(os.getenv("REDIS_PORT", "6379")),
            redis_password=os.getenv("REDIS_PASSWORD"),
            key_prefix=os.getenv("RECO_BANDIT_STATE_KEY_PREFIX", "recommend:bandit"),
            window_end=window_end,
            lookback_hours=int(os.getenv("RECO_BANDIT_LOOKBACK_HOURS", "72")),
            dwell_threshold_seconds=int(os.getenv("RECO_VALID_READ_DWELL_SECONDS", "5")),
        )

    update_bandit_posteriors_task = task_update_bandit_posteriors()

    aggregate_hourly_metrics >> build_path_c_snapshot >> build_path_a2_snapshot >> update_bandit_posteriors_task
