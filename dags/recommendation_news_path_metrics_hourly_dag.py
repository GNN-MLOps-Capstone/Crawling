from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    "owner": "dongbin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="recommendation_news_path_metrics_hourly",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 * * * *",
    catchup=False,
    doc_md="추천 노출/클릭/체류시간 지표를 1시간 단위로 집계하는 DAG",
    tags=["recommendation", "aggregation", "hourly"],
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

    aggregate_hourly_metrics >> build_path_c_snapshot
