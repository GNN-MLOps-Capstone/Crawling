# table_init_dag.py
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="db_init_create_tables",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    doc_md="데이터베이스에 초기 테이블을 생성하는 단발성 DAG",
    tags=['database', 'init'],
) as dag:
    create_tables_task = SQLExecuteQueryOperator(
        task_id="create_news_tables_from_file",
        conn_id="news_data_db",
        sql="sql/create_news_table.sql",
    )

