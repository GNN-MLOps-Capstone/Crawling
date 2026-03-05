from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG

PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'

# DAG 정의
with DAG(
        dag_id="news_dag",
        start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
        schedule='0 */3 * * *',
        catchup=False,
        doc_md="""
    ### 
    - first step: naver API를 호출하여 데이터를 수집하고 PostgreSQL에 저장합니다.
    - second step: 수집된 URL을 기반으로 도메인 기준 필터링을 하고, 크롤링을 하여 PostgreSQL에 저장합니다.
    - 이후 추가 예정
    """,
        tags=["news", "ingestion", "filtering", "crawling", "external_python"],
) as dag:
    @task
    def prepare_ingestion_context():
        pg_conn = BaseHook.get_connection('news_data_db')
        api_conn = BaseHook.get_connection('naver_api')

        db_config = {
            "host": pg_conn.host,
            "port": pg_conn.port,
            "user": pg_conn.login,
            "password": pg_conn.password,
            "dbname": pg_conn.schema,
        }
        api_config = {
            "client_id": api_conn.extra_dejson.get('naver_client_id'),
            "client_secret": api_conn.extra_dejson.get('naver_client_secret'),
        }
        return {
            "db": db_config,
            "api": api_config,
            "filter_file_path": "/opt/airflow/dags/modules/ingestion/filter_domain_list_v1.00.txt",
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id="collect_naver_news")
    def collect_naver_news(context):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.ingestion.collect_naver_news import run_collect_naver_news
        return run_collect_naver_news(
            db_config=context["db"],
            api_config=context["api"],
            query='다',
            total=1000,
        )

    @task.external_python(python=PYTHON_VENV_PATH, task_id="news_crawler")
    def news_crawler(context):
        import sys
        sys.path.append('/opt/airflow/dags')
        from modules.ingestion.news_crawler import run_news_crawler
        return run_news_crawler(
            db_config=context["db"],
            filter_file_path=context["filter_file_path"],
            crawler_version='0.02',
            max_workers=4,
        )

    context = prepare_ingestion_context()
    collect_task = collect_naver_news(context)
    crawl_task = news_crawler(context)
    collect_task >> crawl_task
