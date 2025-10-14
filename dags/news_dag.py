from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# 실행 함수 import
from pipelines.ingestion.collect_naver_news import run_collect_naver_news
from pipelines.ingestion.news_crawler import run_news_crawler
    # def run_collect_naver_news():
    #     print("API 스크립트 실행 (실제 모듈을 찾을 수 없음)")
    # def run_news_crawler():
    #     print("크롤러 스크립트 실행 (실제 모듈을 찾을 수 없음)")

# DAG 정의
with DAG(
    dag_id="news_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule='@hourly',
    catchup=False,
    doc_md="""
    ### 
    - first step: naver API를 호출하여 데이터를 수집하고 PostgreSQL에 저장합니다.
    - second step: 수집된 URL을 기반으로 도메인 기준 필터링을 하고, 크롤링을 하여 PostgreSQL에 저장합니다.
    - 이후 추가 예정
    """,
    tags=["news", "ingestion", "filtering", "crawling"],
) as dag:
    # Task 정의
    collect_naver_news = PythonOperator(
        task_id="collect_naver_news",
        python_callable=run_collect_naver_news,
    )

    news_crawler = PythonOperator(
        task_id="news_crawler",
        python_callable=run_news_crawler,
    )

    # Task 의존성 설정
    collect_naver_news >> news_crawler




