from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from pipelines.ingestion.stock_crawler import update_stock_details

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='crawl_stock_details_naver',
        default_args=default_args,
        description='DB의 모든 종목에 대해 네이버 금융 상세정보(시장, 업종, 개요) 크롤링 및 업데이트',
        schedule_interval='@once',  # 필요할 때 수동 실행하거나 매주 1회 정도로 설정
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['stock', 'crawling', 'enrichment']
) as dag:
    t1 = PythonOperator(
        task_id='crawl_and_update_stocks',
        python_callable=update_stock_details,
        op_kwargs={
            'conn_id': 'news_data_db'  # Airflow Connection ID
        },
        # 크롤링이 오래 걸릴 수 있으므로 타임아웃을 넉넉하게 설정 (예: 2시간)
        execution_timeout=timedelta(hours=2)
    )

    t1