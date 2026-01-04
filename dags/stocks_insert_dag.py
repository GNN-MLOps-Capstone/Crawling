from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from pipelines.ingestion.stock_parser import parse_and_load_all

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='import_stock_csv_full',
        default_args=default_args,
        description='CSV로부터 종목 생성(Stocks) 및 별명 추가(Aliases)',
        schedule_interval='@once',
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['stock', 'setup']
) as dag:
    t1 = PythonOperator(
        task_id='load_stocks_and_aliases',
        python_callable=parse_and_load_all,
        op_kwargs={
            # 실제 CSV 파일 경로를 여기에 적으세요
            'csv_path': '/opt/airflow/dags/pipelines/ingestion/stock_list.csv',
            'conn_id': 'news_data_db'
        }
    )

    t1