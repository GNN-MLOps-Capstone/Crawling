import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param

# [설정]
PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")
BUCKET = 'silver'

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='news_data_load_modular',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={"updated_dates": Param([], type="array")},
    tags=['silver', 'db', 'loading', 'modular']
)
def load_pipeline():
    @task(multiple_outputs=True)
    def prepare_context(**context):
        params = context.get('params', {})
        conf = context['dag_run'].conf
        dates = conf.get('updated_dates') or params.get('updated_dates', [])

        # 1. Target Paths 계산
        targets = []
        for date_str in dates:
            y, m, d = date_str.split('-')
            targets.append({
                "date": date_str,
                "refined": f"refined_news/year={y}/month={m}/day={d}/data.parquet",
                "keywords": f"extracted_keywords/year={y}/month={m}/day={d}/data.parquet",
                "stocks": f"extracted_stocks/year={y}/month={m}/day={d}/data.parquet"
            })

        # 2. Latest Snapshot 찾기
        s3_hook = S3Hook(aws_conn_id='MINIO_S3')
        keys = s3_hook.list_keys(bucket_name=BUCKET, prefix="keyword_embeddings/date=")
        latest_snapshot = None
        if keys:
            valid = [k for k in keys if "keyword_embeddings.parquet" in k]
            if valid: latest_snapshot = max(valid)  # 사전순 정렬 최신값

        # 3. Connections
        s3_conn = BaseHook.get_connection('MINIO_S3')
        aws_info = {"access_key": s3_conn.login, "secret_key": s3_conn.password,
                    "endpoint_url": s3_conn.extra_dejson.get('endpoint_url')}

        pg_conn = BaseHook.get_connection('news_data_db')
        pg_info = {"host": pg_conn.host, "port": pg_conn.port, "user": pg_conn.login, "password": pg_conn.password,
                   "dbname": pg_conn.schema}

        return {"targets": targets, "snapshot": latest_snapshot, "aws": aws_info, "pg": pg_info}

    @task.external_python(python=PYTHON_VENV_PATH)
    def task_load_to_db(targets, snapshot_path, aws_info, pg_info):
        import sys
        sys.path.append('/opt/airflow/dags')

        # 🟢 Service 호출
        from modules.loading.load_service import run_db_loading
        return run_db_loading(targets, snapshot_path, aws_info, pg_info)

    # --- Flow ---
    ctx = prepare_context()
    task_load_to_db(ctx['targets'], ctx['snapshot'], ctx['aws'], ctx['pg'])


dag_instance = load_pipeline()