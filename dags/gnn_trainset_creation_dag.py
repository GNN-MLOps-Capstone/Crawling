import pendulum
from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from modules.snapshot_utils import select_latest_snapshot_on_or_before

# [환경 설정]
local_tz = pendulum.timezone("Asia/Seoul")
MINIO_CONN_ID = 'MINIO_S3'
BUCKET_NAME = 'silver'
DOCKER_IMAGE = 'gnn-worker:latest'

DOCKER_NETWORK = 'crawling_news-network'
SILVER_DATASET = Dataset("s3://silver/trainset")

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='gnn_trainset_creation',
    default_args=default_args,
    schedule='0 4 * * *',
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={
        "target_date": Param(
            default=datetime.now(local_tz).strftime('%Y-%m-%d'),
            type="string", format="date", description="Cut-off Date"
        )
    },
    tags=['silver', 'gnn', 'dataset', 'docker']
)
def trainset_pipeline():
    @task(multiple_outputs=True)
    def prepare_context(**context):
        params = context.get('params', {}) or {}
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        scheduled_target_date = context['logical_date'].subtract(days=1).to_date_string()
        run_type = str(getattr(dag_run, "run_type", "")).lower()

        if conf.get('target_date'):
            target_date_str = conf['target_date']
        elif run_type == 'scheduled':
            target_date_str = scheduled_target_date
        else:
            target_date_str = params.get('target_date') or scheduled_target_date

        # YYYY-MM-DD -> YYYYMMDD
        dt = pendulum.parse(target_date_str)
        date_nodash = dt.format('YYYYMMDD')

        # 1. AWS Info
        conn = BaseHook.get_connection(MINIO_CONN_ID)
        # Docker 내부 통신용 URL 변환 (Localhost -> host.docker.internal)
        endpoint = conn.extra_dejson.get('endpoint_url')
        if "localhost" in endpoint or "127.0.0.1" in endpoint:
            endpoint = endpoint.replace("localhost", "host.docker.internal").replace("127.0.0.1",
                                                                                     "host.docker.internal")

        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": endpoint
        }

        # 2. Snapshot 찾기
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        kw_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix="keyword_embeddings/date=")
        latest_kw_snap = select_latest_snapshot_on_or_before(
            kw_keys,
            target_date_str,
            prefix="keyword_embeddings/date=",
            suffix="keyword_embeddings.parquet",
        )

        st_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix="stock_embeddings/")
        latest_st_snap = None
        if st_keys:
            valid_st = [k for k in st_keys if "stock_embeddings_" in k and k.endswith(".parquet")]
            if valid_st: latest_st_snap = max(valid_st)

        # 3. 🟢 [추가] DB Info 가져오기
        pg_conn = BaseHook.get_connection('news_data_db')
        # Docker Network 내부에서 DB 호스트명 찾기
        db_host = pg_conn.host
        if db_host in ['localhost', '127.0.0.1']:
            db_host = 'host.docker.internal'  # 또는 Docker Compose 서비스명 (예: postgres)

        db_info = {
            "host": db_host,
            "port": pg_conn.port,
            "user": pg_conn.login,
            "password": pg_conn.password,
            "dbname": pg_conn.schema
        }

        return {
            "target_date": target_date_str,
            "aws": aws_info,
            "db": db_info,  # 🟢 리턴값에 추가
            "paths": {
                "keyword_snapshot": latest_kw_snap,
                "stock_snapshot": latest_st_snap,
                "output": f"trainset/date={date_nodash}/hetero_graph.pt"
            }
        }

    # 1. Context 준비
    ctx = prepare_context()

    # 2. Docker Operator 선언
    build_graph_task = DockerOperator(
        task_id='build_graph_in_docker',
        image=DOCKER_IMAGE,
        api_version='auto',
        auto_remove='force',

        mount_tmp_dir=False,
        network_mode=DOCKER_NETWORK,

        mounts=[
            Mount(source='/home/dobi/Crawling/dags/modules', target='/app/modules', type='bind')
        ],
        outlets=[SILVER_DATASET],
        command="""
        python -c "
import os, json
from modules.dataset.graph_builder import run_graph_building

target_date = os.environ['TARGET_DATE']
paths = json.loads(os.environ['PATHS_JSON'])
aws_info = json.loads(os.environ['AWS_INFO_JSON'])
db_info = json.loads(os.environ['DB_INFO_JSON']) # 🟢 정상적으로 읽어옴

run_graph_building(target_date, paths, aws_info, db_info)
        "
        """,
        environment={
            'TARGET_DATE': "{{ ti.xcom_pull(task_ids='prepare_context')['target_date'] }}",
            'PATHS_JSON': "{{ ti.xcom_pull(task_ids='prepare_context')['paths'] | tojson }}",
            'AWS_INFO_JSON': "{{ ti.xcom_pull(task_ids='prepare_context')['aws'] | tojson }}",
            'DB_INFO_JSON': "{{ ti.xcom_pull(task_ids='prepare_context')['db'] | tojson }}",  # 🟢 환경변수 주입 추가
        },
        docker_url="unix://var/run/docker.sock"

    )

    ctx >> build_graph_task


dag_instance = trainset_pipeline()
