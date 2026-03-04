import pendulum
from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

local_tz = pendulum.timezone("Asia/Seoul")
GOLD_MODEL_ARTIFACT = Dataset("s3://gold/gnn")
DOCKER_IMAGE = 'gnn-worker:latest'
DOCKER_NETWORK = 'crawling_news-network'

default_args = {
    'owner': 'dongbin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='graph_to_db',
    default_args=default_args,
    schedule=[GOLD_MODEL_ARTIFACT],
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['gold', 'serving', 'db_load']
)
def serving_pipeline():
    @task(multiple_outputs=True)
    def prepare_serving_config():
        # 1. Artifact 정보 찾기 (MinIO)
        s3_hook = S3Hook(aws_conn_id='MINIO_S3')
        keys = s3_hook.list_keys(bucket_name='gold', prefix='models/gnn/v_')
        if not keys: raise ValueError("No artifacts found!")

        latest_file = sorted([k for k in keys if 'node_embeddings.pkl' in k])[-1]
        parent_path = "/".join(latest_file.split('/')[:-1])
        version_name = parent_path.split('/')[-1]
        date_str = version_name.replace('v_', '')

        artifact_info = {
            "model_bucket": "gold",
            "model_emb_key": f"{parent_path}/node_embeddings.pkl",
            "map_bucket": "silver",
            "map_key": f"trainset/date={date_str}/node_mapping.pkl",
            "version": version_name
        }

        # 2. AWS 접속 정보
        conn = BaseHook.get_connection('MINIO_S3')
        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": "http://minio:9000"  # Docker 내부 통신용
        }

        # 3. DB 접속 정보
        pg_conn = BaseHook.get_connection('news_data_db')
        # Docker Network 내부에서 DB 호스트명 찾기 (보통 'postgres' 또는 'db')
        # 만약 localhost라면 host.docker.internal로 변경
        db_host = pg_conn.host
        if db_host in ['localhost', '127.0.0.1']:
            db_host = 'host.docker.internal'

        db_info = {
            "host": db_host,
            "port": pg_conn.port,
            "user": pg_conn.login,
            "password": pg_conn.password,
            "dbname": pg_conn.schema
        }

        return {"artifact": artifact_info, "aws": aws_info, "db": db_info}

    config = prepare_serving_config()

    # DockerOperator로 실행 (gnn-worker 환경 사용)
    deploy_task = DockerOperator(
        task_id='deploy_to_db',
        image=DOCKER_IMAGE,
        api_version='auto',
        auto_remove='force',
        network_mode=DOCKER_NETWORK,
        mounts=[
            Mount(source='/home/dobi/Crawling/dags/modules', target='/app/modules', type='bind')
        ],
        command="""
        python -c "
import os, json
from modules.serving.deploy import run_deploy

artifact_info = json.loads(os.environ['ARTIFACT_INFO'])
aws_info = json.loads(os.environ['AWS_INFO'])
db_info = json.loads(os.environ['DB_INFO'])

run_deploy(artifact_info, aws_info, db_info)
        "
        """,
        environment={
            'ARTIFACT_INFO': "{{ ti.xcom_pull(task_ids='prepare_serving_config')['artifact'] | tojson }}",
            'AWS_INFO': "{{ ti.xcom_pull(task_ids='prepare_serving_config')['aws'] | tojson }}",
            'DB_INFO': "{{ ti.xcom_pull(task_ids='prepare_serving_config')['db'] | tojson }}",
        },
        docker_url="unix://var/run/docker.sock"
    )

    config >> deploy_task


dag_instance = serving_pipeline()