import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

local_tz = pendulum.timezone("Asia/Seoul")
DOCKER_IMAGE = 'gnn-worker:latest'
DOCKER_NETWORK = 'crawling_news-network'
MINIO_CONN_ID = "MINIO_S3"
DEFAULT_EXPERIMENT_NAME = "News_GNN_v1"

default_args = {
    'owner': 'dongbin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='graph_to_db',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={
        "experiment_name": Param(
            default=DEFAULT_EXPERIMENT_NAME,
            type="string",
            description="MLflow experiment name"
        ),
        "model_status": Param(
            default="product",
            type="string",
            description="MLflow run tag status to deploy (e.g. product/candidate)"
        ),
        "candidate_version": Param(
            default="",
            type="string",
            description="Optional MLflow run tag candidate_version filter"
        ),
    },
    tags=['gold', 'serving', 'db_load']
)
def serving_pipeline():
    @task(multiple_outputs=True)
    def prepare_serving_config(**context):
        # 1. Artifact 선택 기준 (MLflow)
        params = context.get("params", {}) or {}
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        experiment_name = conf.get("experiment_name", params.get("experiment_name", DEFAULT_EXPERIMENT_NAME))
        model_status = conf.get("model_status", params.get("model_status", "product"))
        candidate_version = conf.get("candidate_version", params.get("candidate_version", ""))

        artifact_info = {
            "experiment_name": experiment_name,
            "status": model_status,
            "candidate_version": candidate_version,
        }

        # 2. AWS 접속 정보
        conn = BaseHook.get_connection(MINIO_CONN_ID)
        endpoint_url = conn.extra_dejson.get("endpoint_url", "http://minio:9000")
        if "localhost" in endpoint_url or "127.0.0.1" in endpoint_url:
            endpoint_url = endpoint_url.replace("localhost", "host.docker.internal").replace(
                "127.0.0.1", "host.docker.internal"
            )
        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": endpoint_url
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
        mount_tmp_dir=False,
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
            'MLFLOW_TRACKING_URI': 'http://mlflow:5000',
        },
        docker_url="unix://var/run/docker.sock"
    )

    config >> deploy_task


dag_instance = serving_pipeline()
