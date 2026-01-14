import pendulum
from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount, DeviceRequest

# [환경 설정]
local_tz = pendulum.timezone("Asia/Seoul")
MINIO_CONN_ID = 'MINIO_S3'
DOCKER_IMAGE = 'gnn-worker:latest'
DOCKER_NETWORK = 'crawling_news-network'

SILVER_DATASET = Dataset("s3://silver/trainset")
GOLD_MODEL_ARTIFACT = Dataset("s3://gold/gnn")

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='gnn_training_pipeline',
    default_args=default_args,
    schedule=[SILVER_DATASET],
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={
        "train_date": Param(
            default=datetime.now(local_tz).strftime('%Y-%m-%d'),
            type="string", format="date", description="학습할 데이터셋의 날짜 (Trainset)"
        ),
        "epochs": Param(default=50, type="integer", description="학습 Epoch 수")
    },
    tags=['gold', 'gnn', 'training', 'custom_model']
)
def training_pipeline():
    @task(multiple_outputs=True)
    def prepare_config(**context):
        params = context.get('params', {})
        train_date_str = params.get('train_date')
        epochs = params.get('epochs')

        dt = pendulum.parse(train_date_str)
        date_nodash = dt.format('YYYYMMDD')

        trainset_path = f"trainset/date={date_nodash}/hetero_graph.pt"
        output_path = f"models/gnn/v_{date_nodash}/"

        conn = BaseHook.get_connection(MINIO_CONN_ID)

        endpoint_url = "http://minio:9000"

        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": endpoint_url
        }

        return {
            "trainset_path": trainset_path,
            "output_path": output_path,
            "aws": aws_info,
            "epochs": epochs
        }

    # 1. Config 준비
    config = prepare_config()

    # 2. Training Task (Docker with GPU)
    train_task = DockerOperator(
        task_id='train_gnn_model',
        image=DOCKER_IMAGE,
        api_version='auto',
        auto_remove='never',  # 디버깅용 유지 (운영 시 force 권장)
        mount_tmp_dir=False,
        network_mode=DOCKER_NETWORK,

        outlets=[GOLD_MODEL_ARTIFACT],

        mounts=[
            Mount(source='/home/dobi/Crawling/dags/modules', target='/app/modules', type='bind')
        ],
        device_requests=[
            DeviceRequest(count=-1, capabilities=[['gpu']])
        ],
        command="""
        python -c "
import os, json
from modules.training.trainer import run_training_pipeline

trainset_path = os.environ['TRAINSET_PATH']
output_path = os.environ['OUTPUT_PATH']
epochs = int(os.environ['EPOCHS'])
aws_info = json.loads(os.environ['AWS_INFO_JSON'])

run_training_pipeline(trainset_path, output_path, aws_info, epochs)
        "
        """,
        environment={
            'TRAINSET_PATH': "{{ ti.xcom_pull(task_ids='prepare_config')['trainset_path'] }}",
            'OUTPUT_PATH': "{{ ti.xcom_pull(task_ids='prepare_config')['output_path'] }}",
            'EPOCHS': "{{ ti.xcom_pull(task_ids='prepare_config')['epochs'] }}",
            'AWS_INFO_JSON': "{{ ti.xcom_pull(task_ids='prepare_config')['aws'] | tojson }}",
            'MLFLOW_TRACKING_URI': 'http://mlflow:5000',
        },
        shm_size='2g',
        docker_url="unix://var/run/docker.sock"
    )

    config >> train_task


dag_instance = training_pipeline()