# FILE: dags/gnn_training_dag.py

import pendulum
from datetime import datetime, timedelta
import json
import os

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
            type="string", format="date", description="학습할 데이터 날짜"
        ),
        "epochs": Param(default=50, type="integer", description="학습 Epoch 수 (Override)")
    },
    tags=['gold', 'gnn', 'training']
)
def training_pipeline():
    @task(multiple_outputs=True)
    def prepare_config(**context):
        """
        1. 날짜 기준 경로(Path) 문자열 계산 (데이터 로드 X)
        2. Config 파일 읽기
        3. MinIO 접속 정보 조회
        """
        # 1. 경로 계산
        train_date_str = context["params"]["train_date"]
        dt = pendulum.parse(train_date_str)
        date_nodash = dt.format('YYYYMMDD')

        trainset_path = f"trainset/date={date_nodash}/hetero_graph.pt"
        output_path = f"models/gnn/v_{date_nodash}/"

        # 2. Config 파일 로드 (modules 폴더 내 json 읽기)
        # Airflow 워커 내부의 경로 기준입니다.
        config_path = "/opt/airflow/dags/modules/training/config.json"

        # 기본값 (파일이 없을 경우 대비)
        config = {
            "model": {"hidden_dim": 64, "out_dim": 32},
            "training": {"lr": 0.01, "epochs": 50}
        }

        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                config.update(file_config)

        # UI 파라미터로 오버라이드
        if 'epochs' in context['params']:
            config['training']['epochs'] = context['params']['epochs']

        # 3. AWS 정보
        connection = BaseHook.get_connection(MINIO_CONN_ID)
        extra = connection.extra_dejson
        aws_info = {
            "access_key": connection.login,
            "secret_key": connection.password,
            "endpoint_url": extra.get('endpoint_url', 'http://minio:9000')
        }

        return {
            "trainset_path": trainset_path,
            "output_path": output_path,
            "aws": aws_info,
            "config": config
        }

    config_data = prepare_config()

    # DockerOperator 실행
    # 실제 Python 모듈 실행은 이 컨테이너 안에서 일어납니다.
    train_task = DockerOperator(
        task_id='train_gnn_model',
        image=DOCKER_IMAGE,
        api_version='auto',
        auto_remove='never',
        mount_tmp_dir=False,
        network_mode=DOCKER_NETWORK,
        outlets=[GOLD_MODEL_ARTIFACT],

        # [중요] 모듈 폴더를 컨테이너 내 /app/modules 로 마운트
        mounts=[
            Mount(source='/home/dobi/Crawling/dags/modules', target='/app/modules', type='bind')
        ],
        device_requests=[
            DeviceRequest(count=-1, capabilities=[['gpu']])
        ],
        # [중요] modules.training.trainer 패키지 경로 사용
        command="""
        python -c "
import os, json
from modules.training.trainer import run_training_pipeline

# 환경변수에서 문자열로 된 설정을 파싱
path_in = os.environ['TRAINSET_PATH']
path_out = os.environ['OUTPUT_PATH']
aws = json.loads(os.environ['AWS_INFO_JSON'])
cfg = json.loads(os.environ['CONFIG_JSON'])

# Trainer 실행
run_training_pipeline(path_in, path_out, aws, cfg)
        "
        """,
        environment={
            'TRAINSET_PATH': "{{ ti.xcom_pull(task_ids='prepare_config')['trainset_path'] }}",
            'OUTPUT_PATH': "{{ ti.xcom_pull(task_ids='prepare_config')['output_path'] }}",
            'AWS_INFO_JSON': "{{ ti.xcom_pull(task_ids='prepare_config')['aws'] | tojson }}",
            'CONFIG_JSON': "{{ ti.xcom_pull(task_ids='prepare_config')['config'] | tojson }}",
            'MLFLOW_TRACKING_URI': 'http://mlflow:5000',
        },
    )

    config_data >> train_task


training_pipeline()