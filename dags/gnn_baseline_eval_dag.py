import json
import os
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

local_tz = pendulum.timezone('Asia/Seoul')
MINIO_CONN_ID = 'MINIO_S3'
DOCKER_IMAGE = 'gnn-worker:latest'
DOCKER_NETWORK = 'crawling_news-network'

default_args = {
    'owner': 'dongbin',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='gnn_baseline_eval',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={
        'train_date': Param(
            default=datetime.now(local_tz).strftime('%Y-%m-%d'),
            type='string',
            format='date',
            description='Baseline을 평가할 trainset 날짜',
        ),
        'model_status': Param(
            default='product',
            type='string',
            description='비교할 GNN MLflow status',
        ),
        'candidate_version': Param(
            default='',
            type='string',
            description='candidate status일 때 선택할 candidate_version',
        ),
    },
    tags=['gold', 'gnn', 'baseline', 'mlflow'],
)
def baseline_eval_pipeline():
    @task(multiple_outputs=True)
    def prepare_config(**context):
        train_date_str = context['params']['train_date']
        dt = pendulum.parse(train_date_str)
        date_nodash = dt.format('YYYYMMDD')

        config_path = '/opt/airflow/dags/modules/training/config.json'
        with open(config_path, 'r') as handle:
            config = json.load(handle)

        connection = BaseHook.get_connection(MINIO_CONN_ID)
        endpoint_url = connection.extra_dejson.get('endpoint_url', 'http://minio:9000')
        if 'localhost' in endpoint_url or '127.0.0.1' in endpoint_url:
            endpoint_url = endpoint_url.replace('localhost', 'host.docker.internal').replace(
                '127.0.0.1', 'host.docker.internal'
            )

        aws_info = {
            'access_key': connection.login,
            'secret_key': connection.password,
            'endpoint_url': endpoint_url,
        }

        params = context.get('params', {}) or {}
        return {
            'trainset_path': f'trainset/date={date_nodash}/hetero_graph.pt',
            'output_path': f'models/gnn_baseline/v_{date_nodash}/',
            'model_status': params.get('model_status', 'product'),
            'candidate_version': params.get('candidate_version', ''),
            'aws': aws_info,
            'config': config,
        }

    config_data = prepare_config()

    command = (
        'python -c '
        '"import os, json; '
        'from modules.training.baseline_eval import run_baseline_eval; '
        'run_baseline_eval('
        "os.environ['TRAINSET_PATH'], "
        "json.loads(os.environ['AWS_INFO_JSON']), "
        "json.loads(os.environ['CONFIG_JSON']), "
        "os.environ.get('OUTPUT_PATH'), "
        "os.environ.get('MODEL_STATUS', 'product'), "
        "os.environ.get('CANDIDATE_VERSION', ''))"
        '"'
    )

    run_baseline = DockerOperator(
        task_id='run_baseline_eval',
        image=DOCKER_IMAGE,
        api_version='auto',
        auto_remove='force',
        mount_tmp_dir=False,
        network_mode=DOCKER_NETWORK,
        mounts=[
            Mount(source='/home/dobi/Crawling/dags/modules', target='/app/modules', type='bind'),
        ],
        command=command,
        environment={
            'TRAINSET_PATH': "{{ ti.xcom_pull(task_ids='prepare_config')['trainset_path'] }}",
            'OUTPUT_PATH': "{{ ti.xcom_pull(task_ids='prepare_config')['output_path'] }}",
            'AWS_INFO_JSON': "{{ ti.xcom_pull(task_ids='prepare_config')['aws'] | tojson }}",
            'CONFIG_JSON': "{{ ti.xcom_pull(task_ids='prepare_config')['config'] | tojson }}",
            'MODEL_STATUS': "{{ ti.xcom_pull(task_ids='prepare_config')['model_status'] }}",
            'CANDIDATE_VERSION': "{{ ti.xcom_pull(task_ids='prepare_config')['candidate_version'] }}",
            'MLFLOW_TRACKING_URI': 'http://mlflow:5000',
        },
        docker_url='unix://var/run/docker.sock',
    )

    config_data >> run_baseline


dag_instance = baseline_eval_pipeline()
