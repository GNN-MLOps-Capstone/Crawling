import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param


PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")
MINIO_CONN_ID = 'MINIO_S3'
TARGET_BUCKET = 'silver'

default_args = {
    'owner': 'dongbin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='news_analysis_daily_finalize',
    default_args=default_args,
    schedule='0 3 * * *',
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    params={
        "target_date": Param(None, type=["null", "string"], description="Finalize target date (KST)"),
    },
    tags=['silver', 'daily', 'finalize']
)
def news_analysis_daily_finalize_pipeline():
    @task(multiple_outputs=True)
    def prepare_context(**context):
        params = context.get('params', {}) or {}
        target_date = params.get('target_date') or context['logical_date'].subtract(days=1).to_date_string()

        aws_conn = BaseHook.get_connection(MINIO_CONN_ID)
        aws_info = {
            "access_key": aws_conn.login,
            "secret_key": aws_conn.password,
            "endpoint_url": aws_conn.extra_dejson.get('endpoint_url')
        }
        return {
            "target_date": target_date,
            "aws": aws_info,
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id='collect_incremental_paths')
    def collect_incremental_paths(target_date, aws_info):
        import boto3

        target_nodash = target_date.replace('-', '')
        prefixes = {
            "refined": "refined_news_incremental/window_start=",
            "keywords": "extracted_keywords_incremental/window_start=",
            "stocks": "extracted_stocks_incremental/window_start=",
            "analysis": "analyzed_news_incremental/window_start=",
        }

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_info['access_key'],
            aws_secret_access_key=aws_info['secret_key'],
            endpoint_url=aws_info['endpoint_url'],
        )

        results = {}
        for name, prefix in prefixes.items():
            paginator = s3.get_paginator('list_objects_v2')
            keys = []
            for page in paginator.paginate(Bucket=TARGET_BUCKET, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if f"window_start={target_nodash}" in key and key.endswith('/data.parquet'):
                        keys.append(key)
            results[name] = sorted(keys)
            print(f"📦 [{name}] incremental files: {len(keys)}")

        return results

    @task.external_python(python=PYTHON_VENV_PATH, task_id='merge_refined_daily')
    def merge_refined_daily(target_date, aws_info, collected_paths):
        import io
        import boto3
        import pandas as pd

        refined_keys = collected_paths.get('refined') or []
        if not refined_keys:
            print(f"⚠️ No refined incremental files for {target_date}")
            return None

        y, m, d = target_date.split('-')
        output_key = f"refined_news/year={y}/month={m}/day={d}/data.parquet"
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_info['access_key'],
            aws_secret_access_key=aws_info['secret_key'],
            endpoint_url=aws_info['endpoint_url'],
        )

        frames = []
        for key in refined_keys:
            obj = s3.get_object(Bucket=TARGET_BUCKET, Key=key)
            frames.append(pd.read_parquet(io.BytesIO(obj['Body'].read())))

        merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=['news_id'], keep='last')

        out_buf = io.BytesIO()
        merged.to_parquet(out_buf, index=False)
        s3.put_object(Bucket=TARGET_BUCKET, Key=output_key, Body=out_buf.getvalue())
        print(f"✅ Saved daily refined: {output_key} ({len(merged)} rows)")
        return output_key

    @task.external_python(python=PYTHON_VENV_PATH, task_id='merge_analysis_daily')
    def merge_analysis_daily(target_date, aws_info, collected_paths):
        import io
        import boto3
        import pandas as pd

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_info['access_key'],
            aws_secret_access_key=aws_info['secret_key'],
            endpoint_url=aws_info['endpoint_url'],
        )
        y, m, d = target_date.split('-')
        outputs = {}
        merge_plan = {
            'keywords': {
                'output': f"extracted_keywords/year={y}/month={m}/day={d}/data.parquet",
                'dedupe': ['news_id', 'keyword'],
            },
            'stocks': {
                'output': f"extracted_stocks/year={y}/month={m}/day={d}/data.parquet",
                'dedupe': ['news_id', 'stock_id'],
            },
            'analysis': {
                'output': f"analyzed_news/year={y}/month={m}/day={d}/data.parquet",
                'dedupe': ['news_id'],
            },
        }

        for name, plan in merge_plan.items():
            keys = collected_paths.get(name) or []
            if not keys:
                print(f"⚠️ No {name} incremental files for {target_date}")
                outputs[name] = None
                continue

            frames = []
            for key in keys:
                obj = s3.get_object(Bucket=TARGET_BUCKET, Key=key)
                frames.append(pd.read_parquet(io.BytesIO(obj['Body'].read())))

            merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=plan['dedupe'], keep='last')
            out_buf = io.BytesIO()
            merged.to_parquet(out_buf, index=False)
            s3.put_object(Bucket=TARGET_BUCKET, Key=plan['output'], Body=out_buf.getvalue())
            outputs[name] = plan['output']
            print(f"✅ Saved daily {name}: {plan['output']} ({len(merged)} rows)")

        return outputs

    @task.external_python(python=PYTHON_VENV_PATH, task_id='build_keyword_snapshot')
    def build_keyword_snapshot(target_date, aws_info):
        import sys
        sys.path.append('/opt/airflow/dags')

        from modules.analysis.keyword_embedding_service import run_embedding_update_service

        results = run_embedding_update_service([target_date], aws_info)
        return results[-1] if results else None

    @task
    def finalize_report(target_date, refined_output, analysis_outputs, snapshot_path):
        report = {
            "target_date": target_date,
            "refined": refined_output,
            "keywords": (analysis_outputs or {}).get("keywords"),
            "stocks": (analysis_outputs or {}).get("stocks"),
            "analysis": (analysis_outputs or {}).get("analysis"),
            "keyword_snapshot": snapshot_path,
        }
        print(f"📋 Finalize report: {report}", flush=True)
        return report

    ctx = prepare_context()
    collected = collect_incremental_paths(ctx["target_date"], ctx["aws"])
    refined = merge_refined_daily(ctx["target_date"], ctx["aws"], collected)
    merged = merge_analysis_daily(ctx["target_date"], ctx["aws"], collected)
    snapshot = build_keyword_snapshot(ctx["target_date"], ctx["aws"])
    report = finalize_report(ctx["target_date"], refined, merged, snapshot)

    collected >> refined
    collected >> merged
    merged >> snapshot >> report
    refined >> report


dag_instance = news_analysis_daily_finalize_pipeline()
