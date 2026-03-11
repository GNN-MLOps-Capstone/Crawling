import pendulum
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


PYTHON_VENV_PATH = '/opt/airflow/venv_nlp/bin/python'
local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = 'news_data_db'
MINIO_CONN_ID = 'MINIO_S3'
BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'
CONFIG_PATH = '/opt/airflow/dags/config/analysis_config.yaml'

default_args = {
    'owner': 'dongbin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='news_ingestion_integration_hourly',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    params={
        "window_start": Param(None, type=["null", "string"], description="KST window start"),
        "window_end": Param(None, type=["null", "string"], description="KST window end"),
    },
    tags=['bronze', 'silver', 'hourly', 'incremental']
)
def news_ingestion_integration_hourly_pipeline():
    @task(multiple_outputs=True)
    def prepare_context(**context):
        import sys
        sys.path.append('/opt/airflow/dags')

        from airflow.models import Variable
        from modules.window_utils import build_window_context

        params = context.get('params', {}) or {}
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        interval_start = context['data_interval_start'].in_timezone('Asia/Seoul')
        interval_end = context['data_interval_end'].in_timezone('Asia/Seoul')

        window_start = conf.get('window_start') or params.get('window_start') or interval_start.to_datetime_string()
        window_end = conf.get('window_end') or params.get('window_end') or interval_end.to_datetime_string()

        conn = BaseHook.get_connection(MINIO_CONN_ID)
        aws_info = {
            "access_key": conn.login,
            "secret_key": conn.password,
            "endpoint_url": conn.extra_dejson.get('endpoint_url'),
            "gemini_api_key": Variable.get("GEMINI_API_KEY", default_var=""),
        }

        return {
            **build_window_context(window_start, window_end),
            "aws": aws_info,
        }

    @task
    def prepare_db_context():
        pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        return {
            "host": pg_conn.host,
            "user": pg_conn.login,
            "password": pg_conn.password,
            "dbname": pg_conn.schema,
            "port": pg_conn.port
        }

    @task
    def get_stock_mapping():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        df = pg_hook.get_pandas_df("SELECT stock_id, stock_name FROM public.stocks")
        return {
            row['stock_name'].lower(): {'id': row['stock_id'], 'name': row['stock_name']}
            for _, row in df.iterrows()
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id='bronze_ingest_window')
    def bronze_ingest_window(window_start, window_end, window_label, db_info, aws_info, bronze_bucket):
        import sys
        sys.path.append('/opt/airflow/dags')

        from modules.ingestion.reader import read_news_by_time_window
        from modules.ingestion.writer import write_news_window_to_minio
        from modules.window_utils import build_incremental_key

        news_df = read_news_by_time_window(window_start, window_end, db_info)
        if news_df.empty:
            return {
                "window_start": window_start,
                "window_end": window_end,
                "window_label": window_label,
                "bronze_keys": [],
                "news_ids": [],
            }

        object_key = build_incremental_key(
            "crawled_news_incremental",
            window_start,
            window_end,
        )
        saved_keys = write_news_window_to_minio(news_df, bronze_bucket, aws_info, object_key)
        news_ids = news_df["news_id"].dropna().astype("int64").tolist()
        return {
            "window_start": window_start,
            "window_end": window_end,
            "window_label": window_label,
            "bronze_keys": saved_keys,
            "news_ids": news_ids,
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id='resolve_target_news')
    def resolve_target_news(window_start, window_end, window_label, db_info):
        import sys
        sys.path.append('/opt/airflow/dags')

        from modules.analysis.window_selector import resolve_target_news_ids_by_window

        news_ids = resolve_target_news_ids_by_window(window_start, window_end, db_info)
        return {
            "window_start": window_start,
            "window_end": window_end,
            "window_label": window_label,
            "news_ids": news_ids,
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id='refinement_incremental')
    def refinement_incremental(window_ctx, bronze_result, target_result, aws_info, pg_info, bronze_bucket, config_path):
        import sys
        sys.path.append('/opt/airflow/dags')

        from modules.analysis.news_service import run_refinement_process_for_window
        from modules.window_utils import build_incremental_key

        bronze_keys = bronze_result.get("bronze_keys") or []
        target_news_ids = target_result.get("news_ids") or []
        output_key = build_incremental_key(
            "refined_news_incremental",
            window_ctx["window_start"],
            window_ctx["window_end"],
        )

        if not bronze_keys or not target_news_ids:
            return {
                "window_label": window_ctx["window_label"],
                "refined": None,
                "news_ids": target_news_ids,
            }

        refined_key = run_refinement_process_for_window(
            window_start=window_ctx["window_start"],
            window_end=window_ctx["window_end"],
            aws_info=aws_info,
            db_info=pg_info,
            output_key=output_key,
            config_path=config_path,
            input_bucket=bronze_bucket,
            input_key=bronze_keys[0],
            target_news_ids=target_news_ids,
        )
        return {
            "window_label": window_ctx["window_label"],
            "refined": refined_key,
            "news_ids": target_news_ids,
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id='gemini_incremental')
    def gemini_incremental(window_ctx, refined_result, aws_info, stock_map, pg_info, silver_bucket, config_path):
        import sys
        sys.path.append('/opt/airflow/dags')

        from modules.analysis.gemini_service import run_gemini_service_for_parquet
        from modules.window_utils import build_incremental_key

        refined_key = refined_result.get("refined")
        if not refined_key:
            return {
                "window_label": window_ctx["window_label"],
                "refined": None,
                "stocks": None,
                "keywords": None,
                "analysis": None,
            }

        output_keys = {
            "stocks": build_incremental_key(
                "extracted_stocks_incremental",
                window_ctx["window_start"],
                window_ctx["window_end"],
            ),
            "keywords": build_incremental_key(
                "extracted_keywords_incremental",
                window_ctx["window_start"],
                window_ctx["window_end"],
            ),
            "analysis": build_incremental_key(
                "analyzed_news_incremental",
                window_ctx["window_start"],
                window_ctx["window_end"],
            ),
        }

        saved = run_gemini_service_for_parquet(
            input_bucket=silver_bucket,
            input_key=refined_key,
            output_keys=output_keys,
            aws_info=aws_info,
            config_path=config_path,
            stock_map=stock_map,
            db_info=pg_info,
        )
        return {
            "window_label": window_ctx["window_label"],
            "refined": refined_key,
            "stocks": saved.get("stocks"),
            "keywords": saved.get("keywords"),
            "analysis": saved.get("analysis"),
        }

    @task.external_python(python=PYTHON_VENV_PATH, task_id='load_incremental_to_db')
    def load_incremental_to_db(window_label, aws_info, pg_info, incremental_paths):
        import sys
        sys.path.append('/opt/airflow/dags')

        from modules.loading.load_service import run_incremental_db_loading

        if not incremental_paths.get("refined"):
            return "No incremental output to load"

        return run_incremental_db_loading(
            target={
                "window_label": window_label,
                "refined": incremental_paths.get("refined"),
                "stocks": incremental_paths.get("stocks"),
                "keywords": incremental_paths.get("keywords"),
                "analysis": incremental_paths.get("analysis"),
            },
            aws_info=aws_info,
            pg_info=pg_info,
            snapshot_path=None,
        )

    @task
    def report_window_status(window_start, window_end, stage):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT filter_status::text AS status, COUNT(*) AS cnt
            FROM public.crawled_news
            WHERE cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' >= %s
              AND cn.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' < %s
            GROUP BY filter_status
            ORDER BY filter_status::text
        """
        sql = sql.replace("cn.", "")
        rows = pg_hook.get_records(sql, parameters=(window_start, window_end))
        result = {status: int(cnt) for status, cnt in rows}
        print(f"[{stage}] {window_start} ~ {window_end} filter_status counts: {result}", flush=True)
        return result

    @task(multiple_outputs=True)
    def build_recommendation_metrics_conf(window_start, window_end):
        kst = ZoneInfo("Asia/Seoul")
        start_dt = datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=kst)
        end_dt = datetime.strptime(window_end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=kst)
        return {
            "window_start": start_dt.astimezone(ZoneInfo("UTC")).isoformat(),
            "window_end": end_dt.astimezone(ZoneInfo("UTC")).isoformat(),
        }

    ctx = prepare_context()
    pg_info = prepare_db_context()
    stock_map = get_stock_mapping()
    status_before = report_window_status(ctx["window_start"], ctx["window_end"], "before")

    bronze = bronze_ingest_window(
        ctx["window_start"],
        ctx["window_end"],
        ctx["window_label"],
        pg_info,
        ctx["aws"],
        BRONZE_BUCKET,
    )
    targets = resolve_target_news(
        ctx["window_start"],
        ctx["window_end"],
        ctx["window_label"],
        pg_info,
    )
    refined = refinement_incremental(ctx, bronze, targets, ctx["aws"], pg_info, BRONZE_BUCKET, CONFIG_PATH)
    analyzed = gemini_incremental(ctx, refined, ctx["aws"], stock_map, pg_info, SILVER_BUCKET, CONFIG_PATH)
    loaded = load_incremental_to_db(ctx["window_label"], ctx["aws"], pg_info, analyzed)
    status_after = report_window_status(ctx["window_start"], ctx["window_end"], "after")
    metrics_conf = build_recommendation_metrics_conf(ctx["window_start"], ctx["window_end"])
    trigger_recommendation_metrics = TriggerDagRunOperator(
        task_id="trigger_recommendation_news_path_metrics_hourly",
        trigger_dag_id="recommendation_news_path_metrics_hourly",
        conf={
            "window_start": "{{ ti.xcom_pull(task_ids='build_recommendation_metrics_conf')['window_start'] }}",
            "window_end": "{{ ti.xcom_pull(task_ids='build_recommendation_metrics_conf')['window_end'] }}",
        },
        wait_for_completion=False,
    )

    status_before >> bronze >> targets >> refined >> analyzed >> loaded >> status_after >> metrics_conf >> trigger_recommendation_metrics


dag_instance = news_ingestion_integration_hourly_pipeline()
