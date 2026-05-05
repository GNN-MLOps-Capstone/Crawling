from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook


local_tz = pendulum.timezone("Asia/Seoul")
POSTGRES_CONN_ID = "news_data_db"

default_args = {
    "owner": "dongbin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

REQUIRED_COLUMNS = {
    "naver_news": {"news_id", "title", "pub_date", "url", "crawl_status", "created_at", "updated_at"},
    "crawled_news": {"crawled_news_id", "news_id", "text", "crawled_at", "filter_status", "created_at"},
    "filtered_news": {
        "filtered_news_id",
        "news_id",
        "refined_text",
        "news_embedding",
        "embedding_model_version",
        "summary",
        "sentiment",
    },
    "news_keyword_mapping": {"news_id", "keyword_id", "extractor_version", "weight"},
    "news_stock_mapping": {"news_id", "stock_id", "extractor_version", "weight"},
    "test_service_embeddings": {"entity_id", "entity_type", "gnn_embedding", "model_version"},
    "recommendation_news_path_metrics": {
        "bucket_start",
        "bucket_end",
        "news_id",
        "path",
        "impression_count",
        "click_count",
        "dwell_5s_count",
        "dwell_30s_count",
        "dwell_event_count",
        "sum_log_dwell_time",
    },
    "recommendation_path_c_snapshot": {"snapshot_at", "news_ids"},
    "recommendation_path_a2_snapshot": {"user_id", "items", "snapshot_at"},
    "recommendation_bandit_state": {
        "scope",
        "user_id",
        "path",
        "alpha",
        "beta",
        "reward_count",
        "impression_count",
        "window_end",
        "updated_at",
    },
}


def _records(sql: str, parameters=None):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SET LOCAL lock_timeout = '1s'")
            cursor.execute(sql, parameters)
            return cursor.fetchall()


def _raise_if_failures(failures: list[str]) -> None:
    if failures:
        raise ValueError("; ".join(failures))


@dag(
    dag_id="operational_health_check_daily",
    default_args=default_args,
    schedule="30 4 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    params={
        "news_freshness_days": Param(3, type="integer", minimum=1),
        "metrics_freshness_days": Param(2, type="integer", minimum=1),
        "fail_on_stale": Param(False, type="boolean"),
    },
    tags=["health", "readonly", "operations"],
)
def operational_health_check_daily():
    @task
    def check_schema_contracts() -> dict:
        table_names = list(REQUIRED_COLUMNS)
        rows = _records(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            """,
            (table_names,),
        )
        columns_by_table: dict[str, set[str]] = {table: set() for table in table_names}
        for table_name, column_name in rows:
            columns_by_table.setdefault(table_name, set()).add(column_name)

        missing = {
            table: sorted(required_columns - columns_by_table.get(table, set()))
            for table, required_columns in REQUIRED_COLUMNS.items()
            if required_columns - columns_by_table.get(table, set())
        }
        _raise_if_failures([f"{table} missing columns: {columns}" for table, columns in missing.items()])
        return {"checked_tables": len(REQUIRED_COLUMNS), "missing": missing}

    @task
    def check_news_outputs(**context) -> dict:
        params = context.get("params", {}) or {}
        freshness_days = int(params.get("news_freshness_days", 3))
        fail_on_stale = bool(params.get("fail_on_stale", False))
        rows = _records(
            """
            SELECT
                MAX(nn.pub_date),
                MAX(cn.crawled_at),
                COUNT(fn.news_id),
                COUNT(*) FILTER (WHERE fn.news_id IS NOT NULL AND nn.news_id IS NULL),
                COUNT(*) FILTER (WHERE cn.news_id IS NOT NULL AND nn.news_id IS NULL)
            FROM public.naver_news nn
            LEFT JOIN public.crawled_news cn ON cn.news_id = nn.news_id
            LEFT JOIN public.filtered_news fn ON fn.news_id = nn.news_id
            """
        )
        max_pub_date, max_crawled_at, filtered_count, orphan_filtered, orphan_crawled = rows[0]
        failures: list[str] = []
        warnings: list[str] = []

        if max_pub_date is None:
            failures.append("naver_news has no pub_date")
        if max_crawled_at is None:
            failures.append("crawled_news has no crawled_at")
        if int(filtered_count or 0) <= 0:
            failures.append("filtered_news is empty")
        if int(orphan_filtered or 0) > 0:
            failures.append(f"filtered_news orphan rows: {orphan_filtered}")
        if int(orphan_crawled or 0) > 0:
            failures.append(f"crawled_news orphan rows: {orphan_crawled}")

        pub_stale, crawl_stale = _records(
            """
            SELECT
                CASE WHEN MAX(pub_date) < NOW() - (%s || ' days')::interval THEN 1 ELSE 0 END,
                CASE WHEN MAX(crawled_at) < NOW() - (%s || ' days')::interval THEN 1 ELSE 0 END
            FROM public.naver_news nn
            FULL OUTER JOIN public.crawled_news cn ON cn.news_id = nn.news_id
            """,
            (freshness_days, freshness_days),
        )[0]
        if pub_stale:
            warnings.append(f"naver_news pub_date is older than {freshness_days} days")
        if crawl_stale:
            warnings.append(f"crawled_news crawled_at is older than {freshness_days} days")
        if fail_on_stale:
            failures.extend(warnings)

        _raise_if_failures(failures)
        return {
            "max_pub_date": str(max_pub_date),
            "max_crawled_at": str(max_crawled_at),
            "filtered_count": int(filtered_count or 0),
            "warnings": warnings,
        }

    @task
    def check_recommendation_outputs(**context) -> dict:
        params = context.get("params", {}) or {}
        freshness_days = int(params.get("metrics_freshness_days", 2))
        fail_on_stale = bool(params.get("fail_on_stale", False))
        failures: list[str] = []
        warnings: list[str] = []

        invalid_path_count = _records(
            """
            SELECT COUNT(*)
            FROM public.recommendation_news_path_metrics
            WHERE path NOT IN ('A1', 'A2', 'B', 'C', 'TOTAL')
            """
        )[0][0]
        if int(invalid_path_count or 0) > 0:
            failures.append(f"invalid recommendation path rows: {invalid_path_count}")

        latest_metric = _records("SELECT MAX(bucket_end) FROM public.recommendation_news_path_metrics")[0][0]
        if latest_metric is None:
            failures.append("recommendation_news_path_metrics is empty")
        else:
            stale_metric = _records(
                """
                SELECT CASE
                    WHEN MAX(bucket_end) < NOW() - (%s || ' days')::interval THEN 1
                    ELSE 0
                END
                FROM public.recommendation_news_path_metrics
                """,
                (freshness_days,),
            )[0][0]
            if stale_metric:
                warnings.append(f"recommendation metrics are older than {freshness_days} days")

        path_c_rows = _records(
            """
            SELECT snapshot_at, cardinality(news_ids)
            FROM public.recommendation_path_c_snapshot
            ORDER BY snapshot_at DESC
            LIMIT 1
            """
        )
        if not path_c_rows:
            failures.append("recommendation_path_c_snapshot is empty")
        elif path_c_rows[0][1] is None:
            failures.append("latest path C snapshot news_ids is null")

        invalid_a2_items = _records(
            """
            SELECT COUNT(*)
            FROM public.recommendation_path_a2_snapshot
            WHERE jsonb_typeof(items) <> 'array'
            """
        )[0][0]
        if int(invalid_a2_items or 0) > 0:
            failures.append(f"path A2 snapshot rows with non-array items: {invalid_a2_items}")

        invalid_bandit = _records(
            """
            SELECT COUNT(*)
            FROM public.recommendation_bandit_state
            WHERE alpha IS NULL
               OR beta IS NULL
               OR path IS NULL
               OR scope NOT IN ('global', 'user')
            """
        )[0][0]
        if int(invalid_bandit or 0) > 0:
            failures.append(f"invalid bandit state rows: {invalid_bandit}")

        if fail_on_stale:
            failures.extend(warnings)
        _raise_if_failures(failures)
        return {"latest_metric": str(latest_metric), "warnings": warnings}

    @task
    def check_embedding_serving_table() -> dict:
        rows = _records(
            """
            SELECT lower(entity_type), COUNT(*)
            FROM public.test_service_embeddings
            GROUP BY lower(entity_type)
            """
        )
        counts_by_type = {entity_type: int(count) for entity_type, count in rows}
        missing_types = sorted({"news", "keyword", "stock"} - set(counts_by_type))
        rows_with_model_version = _records(
            """
            SELECT COUNT(*)
            FROM public.test_service_embeddings
            WHERE model_version IS NOT NULL
            """
        )[0][0]

        failures = []
        if missing_types:
            failures.append(f"test_service_embeddings missing entity types: {missing_types}")
        if int(rows_with_model_version or 0) <= 0:
            failures.append("test_service_embeddings has no model_version")
        _raise_if_failures(failures)
        return {"counts_by_type": counts_by_type, "rows_with_model_version": int(rows_with_model_version or 0)}

    @task
    def summarize_health(schema_report: dict, news_report: dict, recommendation_report: dict, embedding_report: dict):
        summary = {
            "schema": schema_report,
            "news": news_report,
            "recommendation": recommendation_report,
            "embedding": embedding_report,
        }
        print(f"Operational health summary: {summary}", flush=True)
        return summary

    schema = check_schema_contracts()
    news = check_news_outputs()
    recommendation = check_recommendation_outputs()
    embedding = check_embedding_serving_table()
    summarize_health(schema, news, recommendation, embedding)


dag_instance = operational_health_check_daily()
