from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

pytestmark = [pytest.mark.live_db, pytest.mark.readonly]

psycopg2 = pytest.importorskip("psycopg2")


FORBIDDEN_SQL = ("insert", "update", "delete", "alter", "drop", "truncate", "create", "copy")


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


def _live_dsn() -> str:
    dsn = os.getenv("LIVE_DB_DSN")
    if dsn:
        return dsn
    required = {
        "host": os.getenv("LIVE_DB_HOST"),
        "port": os.getenv("LIVE_DB_PORT", "5432"),
        "dbname": os.getenv("LIVE_DB_NAME"),
        "user": os.getenv("LIVE_DB_USER"),
        "password": os.getenv("LIVE_DB_PASSWORD"),
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        pytest.fail(f"missing live DB connection env vars: {', '.join(missing)}")
    return " ".join(f"{key}={value}" for key, value in required.items())


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    if os.getenv("ALLOW_LIVE_READONLY_TESTS") != "1":
        pytest.skip("set ALLOW_LIVE_READONLY_TESTS=1 to run live read-only DB tests")
    connection = psycopg2.connect(_live_dsn())
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
            cursor.execute("SET statement_timeout = '5s'")
            cursor.execute("SET lock_timeout = '1s'")
            cursor.execute("SET idle_in_transaction_session_timeout = '10s'")
        yield connection
    finally:
        connection.rollback()
        connection.close()


def execute_readonly(cursor, sql: str, params: tuple | None = None):
    normalized = " ".join(sql.strip().lower().split())
    assert normalized.startswith(("select", "with", "show")), sql
    assert not any(token in normalized for token in FORBIDDEN_SQL), sql
    cursor.execute(sql, params or ())
    return cursor.fetchall()


def test_live_schema_exposes_required_tables_and_columns(conn) -> None:
    table_names = tuple(REQUIRED_COLUMNS)
    with conn.cursor() as cursor:
        rows = execute_readonly(
            cursor,
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            """,
            (list(table_names),),
        )

    columns_by_table: dict[str, set[str]] = {table: set() for table in table_names}
    for table_name, column_name in rows:
        columns_by_table.setdefault(table_name, set()).add(column_name)

    missing = {
        table: sorted(required_columns - columns_by_table.get(table, set()))
        for table, required_columns in REQUIRED_COLUMNS.items()
        if required_columns - columns_by_table.get(table, set())
    }
    assert not missing


def test_live_news_tables_have_joinable_recent_outputs(conn) -> None:
    with conn.cursor() as cursor:
        rows = execute_readonly(
            cursor,
            """
            SELECT COUNT(*)
            FROM public.filtered_news fn
            LEFT JOIN public.naver_news nn ON nn.news_id = fn.news_id
            WHERE fn.news_id IS NOT NULL
              AND nn.news_id IS NULL
            """,
        )
        assert rows[0][0] == 0

        rows = execute_readonly(
            cursor,
            """
            SELECT COUNT(*)
            FROM public.crawled_news cn
            LEFT JOIN public.naver_news nn ON nn.news_id = cn.news_id
            WHERE cn.news_id IS NOT NULL
              AND nn.news_id IS NULL
            """,
        )
        assert rows[0][0] == 0

        rows = execute_readonly(
            cursor,
            """
            SELECT
                MAX(nn.pub_date),
                MAX(cn.crawled_at),
                COUNT(fn.news_id)
            FROM public.naver_news nn
            LEFT JOIN public.crawled_news cn ON cn.news_id = nn.news_id
            LEFT JOIN public.filtered_news fn ON fn.news_id = nn.news_id
            """,
        )
        max_pub_date, max_crawled_at, filtered_count = rows[0]

    assert max_pub_date is not None
    assert max_crawled_at is not None
    assert filtered_count > 0


def test_live_recommendation_snapshots_and_bandit_state_have_valid_shape(conn) -> None:
    with conn.cursor() as cursor:
        rows = execute_readonly(
            cursor,
            """
            SELECT COUNT(*)
            FROM public.recommendation_news_path_metrics
            WHERE path NOT IN ('A1', 'A2', 'B', 'C', 'TOTAL')
            """,
        )
        assert rows[0][0] == 0

        rows = execute_readonly(
            cursor,
            """
            SELECT snapshot_at, cardinality(news_ids)
            FROM public.recommendation_path_c_snapshot
            ORDER BY snapshot_at DESC
            LIMIT 1
            """,
        )
        assert rows
        assert rows[0][0] is not None
        assert rows[0][1] is not None

        rows = execute_readonly(
            cursor,
            """
            SELECT COUNT(*)
            FROM public.recommendation_path_a2_snapshot
            WHERE jsonb_typeof(items) <> 'array'
            """,
        )
        assert rows[0][0] == 0

        rows = execute_readonly(
            cursor,
            """
            SELECT COUNT(*)
            FROM public.recommendation_bandit_state
            WHERE alpha IS NULL
               OR beta IS NULL
               OR path IS NULL
               OR scope NOT IN ('global', 'user')
            """,
        )
        assert rows[0][0] == 0


def test_live_embedding_serving_table_has_entity_types_and_model_version(conn) -> None:
    with conn.cursor() as cursor:
        rows = execute_readonly(
            cursor,
            """
            SELECT lower(entity_type), COUNT(*)
            FROM public.test_service_embeddings
            GROUP BY lower(entity_type)
            """,
        )
        counts_by_type = {entity_type: count for entity_type, count in rows}

        rows = execute_readonly(
            cursor,
            """
            SELECT COUNT(*)
            FROM public.test_service_embeddings
            WHERE model_version IS NOT NULL
            """,
        )
        rows_with_model_version = rows[0][0]

    assert {"news", "keyword", "stock"} <= set(counts_by_type)
    assert all(counts_by_type[entity_type] > 0 for entity_type in ("news", "keyword", "stock"))
    assert rows_with_model_version > 0
