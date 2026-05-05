from __future__ import annotations

import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from modules.loading.keyword_loader import KeywordLoader
from modules.loading.news_loader import NewsLoader


REPO_ROOT = Path(__file__).resolve().parents[1]


class _FakeS3Client:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def get_object(self, Bucket: str, Key: str):
        del Bucket
        return {"Body": io.BytesIO(self.objects[Key])}


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    return out.getvalue()


def test_news_loader_builds_filtered_news_upsert_rows(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_bulk_insert(self, sql: str, data: list, page_size: int = 1000):
        del self
        captured["sql"] = sql
        captured["data"] = data
        captured["page_size"] = page_size

    monkeypatch.setattr(NewsLoader, "bulk_insert", fake_bulk_insert)
    s3 = _FakeS3Client(
        {
            "refined.parquet": _parquet_bytes(
                pd.DataFrame(
                    [
                        {
                            "news_id": 1001,
                            "refined_text": "refined body",
                            "news_embedding": np.array([0.1, 0.2]),
                        }
                    ]
                )
            )
        }
    )

    NewsLoader({"host": "postgres"}).load_filtered_news(s3, "silver", "refined.parquet")

    assert "INSERT INTO public.filtered_news" in str(captured["sql"])
    assert "ON CONFLICT (news_id)" in str(captured["sql"])
    assert captured["page_size"] == 500
    assert captured["data"] == [(1001, "refined body", [0.1, 0.2], "bge-m3")]


def test_keyword_loader_builds_master_keyword_upsert_rows(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_bulk_insert(self, sql: str, data: list, page_size: int = 1000):
        del self
        captured["sql"] = sql
        captured["data"] = data
        captured["page_size"] = page_size

    monkeypatch.setattr(KeywordLoader, "bulk_insert", fake_bulk_insert)
    s3 = _FakeS3Client(
        {
            "keyword_embeddings/date=20260309/keyword_embeddings.parquet": _parquet_bytes(
                pd.DataFrame(
                    [
                        {
                            "keyword": "AI",
                            "vector": np.array([0.3, 0.4]),
                        }
                    ]
                )
            )
        }
    )

    KeywordLoader({"host": "postgres"}).sync_master_keywords(
        s3,
        "silver",
        "keyword_embeddings/date=20260309/keyword_embeddings.parquet",
    )

    assert "INSERT INTO public.keywords" in str(captured["sql"])
    assert "ON CONFLICT (word)" in str(captured["sql"])
    assert captured["page_size"] == 1000
    assert captured["data"] == [("AI", [0.3, 0.4])]


def test_recommendation_snapshot_sql_files_expose_downstream_contract_columns() -> None:
    aggregate_sql = (REPO_ROOT / "dags/sql/aggregate_recommendation_news_path_metrics_hourly.sql").read_text(
        encoding="utf-8"
    )
    path_c_sql = (REPO_ROOT / "dags/sql/build_recommendation_path_c_snapshot.sql").read_text(encoding="utf-8")
    path_a2_sql = (REPO_ROOT / "dags/sql/build_recommendation_path_a2_snapshot.sql").read_text(encoding="utf-8")

    assert "INSERT INTO public.recommendation_news_path_metrics" in aggregate_sql
    for column in (
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
    ):
        assert column in aggregate_sql
    assert "ON CONFLICT (bucket_start, bucket_end, news_id, path)" in aggregate_sql

    assert "INSERT INTO public.recommendation_path_c_snapshot" in path_c_sql
    assert "snapshot_at" in path_c_sql
    assert "news_ids" in path_c_sql
    assert "ON CONFLICT (snapshot_at)" in path_c_sql

    assert "INSERT INTO public.recommendation_path_a2_snapshot" in path_a2_sql
    assert "user_id" in path_a2_sql
    assert "items" in path_a2_sql
    assert "snapshot_at" in path_a2_sql
    assert "ON CONFLICT (user_id)" in path_a2_sql
