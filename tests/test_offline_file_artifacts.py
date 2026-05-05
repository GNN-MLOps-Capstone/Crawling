from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from modules.ingestion import writer
from modules.analysis import post_gemini_filter
from modules.window_utils import build_incremental_key


class _WritableBytesIO(io.BytesIO):
    def __init__(self, on_close):
        super().__init__()
        self._on_close = on_close

    def close(self) -> None:
        if not self.closed:
            self._on_close(self.getvalue())
        super().close()


class _FakeS3FileSystem:
    def __init__(self) -> None:
        self.storage: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.storage

    def open(self, path: str, mode: str = "rb"):
        if "w" in mode:
            return _WritableBytesIO(lambda payload: self.storage.__setitem__(path, payload))
        return io.BytesIO(self.storage[path])


class _FakeS3Client:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.puts: dict[str, bytes] = {}

    def get_object(self, Bucket: str, Key: str):
        del Bucket
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes):
        del Bucket
        self.objects[Key] = Body
        self.puts[Key] = Body


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    return out.getvalue()


def _read_parquet_bytes(payload: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(payload))


def test_bronze_window_writer_creates_incremental_parquet_with_expected_schema(monkeypatch) -> None:
    fake_fs = _FakeS3FileSystem()
    monkeypatch.setattr(writer.s3fs, "S3FileSystem", lambda **_: fake_fs)

    object_key = build_incremental_key(
        "crawled_news_incremental",
        "2026-03-09 01:00:00+09:00",
        "2026-03-09 02:00:00+09:00",
    )
    news_df = pd.DataFrame(
        [
            {
                "news_id": 1001,
                "title": "title",
                "url": "https://example.com/news",
                "pub_date": "2026-03-09 01:10:00",
                "text": "body",
            }
        ]
    )

    saved_keys = writer.write_news_window_to_minio(
        news_df,
        "bronze",
        {"access_key": "a", "secret_key": "b", "endpoint_url": "http://minio:9000"},
        object_key,
    )

    assert saved_keys == [object_key]
    stored_df = _read_parquet_bytes(fake_fs.storage[f"bronze/{object_key}"])
    assert set(stored_df.columns) >= {"news_id", "title", "url", "pub_date", "text"}
    assert stored_df.to_dict("records")[0]["news_id"] == 1001


def test_post_gemini_filter_rewrites_incremental_parquets_and_captures_filtered_ids(monkeypatch, tmp_path) -> None:
    paths = {
        "refined": "refined_news_incremental/window_start=202603090100/window_end=202603090200/data.parquet",
        "stocks": "extracted_stocks_incremental/window_start=202603090100/window_end=202603090200/data.parquet",
        "keywords": "extracted_keywords_incremental/window_start=202603090100/window_end=202603090200/data.parquet",
        "analysis": "analyzed_news_incremental/window_start=202603090100/window_end=202603090200/data.parquet",
    }
    refined_df = pd.DataFrame(
        [
            {"news_id": 1, "refined_text": "mapped news"},
            {"news_id": 2, "refined_text": "alpha beta gamma"},
            {"news_id": 3, "refined_text": "unrelated"},
        ]
    )
    stocks_df = pd.DataFrame([{"news_id": 1, "stock_id": "005930"}])
    keywords_df = pd.DataFrame(
        [
            {"news_id": 1, "keyword": "mapped"},
            {"news_id": 2, "keyword": "alpha"},
            {"news_id": 3, "keyword": "noise"},
        ]
    )
    analysis_df = pd.DataFrame(
        [
            {"news_id": 1, "summary": "mapped"},
            {"news_id": 2, "summary": "included"},
            {"news_id": 3, "summary": "filtered"},
        ]
    )
    fake_s3 = _FakeS3Client(
        {
            paths["refined"]: _parquet_bytes(refined_df),
            paths["stocks"]: _parquet_bytes(stocks_df),
            paths["keywords"]: _parquet_bytes(keywords_df),
            paths["analysis"]: _parquet_bytes(analysis_df),
        }
    )
    monkeypatch.setattr(post_gemini_filter.boto3, "client", lambda *args, **kwargs: fake_s3)

    status_updates: list[set[int]] = []
    monkeypatch.setattr(
        post_gemini_filter,
        "_update_filter_status",
        lambda db_info, news_ids, filter_version, filter_reason: status_updates.append(set(news_ids)) or len(news_ids),
    )
    config_path = tmp_path / "analysis_config.yaml"
    config_path.write_text(
        """
pipeline:
  filter_version: test_filter_v1
post_gemini_filter:
  enabled: true
  include_terms: [alpha, beta, gamma]
  exclude_terms: []
  min_include_matches: 3
  max_exclude_matches: 0
  filter_reason: test_reason
""",
        encoding="utf-8",
    )

    result = post_gemini_filter.run_post_gemini_filter_for_incremental(
        paths,
        {"access_key": "a", "secret_key": "b", "endpoint_url": "http://minio:9000"},
        {"host": "postgres"},
        "silver",
        str(config_path),
    )

    assert result == paths
    assert status_updates == [{3}]
    assert set(fake_s3.puts) == set(paths.values())
    assert set(_read_parquet_bytes(fake_s3.objects[paths["refined"]])["news_id"]) == {1, 2}
    assert set(_read_parquet_bytes(fake_s3.objects[paths["keywords"]])["news_id"]) == {1, 2}
    assert set(_read_parquet_bytes(fake_s3.objects[paths["analysis"]])["news_id"]) == {1, 2}
