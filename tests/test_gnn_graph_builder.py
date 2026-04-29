from __future__ import annotations

import io
import pickle
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest
import torch

from dags.modules.dataset import graph_builder

pytestmark = pytest.mark.gnn


@dataclass
class _NamedBuffer(io.BytesIO):
    name: str


class _FakeS3FileSystem:
    def __init__(self) -> None:
        self.storage_options: dict[str, object] = {}
        self.uploads: dict[str, bytes] = {}

    def open(self, path: str, mode: str = "rb") -> _NamedBuffer:
        del mode
        return _NamedBuffer(name=path)

    def put(self, local_path: str, remote_path: str) -> None:
        with open(local_path, "rb") as handle:
            self.uploads[remote_path] = handle.read()


class _FakeConnection:
    def close(self) -> None:
        return None


def test_run_graph_building_creates_filtered_graph_and_mapping(monkeypatch) -> None:
    fake_fs = _FakeS3FileSystem()

    keyword_snapshot = pd.DataFrame(
        {
            "keyword": ["alpha", "beta"],
            "vector": [np.array([1.0, 0.0]), np.array([0.0, 1.0])],
        }
    )
    stock_snapshot = pd.DataFrame(
        {
            "stock_id": ["005930", "000660"],
            "summary_embedding": [np.array([0.1, 0.2]), np.array([0.3, 0.4])],
        }
    )
    refined_news = pd.DataFrame(
        {
            "news_id": [10, 11, 12],
            "pub_date": pd.to_datetime(["2026-03-09", "2026-03-10", "2026-03-11"]),
            "news_embedding": [
                np.array([0.5, 0.1]),
                np.array([0.2, 0.7]),
                np.array([0.9, 0.9]),
            ],
        }
    )
    extracted_keywords = pd.DataFrame(
        {
            "news_id": [10, 11, 12],
            "keyword": ["alpha", "beta", "alpha"],
        }
    )
    extracted_stocks = pd.DataFrame(
        {
            "news_id": [10, 11, 12],
            "stock_id": ["005930", "000660", "005930"],
        }
    )
    keyword_rows = pd.DataFrame(
        {
            "keyword_id": [101, 102],
            "word": ["alpha", "beta"],
        }
    )
    stock_rows = pd.DataFrame(
        {
            "stock_id": ["005930", "000660"],
            "stock_name": ["Samsung Electronics", "SK hynix"],
        }
    )

    def fake_read_parquet(source, *args, **kwargs):
        del args, kwargs
        source_name = source.name if hasattr(source, "name") else str(source)
        if "keyword_embeddings" in source_name:
            return keyword_snapshot.copy()
        if "stock_embeddings" in source_name:
            return stock_snapshot.copy()
        if "refined_news" in source_name:
            return refined_news.copy()
        if "extracted_keywords" in source_name:
            return extracted_keywords.copy()
        if "extracted_stocks" in source_name:
            return extracted_stocks.copy()
        raise AssertionError(f"unexpected parquet source: {source_name}")

    def fake_read_sql(query: str, conn) -> pd.DataFrame:
        del conn
        if "FROM public.keywords" in query:
            return keyword_rows.copy()
        if "FROM public.stocks" in query:
            return stock_rows.copy()
        raise AssertionError(f"unexpected sql query: {query}")

    monkeypatch.setattr(graph_builder.s3fs, "S3FileSystem", lambda **_: fake_fs)
    monkeypatch.setattr(graph_builder.psycopg2, "connect", lambda **_: _FakeConnection())
    monkeypatch.setattr(graph_builder.pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(graph_builder.pd, "read_sql", fake_read_sql)

    remote_path = graph_builder.run_graph_building(
        "2026-03-10",
        {
            "keyword_snapshot": "keyword_embeddings/date=20260310/keyword_embeddings.parquet",
            "stock_snapshot": "stock_embeddings/stock_embeddings_20260310.parquet",
            "output": "trainset/date=20260310/hetero_graph.pt",
        },
        {
            "access_key": "a",
            "secret_key": "b",
            "endpoint_url": "http://minio:9000",
        },
        {
            "host": "postgres",
            "port": 5432,
            "user": "user",
            "password": "pw",
            "dbname": "db",
        },
    )

    assert remote_path == "silver/trainset/date=20260310/hetero_graph.pt"

    graph_bytes = fake_fs.uploads["silver/trainset/date=20260310/hetero_graph.pt"]
    mapping_bytes = fake_fs.uploads["silver/trainset/date=20260310/node_mapping.pkl"]

    graph = torch.load(io.BytesIO(graph_bytes), weights_only=False)
    mapping = pickle.load(io.BytesIO(mapping_bytes))

    assert graph["news"].num_nodes == 2
    assert graph["keyword"].num_nodes == 2
    assert graph["stock"].num_nodes == 2
    assert graph["news", "has_keyword", "keyword"].edge_index.size(1) == 2
    assert graph["news", "has_stock", "stock"].edge_index.size(1) == 2
    assert mapping["news"]["id_to_idx"] == {10: 0, 11: 1}
    assert mapping["keyword"]["meta"] == {101: "alpha", 102: "beta"}
    assert mapping["stock"]["meta"] == {
        "005930": "Samsung Electronics",
        "000660": "SK hynix",
    }
