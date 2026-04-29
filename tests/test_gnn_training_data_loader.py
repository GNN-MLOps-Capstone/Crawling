from __future__ import annotations

import io
import pickle

import pytest
import torch

torch_geometric = pytest.importorskip("torch_geometric")
from torch_geometric.data import HeteroData

from dags.modules.training import data_loader

pytestmark = pytest.mark.gnn


class _FakeS3FileSystem:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads

    def open(self, path: str, mode: str = "rb") -> io.BytesIO:
        del mode
        return io.BytesIO(self.payloads[path])

    def exists(self, path: str) -> bool:
        return path in self.payloads


def _build_temporal_graph() -> HeteroData:
    data = HeteroData()
    data["news"].x = torch.tensor([[1.0], [2.0], [3.0]], dtype=torch.float)
    data["keyword"].x = torch.tensor([[1.0], [2.0]], dtype=torch.float)
    data["stock"].x = torch.tensor([[1.0], [2.0]], dtype=torch.float)
    data["news"].num_nodes = 3
    data["keyword"].num_nodes = 2
    data["stock"].num_nodes = 2
    data["news"].pub_ts = torch.tensor(
        [
            1735689600,  # 2025-01-01 UTC
            1739145600,  # 2025-02-10 UTC
            1740787200,  # 2025-03-01 UTC
        ],
        dtype=torch.long,
    )
    keyword_edge = torch.tensor([[0, 1, 2], [0, 1, 1]], dtype=torch.long)
    stock_edge = torch.tensor([[0, 1, 2], [0, 1, 1]], dtype=torch.long)
    data["news", "has_keyword", "keyword"].edge_index = keyword_edge
    data["keyword", "rev_has_keyword", "news"].edge_index = torch.stack([keyword_edge[1], keyword_edge[0]], dim=0)
    data["news", "has_stock", "stock"].edge_index = stock_edge
    data["stock", "rev_has_stock", "news"].edge_index = torch.stack([stock_edge[1], stock_edge[0]], dim=0)
    return data


def test_temporal_link_split_uses_train_only_message_passing() -> None:
    data = _build_temporal_graph()

    (train_data, val_data, test_data), edge_types = data_loader.temporal_link_split(
        data=data,
        split_policy="date",
        train_end_date="2025-01-31",
        val_end_date="2025-02-15",
        temporal_message_passing="train_only",
        relation_min_cooccur=1,
    )

    assert edge_types == [
        ("news", "has_keyword", "keyword"),
        ("news", "has_stock", "stock"),
    ]
    assert train_data["news", "has_keyword", "keyword"].relation_edge_index.size(1) == 1
    assert val_data["news", "has_keyword", "keyword"].relation_edge_index.size(1) == 1
    assert test_data["news", "has_keyword", "keyword"].relation_edge_index.size(1) == 1

    expected_train_edges = torch.tensor([[0], [0]], dtype=torch.long)
    expected_full_edges = torch.tensor([[0, 1, 2], [0, 1, 1]], dtype=torch.long)

    assert torch.equal(train_data["news", "has_keyword", "keyword"].edge_index, expected_train_edges)
    assert torch.equal(val_data["news", "has_keyword", "keyword"].edge_index, expected_train_edges)
    assert torch.equal(test_data["news", "has_keyword", "keyword"].edge_index, expected_train_edges)
    assert torch.equal(
        test_data["news", "has_keyword", "keyword"].online_update_edge_index,
        expected_full_edges,
    )


def test_load_data_from_s3_preserves_filtered_serving_mapping(monkeypatch) -> None:
    data = HeteroData()
    data["news"].x = torch.tensor([[1.0], [2.0], [3.0]], dtype=torch.float)
    data["keyword"].x = torch.tensor([[10.0], [20.0]], dtype=torch.float)
    data["stock"].x = torch.tensor([[30.0], [40.0]], dtype=torch.float)
    data["news"].num_nodes = 3
    data["keyword"].num_nodes = 2
    data["stock"].num_nodes = 2
    data["news", "has_keyword", "keyword"].edge_index = torch.tensor(
        [[0, 1, 2], [0, 0, 1]],
        dtype=torch.long,
    )
    data["keyword", "rev_has_keyword", "news"].edge_index = torch.tensor(
        [[0, 0, 1], [0, 1, 2]],
        dtype=torch.long,
    )
    data["news", "has_stock", "stock"].edge_index = torch.tensor(
        [[0, 1, 2], [0, 0, 1]],
        dtype=torch.long,
    )
    data["stock", "rev_has_stock", "news"].edge_index = torch.tensor(
        [[0, 0, 1], [0, 1, 2]],
        dtype=torch.long,
    )

    graph_bytes = io.BytesIO()
    torch.save(data, graph_bytes)

    mapping_bytes = io.BytesIO()
    pickle.dump(
        {
            "news": {"id_to_idx": {1: 0, 2: 1, 3: 2}, "meta": {1: "2026-03-10T00:00:00"}},
            "keyword": {"id_to_idx": {100: 0, 101: 1}, "meta": {100: "alpha", 101: "beta"}},
            "stock": {"id_to_idx": {200: 0, 201: 1}, "meta": {200: "005930", 201: "000660"}},
        },
        mapping_bytes,
    )

    fake_fs = _FakeS3FileSystem(
        {
            "silver/trainset/date=20260310/hetero_graph.pt": graph_bytes.getvalue(),
            "silver/trainset/date=20260310/node_mapping.pkl": mapping_bytes.getvalue(),
        }
    )
    monkeypatch.setattr(data_loader.s3fs, "S3FileSystem", lambda **_: fake_fs)

    filtered_data, name_to_idx_map, _, serving_mapping = data_loader.load_data_from_s3(
        "trainset/date=20260310/hetero_graph.pt",
        {
            "access_key": "a",
            "secret_key": "b",
            "endpoint_url": "http://minio:9000",
        },
        config={
            "data_processing": {
                "min_freq_keyword": 2,
                "min_freq_stock": 2,
                "min_pmi": 100.0,
                "min_cooccur": 100,
            },
            "training": {"split_mode": "random"},
        },
    )

    assert filtered_data["keyword"].num_nodes == 1
    assert filtered_data["stock"].num_nodes == 1
    assert name_to_idx_map["keyword"] == {"alpha": 0}
    assert name_to_idx_map["stock"] == {"005930": 0}
    assert serving_mapping["keyword"]["id_to_idx"] == {100: 0}
    assert serving_mapping["stock"]["id_to_idx"] == {200: 0}
    assert 101 not in serving_mapping["keyword"]["meta"]
    assert 201 not in serving_mapping["stock"]["meta"]
