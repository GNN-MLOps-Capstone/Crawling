from __future__ import annotations

import sys
from pathlib import Path

import pytest
import torch

pytest.importorskip("torch_geometric")
from torch_geometric.data import HeteroData

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from modules.training.baseline_eval import _evaluate_embedding_methods

pytestmark = pytest.mark.gnn


def _build_eval_split() -> HeteroData:
    data = HeteroData()
    data["news", "has_keyword", "keyword"].relation_edge_index = torch.tensor(
        [[0, 1], [0, 1]],
        dtype=torch.long,
    )
    data["news", "has_stock", "stock"].relation_edge_index = torch.tensor(
        [[0, 1], [0, 1]],
        dtype=torch.long,
    )
    return data


def test_embedding_baseline_reports_similarity_separation() -> None:
    results = _evaluate_embedding_methods(
        _build_eval_split(),
        {
            "bge": {
                "keyword": torch.tensor([[1.0, 0.0], [0.0, 1.0]]),
                "stock": torch.tensor([[1.0, 0.0], [0.0, 1.0]]),
            },
            "gnn": {
                "keyword": torch.tensor([[1.0, 0.0], [0.0, 1.0]]),
                "stock": torch.tensor([[1.0, 0.0], [0.0, 1.0]]),
            },
        },
        k=1,
        seed=7,
    )

    assert set(results) == {"bge", "gnn"}
    assert results["bge"]["key2stock_positive_sim"] == 1.0
    assert results["bge"]["key2stock_random_sim"] == 0.0
    assert results["bge"]["key2stock_separation"] == 1.0
    assert results["gnn"]["key2stock_hit_at_1"] == 1.0
