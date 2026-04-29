from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import torch

pytest.importorskip("mlflow")
pytest.importorskip("torch_geometric")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from modules.training import trainer

pytestmark = pytest.mark.gnn


class _FakeGraph:
    def __init__(self) -> None:
        self.x_dict = {"news": torch.tensor([[1.0]], dtype=torch.float)}
        self.edge_index_dict = {}

    def to(self, device):
        del device
        return self


class _FakeModel:
    def to(self, device):
        del device
        return self

    def load_state_dict(self, state_dict) -> None:
        del state_dict

    def eval(self):
        return self

    def encoder(self, x_dict, edge_index_dict):
        del x_dict, edge_index_dict
        return {"news": torch.tensor([[1.0, 0.0]], dtype=torch.float)}


class _FakeRun:
    class _Info:
        run_id = "run-1"

    info = _Info()


class _FakeRunContext:
    def __enter__(self):
        return _FakeRun()

    def __exit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return False


@pytest.mark.parametrize(
    ("final_score", "missing", "should_promote"),
    [
        (0.31, 0.0, True),
        (0.31, 1.0, False),
        (0.23, 0.0, False),
    ],
)
def test_run_evaluation_stage_applies_candidate_gate(
    monkeypatch,
    tmp_path,
    final_score: float,
    missing: float,
    should_promote: bool,
) -> None:
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    (stage_dir / "config_snapshot.json").write_text(
        json.dumps(
            {
                "model": {"hidden_dim": 1, "out_dim": 1},
                "training": {"epochs": 3, "val_ratio": 0.2, "test_ratio": 0.2},
                "mlflow": {"experiment_name": "News_GNN_v1"},
            }
        ),
        encoding="utf-8",
    )
    (stage_dir / "stage_metadata.json").write_text(
        json.dumps(
            {
                "trainset_path": "trainset/date=20260310/hetero_graph.pt",
                "output_path": "models/gnn/v_20260310/",
                "gate_threshold": 0.23,
                "eval_k": 10,
                "implicit_eval_seed": 42,
                "implicit_sample_limit": 100,
                "best_epoch": 2,
            }
        ),
        encoding="utf-8",
    )
    torch.save({}, stage_dir / "best_model.pt")
    torch.save({}, stage_dir / "final_model.pt")

    raw_graph = _FakeGraph()
    test_graph = _FakeGraph()
    mlflow_tags: list[tuple[str, str]] = []

    def fake_evaluate(*args, **kwargs):
        split_prefix = kwargs["split_prefix"]
        if split_prefix == "holdout":
            return {"holdout_final_score": 0.11, "holdout_final_score_missing": 0.0}
        if split_prefix == "holdout_after_update":
            return {"holdout_after_update_final_score": 0.12, "holdout_after_update_final_score_missing": 0.0}
        if split_prefix == "holdout_final":
            return {"holdout_final_final_score": final_score, "holdout_final_final_score_missing": missing}
        if split_prefix == "holdout_final_after_update":
            return {
                "holdout_final_after_update_final_score": final_score,
                "holdout_final_after_update_final_score_missing": missing,
            }
        raise AssertionError(f"unexpected split prefix: {split_prefix}")

    monkeypatch.setattr(trainer, "_prepare_runtime_env", lambda aws_info: None)
    monkeypatch.setattr(trainer, "load_data_from_s3", lambda *args, **kwargs: (raw_graph, {}, None, {"news": {"id_to_idx": {1: 0}, "meta": {}}}))
    monkeypatch.setattr(trainer, "load_json_file", lambda filename: [])
    monkeypatch.setattr(trainer, "preprocess_data", lambda *args, **kwargs: ((None, None, test_graph), [("news", "has_keyword", "keyword")]))
    monkeypatch.setattr(trainer, "_build_model", lambda raw_data, config, device: _FakeModel())
    monkeypatch.setattr(trainer, "_build_relation_ground_truth_edge_index_dict", lambda data, target_edge_types: {})
    monkeypatch.setattr(trainer, "_build_message_passing_edge_index_dict", lambda data, target_edge_types, use_online_update=False: {})
    monkeypatch.setattr(trainer, "evaluate", fake_evaluate)
    monkeypatch.setattr(trainer, "build_gold_similarity_report", lambda *args, **kwargs: "")
    monkeypatch.setattr(trainer.mlflow, "set_experiment", lambda experiment_name: None)
    monkeypatch.setattr(trainer.mlflow, "start_run", lambda **kwargs: _FakeRunContext())
    monkeypatch.setattr(trainer.mlflow, "log_metrics", lambda metrics, step=None: None)
    monkeypatch.setattr(trainer.mlflow, "log_metric", lambda key, value, step=None: None)
    monkeypatch.setattr(trainer.mlflow, "log_artifact", lambda path: None)
    monkeypatch.setattr(trainer.mlflow, "set_tag", lambda key, value: mlflow_tags.append((key, value)))
    monkeypatch.setattr(trainer.torch.cuda, "is_available", lambda: False)

    score = trainer.run_evaluation_stage(
        str(stage_dir),
        {"access_key": "a", "secret_key": "b", "endpoint_url": "http://minio:9000"},
    )

    assert score == final_score
    assert ("candidate_version", "v_20260310") in mlflow_tags
    if should_promote:
        assert ("status", "candidate") in mlflow_tags
        assert ("gate_passed", "true") in mlflow_tags
    else:
        assert ("status", "candidate") not in mlflow_tags
        assert ("gate_passed", "false") in mlflow_tags
