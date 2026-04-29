from __future__ import annotations

import pickle
from dataclasses import dataclass, field

import numpy as np
import pytest

pytest.importorskip("mlflow")

from dags.modules.serving import deploy

pytestmark = pytest.mark.gnn


@dataclass
class _RunInfo:
    run_id: str


@dataclass
class _RunData:
    tags: dict[str, str]


@dataclass
class _Run:
    run_id: str
    tags: dict[str, str]

    @property
    def info(self) -> _RunInfo:
        return _RunInfo(run_id=self.run_id)

    @property
    def data(self) -> _RunData:
        return _RunData(tags=self.tags)


@dataclass
class _FakeMlflowClient:
    selected_runs: list[_Run]
    product_runs: list[_Run]
    set_tags: list[tuple[str, str, str]] = field(default_factory=list)
    search_calls: int = 0

    def get_experiment_by_name(self, name: str):
        return type("Experiment", (), {"experiment_id": "exp-1", "name": name})()

    def search_runs(self, experiment_ids, filter_string, order_by, max_results):
        del experiment_ids, filter_string, order_by, max_results
        self.search_calls += 1
        if self.search_calls == 1:
            return self.selected_runs
        return self.product_runs

    def set_tag(self, run_id: str, key: str, value: str) -> None:
        self.set_tags.append((run_id, key, value))


class _FakeCursor:
    def close(self) -> None:
        return None


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.committed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        raise AssertionError("rollback should not be called")

    def close(self) -> None:
        return None


def test_run_deploy_inserts_embeddings_and_promotes_model(monkeypatch, tmp_path) -> None:
    emb_path = tmp_path / "node_embeddings.pkl"
    map_path = tmp_path / "node_mapping.pkl"
    with emb_path.open("wb") as handle:
        pickle.dump(
            {
                "news": np.array([[0.1, 0.2]]),
                "keyword": np.array([[0.3, 0.4]]),
                "stock": np.array([[0.5, 0.6]]),
            },
            handle,
        )
    with map_path.open("wb") as handle:
        pickle.dump(
            {
                "news": {"id_to_idx": {1001: 0}, "meta": {1001: "2026-03-10T00:00:00"}},
                "keyword": {"id_to_idx": {7: 0}, "meta": {7: "AI"}},
                "stock": {"id_to_idx": {"005930": 0}, "meta": {"005930": "Samsung Electronics"}},
            },
            handle,
        )

    client = _FakeMlflowClient(
        selected_runs=[_Run("run-new", {"candidate_version": "v_20260310"})],
        product_runs=[_Run("run-old", {"status": "product"}), _Run("run-new", {"status": "candidate"})],
    )
    inserted_rows: list[tuple[str, str, object, list[float], str]] = []
    connection = _FakeConnection()

    monkeypatch.setattr(deploy.mlflow, "set_tracking_uri", lambda tracking_uri: None)
    monkeypatch.setattr(deploy, "MlflowClient", lambda tracking_uri: client)
    monkeypatch.setattr(
        deploy.mlflow.artifacts,
        "download_artifacts",
        lambda run_id, artifact_path: str(emb_path if artifact_path == "node_embeddings.pkl" else map_path),
    )
    monkeypatch.setattr(deploy.psycopg2, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        deploy,
        "execute_values",
        lambda cursor, query, values, page_size: inserted_rows.extend(values),
    )

    deploy.run_deploy(
        {"experiment_name": "News_GNN_v1", "status": "candidate", "candidate_version": "v_20260310"},
        {"access_key": "a", "secret_key": "b", "endpoint_url": "http://minio:9000"},
        {"host": "postgres", "port": 5432, "user": "user", "password": "pw", "dbname": "db"},
    )

    assert connection.committed is True
    assert inserted_rows == [
        ("1001", "news", "2026-03-10T00:00:00", [0.1, 0.2], "v_20260310"),
        ("7", "keyword", "AI", [0.3, 0.4], "v_20260310"),
        ("005930", "stock", "Samsung Electronics", [0.5, 0.6], "v_20260310"),
    ]
    assert ("run-old", "status", "legacy") in client.set_tags
    assert ("run-new", "status", "product") in client.set_tags
