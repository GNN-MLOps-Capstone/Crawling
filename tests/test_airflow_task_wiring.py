from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"


def _source(relative_path: str) -> str:
    return (DAGS_DIR / relative_path).read_text(encoding="utf-8")


def _tree(relative_path: str) -> ast.Module:
    return ast.parse(_source(relative_path), filename=relative_path)


def _constant_value(node: ast.AST) -> object:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.JoinedStr):
        return "".join(
            value.value if isinstance(value, ast.Constant) else "{}"
            for value in node.values
        )
    return None


def _keyword(call: ast.Call, name: str) -> ast.AST | None:
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    return None


def _dict_value(dict_node: ast.AST | None, key: str) -> ast.AST | None:
    if not isinstance(dict_node, ast.Dict):
        return None
    for key_node, value_node in zip(dict_node.keys, dict_node.values):
        if isinstance(key_node, ast.Constant) and key_node.value == key:
            return value_node
    return None


def _dict_keys(dict_node: ast.AST | None) -> set[str]:
    if not isinstance(dict_node, ast.Dict):
        return set()
    return {
        key.value
        for key in dict_node.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }


def _calls_by_name(tree: ast.AST, name: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == name)
            or (isinstance(node.func, ast.Attribute) and node.func.attr == name)
        )
    ]


def _operator_call(tree: ast.AST, operator_name: str, task_id: str) -> ast.Call:
    for call in _calls_by_name(tree, operator_name):
        task_id_node = _keyword(call, "task_id")
        if _constant_value(task_id_node) == task_id:
            return call
    raise AssertionError(f"{operator_name} task_id={task_id!r} not found")


def _task_decorator_ids(tree: ast.AST) -> set[str]:
    task_ids: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == "task":
                task_ids.add(node.name)
            if isinstance(decorator, ast.Attribute) and decorator.attr in {"external_python", "short_circuit"}:
                task_ids.add(node.name)
            if isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name) and decorator.func.id == "task":
                    task_ids.add(str(_constant_value(_keyword(decorator, "task_id")) or node.name))
                if isinstance(decorator.func, ast.Attribute) and decorator.func.attr in {
                    "external_python",
                    "short_circuit",
                }:
                    task_ids.add(str(_constant_value(_keyword(decorator, "task_id")) or node.name))
    return task_ids


def _operator_task_ids(tree: ast.AST) -> set[str]:
    task_ids: set[str] = set()
    for call in ast.walk(tree):
        if not isinstance(call, ast.Call):
            continue
        task_id_node = _keyword(call, "task_id")
        task_id = _constant_value(task_id_node)
        if isinstance(task_id, str):
            task_ids.add(task_id)
    return task_ids


def _compact_source(source: str) -> str:
    return "".join(source.split())


def _assert_shift_chain(source: str, chain: list[str]) -> None:
    compact = "".join(source.split())
    assert ">>".join(chain) in compact


def test_news_crawling_dag_wires_collect_crawl_and_hourly_trigger() -> None:
    tree = _tree("news_dag.py")
    source = _source("news_dag.py")

    assert _task_decorator_ids(tree) >= {
        "prepare_ingestion_context",
        "collect_naver_news",
        "news_crawler",
    }
    assert _operator_task_ids(tree) >= {"trigger_news_ingestion_integration_hourly"}
    _assert_shift_chain(source, ["collect_task", "crawl_task", "trigger_hourly_integration"])

    trigger = _operator_call(tree, "TriggerDagRunOperator", "trigger_news_ingestion_integration_hourly")
    assert _constant_value(_keyword(trigger, "trigger_dag_id")) == "news_ingestion_integration_hourly"
    assert _dict_keys(_keyword(trigger, "conf")) == {"window_start", "window_end"}
    assert "run_collect_naver_news" in source
    assert "run_news_crawler" in source
    assert "filter_domain_list_v1.00.txt" in source


def test_hourly_ingestion_dag_keeps_incremental_pipeline_order_and_trigger_conf() -> None:
    tree = _tree("news_ingestion_integration_hourly_dag.py")
    source = _source("news_ingestion_integration_hourly_dag.py")

    assert _task_decorator_ids(tree) >= {
        "prepare_context",
        "prepare_db_context",
        "get_stock_mapping",
        "bronze_ingest_window",
        "resolve_target_news",
        "refinement_incremental",
        "gemini_incremental",
        "post_gemini_filter_incremental",
        "load_incremental_to_db",
        "report_window_status",
        "build_recommendation_metrics_conf",
    }
    _assert_shift_chain(
        source,
        [
            "status_before",
            "bronze",
            "targets",
            "refined",
            "analyzed",
            "filtered",
            "loaded",
            "status_after",
            "metrics_conf",
            "trigger_recommendation_metrics",
        ],
    )

    trigger = _operator_call(
        tree,
        "TriggerDagRunOperator",
        "trigger_recommendation_news_path_metrics_hourly",
    )
    assert _constant_value(_keyword(trigger, "trigger_dag_id")) == "recommendation_news_path_metrics_hourly"
    assert _dict_keys(_keyword(trigger, "conf")) == {"window_start", "window_end"}
    assert "read_news_by_time_window" in source
    assert "write_news_window_to_minio" in source
    assert "run_incremental_db_loading" in source


def test_daily_finalize_dag_keeps_daily_merge_snapshot_and_report_dependencies() -> None:
    tree = _tree("news_analysis_daily_finalize_dag.py")
    source = _source("news_analysis_daily_finalize_dag.py")
    compact = _compact_source(source)

    assert _task_decorator_ids(tree) >= {
        "prepare_context",
        "collect_incremental_paths",
        "merge_refined_daily",
        "merge_analysis_daily",
        "build_keyword_snapshot",
        "finalize_report",
    }
    assert "collected>>refined" in compact
    assert "collected>>merged" in compact
    assert "merged>>snapshot>>report" in compact
    assert "refined>>report" in compact
    assert "run_embedding_update_service" in source
    assert "refined_news_incremental/window_start=" in source
    assert "extracted_keywords_incremental/window_start=" in source
    assert "extracted_stocks_incremental/window_start=" in source
    assert "analyzed_news_incremental/window_start=" in source


def test_recommendation_metrics_dag_wires_sql_snapshots_and_bandit_update() -> None:
    tree = _tree("recommendation_news_path_metrics_hourly_dag.py")
    source = _source("recommendation_news_path_metrics_hourly_dag.py")

    assert _operator_task_ids(tree) >= {
        "aggregate_recommendation_news_path_metrics_hourly",
        "build_recommendation_path_c_snapshot",
        "build_recommendation_path_a2_snapshot",
    }
    assert _task_decorator_ids(tree) >= {"update_bandit_posteriors"}
    _assert_shift_chain(
        source,
        [
            "aggregate_hourly_metrics",
            "build_path_c_snapshot",
            "build_path_a2_snapshot",
            "update_bandit_posteriors_task",
        ],
    )

    aggregate = _operator_call(
        tree,
        "SQLExecuteQueryOperator",
        "aggregate_recommendation_news_path_metrics_hourly",
    )
    path_c = _operator_call(tree, "SQLExecuteQueryOperator", "build_recommendation_path_c_snapshot")
    path_a2 = _operator_call(tree, "SQLExecuteQueryOperator", "build_recommendation_path_a2_snapshot")

    assert _constant_value(_keyword(aggregate, "conn_id")) == "news_data_db"
    assert _constant_value(_keyword(path_c, "conn_id")) == "news_data_db"
    assert _constant_value(_keyword(path_a2, "conn_id")) == "news_data_db"
    assert _constant_value(_keyword(aggregate, "sql")) == "sql/aggregate_recommendation_news_path_metrics_hourly.sql"
    assert _constant_value(_keyword(path_c, "sql")) == "sql/build_recommendation_path_c_snapshot.sql"
    assert _constant_value(_keyword(path_a2, "sql")) == "sql/build_recommendation_path_a2_snapshot.sql"
    assert "update_bandit_posteriors" in source
    assert "RECO_BANDIT_LOOKBACK_HOURS" in source
    assert "RECO_VALID_READ_DWELL_SECONDS" in source


def test_gnn_trainset_creation_dag_wires_context_to_graph_builder_container() -> None:
    tree = _tree("gnn_trainset_creation_dag.py")
    source = _source("gnn_trainset_creation_dag.py")

    assert _task_decorator_ids(tree) >= {"prepare_context"}
    _assert_shift_chain(source, ["ctx", "build_graph_task"])

    build_graph = _operator_call(tree, "DockerOperator", "build_graph_in_docker")
    assert _dict_keys(_keyword(build_graph, "environment")) >= {
        "TARGET_DATE",
        "PATHS_JSON",
        "AWS_INFO_JSON",
        "DB_INFO_JSON",
    }
    assert _constant_value(_keyword(build_graph, "network_mode")) == "DOCKER_NETWORK"
    assert "run_graph_building" in source
    assert "select_latest_snapshot_on_or_before" in source


def test_gnn_training_dag_wires_train_evaluate_gate_and_deploy_trigger() -> None:
    tree = _tree("gnn_training_dag.py")
    source = _source("gnn_training_dag.py")

    assert _task_decorator_ids(tree) >= {"prepare_config", "check_candidate_tagged"}
    _assert_shift_chain(
        source,
        ["config_data", "train_task", "evaluate_task", "check_gate", "trigger_graph2db"],
    )

    train = _operator_call(tree, "DockerOperator", "train_gnn_model")
    assert _dict_keys(_keyword(train, "environment")) >= {
        "TRAINSET_PATH",
        "OUTPUT_PATH",
        "AWS_INFO_JSON",
        "CONFIG_JSON",
        "STAGE_DIR_IN_CONTAINER",
        "GATE_THRESHOLD",
        "MLFLOW_TRACKING_URI",
    }

    evaluate = _operator_call(tree, "DockerOperator", "evaluate_gnn_model")
    assert _dict_keys(_keyword(evaluate, "environment")) >= {
        "STAGE_DIR_IN_CONTAINER",
        "AWS_INFO_JSON",
        "MLFLOW_TRACKING_URI",
    }

    trigger = _operator_call(tree, "TriggerDagRunOperator", "trigger_graph2db")
    assert _constant_value(_keyword(trigger, "trigger_dag_id")) == "graph_to_db"
    assert _dict_keys(_keyword(trigger, "conf")) == {"model_status", "candidate_version"}
    assert "run_training_stage" in source
    assert "run_evaluation_stage" in source
    assert "tags.gate_passed = 'true'" in source

def test_graph_to_db_dag_wires_serving_config_to_deploy_container() -> None:
    tree = _tree("graph2db_dag.py")
    source = _source("graph2db_dag.py")

    assert _task_decorator_ids(tree) >= {"prepare_serving_config"}
    _assert_shift_chain(source, ["config", "deploy_task"])

    deploy = _operator_call(tree, "DockerOperator", "deploy_to_db")
    assert _dict_keys(_keyword(deploy, "environment")) >= {
        "ARTIFACT_INFO",
        "AWS_INFO",
        "DB_INFO",
        "MLFLOW_TRACKING_URI",
    }
    assert "run_deploy" in source
    assert "host.docker.internal" in source
