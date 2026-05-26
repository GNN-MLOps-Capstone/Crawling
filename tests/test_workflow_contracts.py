from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"


def _source(relative_path: str) -> str:
    return (DAGS_DIR / relative_path).read_text(encoding="utf-8")


def _tree(relative_path: str) -> ast.Module:
    return ast.parse(_source(relative_path), filename=relative_path)


def _constant_value(node: ast.AST | None) -> object:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return node.id
    return None


def _keyword(call: ast.Call, name: str) -> ast.AST | None:
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
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
        if _constant_value(_keyword(call, "task_id")) == task_id:
            return call
    raise AssertionError(f"{operator_name} task_id={task_id!r} not found")


def _function_def(tree: ast.AST, name: str) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found")


def _return_dict_key_sets(function: ast.FunctionDef) -> list[set[str]]:
    key_sets: list[set[str]] = []
    for node in ast.walk(function):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            key_sets.append(_dict_keys(node.value))
    return key_sets


def _all_return_keys(function: ast.FunctionDef) -> set[str]:
    keys: set[str] = set()
    for key_set in _return_dict_key_sets(function):
        keys.update(key_set)
    return keys


def _assigned_dict_keys(function: ast.FunctionDef, variable_name: str) -> set[str]:
    for node in ast.walk(function):
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == variable_name for target in node.targets):
            return _dict_keys(node.value)
    return set()


def _dag_param_keys(tree: ast.AST, dag_id: str) -> set[str]:
    for call in _calls_by_name(tree, "DAG") + _calls_by_name(tree, "dag"):
        if _constant_value(_keyword(call, "dag_id")) == dag_id:
            return _dict_keys(_keyword(call, "params"))
    raise AssertionError(f"dag {dag_id!r} not found")


def test_crawling_workflow_trigger_contract_matches_hourly_ingestion_params() -> None:
    news_tree = _tree("news_dag.py")
    news_source = _source("news_dag.py")
    ingestion_tree = _tree("news_ingestion_integration_hourly_dag.py")

    prepare_context = _function_def(news_tree, "prepare_ingestion_context")
    assert _all_return_keys(prepare_context) >= {"db", "api", "filter_file_path"}

    trigger = _operator_call(news_tree, "TriggerDagRunOperator", "trigger_news_ingestion_integration_hourly")
    trigger_conf_keys = _dict_keys(_keyword(trigger, "conf"))
    downstream_param_keys = _dag_param_keys(ingestion_tree, "news_ingestion_integration_hourly")

    assert trigger_conf_keys == {"window_start", "window_end"}
    assert trigger_conf_keys <= downstream_param_keys
    assert _constant_value(_keyword(trigger, "trigger_dag_id")) == "news_ingestion_integration_hourly"
    assert "data_interval_start.in_timezone('Asia/Seoul')" in news_source
    assert "data_interval_end.in_timezone('Asia/Seoul')" in news_source
    assert "db_config=context[\"db\"]" in news_source
    assert "api_config=context[\"api\"]" in news_source
    assert "filter_file_path=context[\"filter_file_path\"]" in news_source
    assert "crawler_version='0.02'" in news_source


def test_ingestion_workflow_artifact_contracts_flow_to_metrics_trigger() -> None:
    tree = _tree("news_ingestion_integration_hourly_dag.py")
    source = _source("news_ingestion_integration_hourly_dag.py")

    bronze_keys = _all_return_keys(_function_def(tree, "bronze_ingest_window"))
    target_keys = _all_return_keys(_function_def(tree, "resolve_target_news"))
    refinement_keys = _all_return_keys(_function_def(tree, "refinement_incremental"))
    gemini_keys = _all_return_keys(_function_def(tree, "gemini_incremental"))
    metrics_conf_keys = _all_return_keys(_function_def(tree, "build_recommendation_metrics_conf"))

    assert bronze_keys >= {"window_start", "window_end", "window_label", "bronze_keys", "news_ids"}
    assert target_keys >= {"window_start", "window_end", "window_label", "news_ids"}
    assert refinement_keys >= {"window_label", "refined", "news_ids"}
    assert gemini_keys >= {"window_label", "refined", "stocks", "keywords", "analysis"}
    assert metrics_conf_keys == {"window_start", "window_end"}
    assert _assigned_dict_keys(_function_def(tree, "gemini_incremental"), "output_keys") == {
        "stocks",
        "keywords",
        "analysis",
    }

    trigger = _operator_call(
        tree,
        "TriggerDagRunOperator",
        "trigger_recommendation_news_path_metrics_hourly",
    )
    assert _dict_keys(_keyword(trigger, "conf")) == {"window_start", "window_end"}
    assert _constant_value(_keyword(trigger, "trigger_dag_id")) == "recommendation_news_path_metrics_hourly"
    assert "if not bronze_keys or not target_news_ids" in source
    assert "if not refined_key" in source
    assert "if not incremental_paths.get(\"refined\")" in source
    assert "run_incremental_db_loading" in source


def test_daily_finalize_workflow_preserves_incremental_to_daily_output_contract() -> None:
    tree = _tree("news_analysis_daily_finalize_dag.py")
    source = _source("news_analysis_daily_finalize_dag.py")

    collect = _function_def(tree, "collect_incremental_paths")
    merge_analysis = _function_def(tree, "merge_analysis_daily")
    finalize = _function_def(tree, "finalize_report")

    assert _assigned_dict_keys(collect, "prefixes") == {"refined", "keywords", "stocks", "analysis"}
    assert _assigned_dict_keys(merge_analysis, "merge_plan") == {"keywords", "stocks", "analysis"}
    assert _assigned_dict_keys(finalize, "report") == {
        "target_date",
        "refined",
        "keywords",
        "stocks",
        "analysis",
        "keyword_snapshot",
    }
    assert "refined_news/year={y}/month={m}/day={d}/data.parquet" in source
    assert "extracted_keywords/year={y}/month={m}/day={d}/data.parquet" in source
    assert "extracted_stocks/year={y}/month={m}/day={d}/data.parquet" in source
    assert "analyzed_news/year={y}/month={m}/day={d}/data.parquet" in source
    assert "return results[-1] if results else None" in source


def test_training_workflow_path_candidate_and_deploy_contracts_align() -> None:
    trainset_tree = _tree("gnn_trainset_creation_dag.py")
    trainset_source = _source("gnn_trainset_creation_dag.py")
    training_tree = _tree("gnn_training_dag.py")
    training_source = _source("gnn_training_dag.py")
    graph2db_tree = _tree("graph2db_dag.py")

    trainset_context = _function_def(trainset_tree, "prepare_context")
    training_config = _function_def(training_tree, "prepare_config")
    serving_config = _function_def(graph2db_tree, "prepare_serving_config")

    assert _all_return_keys(trainset_context) >= {"target_date", "aws", "db", "paths"}
    assert _dict_keys(next(node.value for node in ast.walk(trainset_context) if isinstance(node, ast.Return))) >= {
        "target_date",
        "aws",
        "db",
        "paths",
    }
    assert _all_return_keys(training_config) >= {
        "trainset_path",
        "output_path",
        "candidate_version",
        "stage_subdir",
        "aws",
        "config",
    }
    assert _all_return_keys(serving_config) == {"artifact", "aws", "db"}
    assert "trainset/date={date_nodash}/hetero_graph.pt" in trainset_source
    assert "trainset/date={date_nodash}/hetero_graph.pt" in training_source
    assert "candidate_version\": f\"v_{date_nodash}\"" in training_source
    assert "stage_subdir\": f\"date={date_nodash}\"" in training_source

    trigger = _operator_call(training_tree, "TriggerDagRunOperator", "trigger_graph2db")
    assert _constant_value(_keyword(trigger, "trigger_dag_id")) == "graph_to_db"
    assert _dict_keys(_keyword(trigger, "conf")) == {"model_status", "candidate_version"}
    assert "model_status\": \"candidate\"" in training_source
    assert _dict_keys(_keyword(_operator_call(graph2db_tree, "DockerOperator", "deploy_to_db"), "environment")) >= {
        "ARTIFACT_INFO",
        "AWS_INFO",
        "DB_INFO",
        "MLFLOW_TRACKING_URI",
    }


def test_metric_aggregation_workflow_consumes_trigger_window_and_updates_bandit_state() -> None:
    tree = _tree("recommendation_news_path_metrics_hourly_dag.py")
    source = _source("recommendation_news_path_metrics_hourly_dag.py")

    assert _dag_param_keys(tree, "recommendation_news_path_metrics_hourly") == {"window_start", "window_end"}
    assert _constant_value(
        _keyword(
            _operator_call(tree, "SQLExecuteQueryOperator", "aggregate_recommendation_news_path_metrics_hourly"),
            "sql",
        )
    ) == "sql/aggregate_recommendation_news_path_metrics_hourly.sql"
    assert _constant_value(
        _keyword(_operator_call(tree, "SQLExecuteQueryOperator", "build_recommendation_path_c_snapshot"), "sql")
    ) == "sql/build_recommendation_path_c_snapshot.sql"
    assert _constant_value(
        _keyword(_operator_call(tree, "SQLExecuteQueryOperator", "build_recommendation_path_a2_snapshot"), "sql")
    ) == "sql/build_recommendation_path_a2_snapshot.sql"
    assert "dag_conf.get(\"window_end\") or data_interval_end.to_iso8601_string()" in source
    assert "postgres_conn_id=\"news_data_db\"" in source
    assert "lookback_hours=int(os.getenv(\"RECO_BANDIT_LOOKBACK_HOURS\", \"72\"))" in source
    assert "dwell_threshold_seconds=int(os.getenv(\"RECO_VALID_READ_DWELL_SECONDS\", \"5\"))" in source
