# Workflow Test Plan

이 문서는 crawling, ingestion, training, metric aggregation workflow의 테스트 목적과 범위를 정리한다.
여기서 workflow 테스트는 Airflow DAG 전체를 실행하는 E2E 테스트가 아니라, task와 DAG 사이에 흐르는 입력/산출물 계약을 검증하는 테스트다.

## 목적

- upstream task의 return dict key가 downstream 입력과 맞는지 검증한다.
- DAG 간 `TriggerDagRunOperator.conf`와 downstream DAG `params`/context 해석이 맞는지 검증한다.
- S3 object key, daily parquet path, trainset/model path, candidate version 같은 산출물 규칙을 고정한다.
- empty/no-op case에서 downstream이 기대한 fallback 값을 받는지 검증한다.
- DockerOperator environment와 컨테이너 내부 Python 코드가 기대하는 이름이 일치하는지 검증한다.

## 범위

### Crawling Workflow

대상 DAG:

- `dags/news_dag.py`

검증 계약:

- `prepare_ingestion_context`가 `db`, `api`, `filter_file_path`를 반환한다.
- `collect_naver_news`가 `context["db"]`, `context["api"]`를 사용한다.
- `news_crawler`가 `context["db"]`, `context["filter_file_path"]`를 사용한다.
- `trigger_news_ingestion_integration_hourly`가 `window_start`, `window_end`를 넘긴다.
- downstream `news_ingestion_integration_hourly`가 같은 이름의 params를 가진다.

### Ingestion / Analysis Workflow

대상 DAG:

- `dags/news_ingestion_integration_hourly_dag.py`
- `dags/news_analysis_daily_finalize_dag.py`

검증 계약:

- hourly context가 `window_start`, `window_end`, `window_label`, `aws`를 만든다.
- bronze result는 `bronze_keys`, `news_ids`, window metadata를 가진다.
- target resolve result는 `news_ids`와 window metadata를 가진다.
- refinement result는 `refined`, `news_ids`, `window_label`을 가진다.
- refined가 없으면 Gemini stage는 `stocks`, `keywords`, `analysis`를 `None`으로 반환한다.
- DB loading은 `refined`가 없으면 no-op 한다.
- metrics trigger conf는 UTC `window_start`, `window_end`를 넘긴다.
- daily finalize는 incremental paths를 daily parquet path로 합치고 report에 모든 output key를 포함한다.

### Training Workflow

대상 DAG:

- `dags/gnn_trainset_creation_dag.py`
- `dags/gnn_training_dag.py`
- `dags/graph2db_dag.py`

검증 계약:

- trainset creation output path는 `trainset/date=YYYYMMDD/hetero_graph.pt` 규칙을 따른다.
- GNN training input path도 같은 날짜 규칙을 사용한다.
- `candidate_version`과 `stage_subdir`는 `v_YYYYMMDD`, `date=YYYYMMDD` 규칙을 따른다.
- train/evaluate Docker task가 같은 stage dir 계약을 공유한다.
- gate 통과 후 `graph_to_db` trigger conf는 `model_status=candidate`, `candidate_version`을 넘긴다.
- `graph_to_db` deploy task는 `artifact`, `aws`, `db` 구조를 컨테이너 환경 변수로 넘긴다.

### Metric Aggregation Workflow

대상 DAG:

- `dags/recommendation_news_path_metrics_hourly_dag.py`

검증 계약:

- upstream trigger conf는 `window_start`, `window_end`를 넘긴다.
- hourly metrics SQL, Path C snapshot SQL, Path A2 snapshot SQL, bandit posterior update 순서를 유지한다.
- SQL task는 모두 `news_data_db` connection을 사용한다.
- bandit posterior update는 `window_end`, `lookback_hours`, `dwell_threshold_seconds` 계약을 가진다.

## 실행

workflow contract 테스트는 DAG 소스를 정적으로 읽기 때문에 Airflow runtime을 요구하지 않는다.
별도 테스트 서비스에서 실행한다.

```bash
docker compose run --rm workflow-contract-test
```

최근 확인 결과:

```text
5 passed
```

## 한계

- 이 테스트는 Airflow scheduler, task instance, XCom 저장소를 실제로 실행하지 않는다.
- 실제 DB, S3, MLflow, Redis 산출물 내용은 모듈 테스트나 별도 runtime smoke 테스트에서 검증한다.
- Jinja template rendering 자체는 실행하지 않고, template key와 downstream 계약만 검증한다.
