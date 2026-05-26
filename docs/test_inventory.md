# Test Inventory

이 문서는 현재 `tests/` 아래 테스트 파일과 각 테스트가 검증하는 계약을 정리한다.
pytest의 `collected items`는 테스트 파일 개수가 아니라 `def test_*` 함수와 parameterized case 단위다.
전체 테스트 레이어와 실행 시점 요약은 `docs/testing_overview.md`를 먼저 본다.

## 요약

- 테스트 파일: 13개
- 테스트 함수: 63개
- pytest item 기준 예상 수: 65개
  - `tests/test_gnn_trainer_gate.py`의 parameterized 테스트 1개가 3개 item으로 수집된다.
- `tests/test_recommend_api.py`만 실행하면 현재 28개 item이 수집된다.

## 실행 환경

현재 공식 로컬 검증은 컨테이너 경로와 정적 DAG wiring 검증으로 나눈다.

- 추천 API 변경: `recommend-api` 컨테이너에서 API 테스트 실행
- DAG module / GNN 변경: `gnn-worker` 기반 `gnn-test` 서비스에서 module/GNN 테스트 실행
- DAG task wiring 변경: `dag-wiring-test` 서비스에서 정적 wiring 테스트 실행
- Workflow contract 변경: `workflow-contract-test` 서비스에서 정적 workflow 계약 테스트 실행
- Offline artifact 변경: `offline-artifact-test` 서비스에서 운영 리소스 미접속 산출물 테스트 실행
- Live read-only 변경: `live-readonly-test` 서비스에서 opt-in 운영/스테이징 DB read-only 테스트 실행

`airflow-*` 컨테이너는 DAG 운영 런타임이고, `jupyter` 컨테이너는 노트북/실험 작업 공간이다.
둘 다 현재 테스트 러너로 사용하지 않는다.
CI 검증은 아직 공식 운영 범위에 두지 않고, 로컬 Docker 검증 결과를 기준으로 삼는다.

### 추천 API 테스트

추천 API 컨테이너에는 `app/`과 `tests/`가 포함되어 있어 추천 API 테스트 실행에 적합하다.

```bash
docker compose exec -T recommend-api python -m pytest tests/test_recommend_api.py
```

주의: `recommend-api` 컨테이너는 테스트 파일을 이미지에 복사하는 구조다. 호스트의 `tests/` 변경을 실행 중 컨테이너가 즉시 보지 못할 수 있다. 이 경우 이미지를 다시 빌드하거나, 빠른 확인용으로 수정 파일을 컨테이너에 복사한 뒤 실행한다.

```bash
docker compose cp tests/test_recommend_api.py recommend-api:/app/tests/test_recommend_api.py
docker compose exec -T recommend-api python -m pytest tests/test_recommend_api.py
```

### DAG Module / GNN 테스트

DAG module과 GNN 테스트는 `gnn-worker` 런타임에 가까운 `gnn-test` 서비스에서 실행한다.
`gnn-test`는 `dockerfile/gnn.Dockerfile`의 `gnn-test` target을 사용하며, 테스트에 필요한 `app/`, `dags/`, `tests/`, `pyproject.toml`만 `/app` 아래에 마운트한다.
저장소 루트를 통째로 마운트하지 않는 이유는 루트의 `mlflow/` 디렉터리가 Python 패키지 `mlflow` import를 가릴 수 있기 때문이다.

```bash
docker compose run --rm gnn-test
```

필요하면 대상 파일을 직접 지정한다.

```bash
docker compose run --rm gnn-test python -m pytest \
  tests/test_hourly_pipeline_utils.py \
  tests/test_bandit_posterior_utils.py \
  tests/test_gnn_graph_builder.py \
  tests/test_gnn_training_data_loader.py \
  tests/test_gnn_trainer_gate.py \
  tests/test_gnn_serving_deploy.py \
  tests/test_gnn_baseline_eval.py
```

### DAG Task Wiring 테스트

`tests/test_airflow_task_wiring.py`는 Airflow 런타임 실행 테스트가 아니라 DAG 소스를 AST로 읽는 정적 wiring 테스트다.
GNN 런타임 검증이 아니므로 `gnn-test` 기본 묶음에는 포함하지 않는다.

pytest가 있는 환경에서 단독 실행한다.

```bash
docker compose run --rm dag-wiring-test
```

### Workflow Contract 테스트

`tests/test_workflow_contracts.py`는 crawling, ingestion, training, metric aggregation workflow의 입력/산출물 계약을 정적으로 검증한다.
Airflow runtime E2E가 아니라 DAG 간 trigger conf, task return key, S3/DB path 규칙, Docker environment 계약을 고정한다.

```bash
docker compose run --rm workflow-contract-test
```

### Offline Artifact 테스트 계획

운영 DB/S3/Redis/MLflow에 붙지 않고 DB row, parquet file, model artifact, payload 산출물 계약을 검증하는 계획은 `docs/offline_artifact_test_plan.md`에 정리한다.
현재 구현된 offline artifact 테스트는 별도 서비스로 실행한다.

```bash
docker compose run --rm offline-artifact-test
```

### Live Read-Only 테스트 계획

운영 또는 스테이징 DB를 read-only로 점검하는 live test 설계는 `docs/live_readonly_test_plan.md`에 정리한다.
기본 테스트 명령에는 포함하지 않고, 구현 시에도 `ALLOW_LIVE_READONLY_TESTS=1` 같은 명시적 opt-in과 별도 marker/service로 분리한다.

```bash
ALLOW_LIVE_READONLY_TESTS=1 docker compose run --rm live-readonly-test
```

opt-in 환경변수가 없으면 live DB에 연결하지 않고 skip된다.

### Operational Health Check DAG

정기 운영 상태 점검용 DAG는 `docs/operational_health_check_dag.md`에 정리한다.
`operational_health_check_daily`는 매일 KST 04:30 실행되며, schema/news/recommendation/embedding 상태를 read-only로 확인한다.

## 테스트 파일별 목적

### `tests/test_recommend_api.py`

추천 API의 요청 검증, cursor pagination, multipath 추천, bandit allocator, impression logging을 검증한다.

- `test_request_validation_limit_required`
  - `limit` 누락 요청이 422로 거부되는지 검증한다.
- `test_cursor_limit_mismatch_returns_400`
  - cursor에 기록된 limit과 요청 limit이 다르면 400 `INVALID_CURSOR`가 반환되는지 검증한다.
- `test_pagination_consistency_with_cursor`
  - cursor 기반 다음 페이지가 이전 페이지와 중복 없이 이어지는지 검증한다.
- `test_cursor_pagination_keeps_paths_with_redis_session_cache`
  - Redis session cache를 사용할 때 cursor 페이지의 path 정보가 유지되는지 검증한다.
- `test_response_contains_multipath_meta_fields`
  - 추천 응답 meta에 multipath source, fallback 여부, fallback reason이 포함되는지 검증한다.
- `test_response_items_include_public_path_codes`
  - 내부 path가 API 응답용 public code인 `A1`, `A2`, `B`, `C`, `LATEST` 형태로 노출되는지 검증한다.
- `test_profile_missing_fallback_reason_when_internal_signals_are_empty`
  - 온보딩/행동 신호가 없을 때 fallback reason이 `profile_missing`으로 잡히는지 검증한다.
- `test_cold_user_without_onboarding_does_not_emit_a1_path`
  - 온보딩 신호가 없는 cold user에게 `A1` path가 나오지 않는지 검증한다.
- `test_primary_failure_falls_back_to_breaking`
  - primary path 조회 실패 시 breaking path로 fallback되는지 검증한다.
- `test_warm_user_without_onboarding_emits_behavior_but_not_a1`
  - 최근 행동은 있지만 온보딩 신호가 없는 warm user에게 behavior는 가능하고 `A1`은 나오지 않는지 검증한다.
- `test_single_recent_action_is_not_enough_for_warm_behavior_path`
  - 최근 행동 1개만으로는 warm behavior path 조건을 만족하지 않는지 검증한다.
- `test_cold_user_replenishes_breaking_pool_when_enabled`
  - breaking 보충 설정이 켜졌을 때 cold user 응답에 `B` path가 포함될 수 있는지 검증한다.
- `test_context_override_still_works_for_debug_requests`
  - 요청 context override가 디버그 요청에서 여전히 추천 컨텍스트로 반영되는지 검증한다.
- `test_context_schema_rejects_unknown_fields`
  - context schema에 정의되지 않은 필드가 422로 거부되는지 검증한다.
- `test_onboarding_personalized_scoring_reorders_candidates`
  - 온보딩 profile entity와 후보 entity embedding 유사도에 따라 후보 순서가 재정렬되는지 검증한다.
- `test_behavior_personalized_scoring_uses_recent_action_entities`
  - 최근 행동 entity를 사용한 behavior scoring이 관련 후보를 우선 배치하는지 검증한다.
- `test_primary_uses_single_base_pool`
  - primary 추천 후보 선별 시 72시간 base pool을 한 번만 조회해 공유하는지 검증한다.
- `test_replayed_cursor_returns_same_page`
  - 같은 cursor를 재호출하면 같은 page와 next cursor가 반환되는지 검증한다.
- `test_prefetch_rolls_over_only_after_current_batch_is_consumed`
  - prefetch 결과가 현재 batch 소비 전에는 timeline에 섞이지 않는지 검증한다.
- `test_stale_session_is_restored_for_cursor_pagination`
  - stale session 상태에서도 cursor pagination이 복구되고 fallback reason이 기록되는지 검증한다.
- `test_bandit_guardrail_promotes_breaking_into_first_window`
  - 첫 페이지 window에 최소 breaking 후보가 들어오도록 guardrail이 동작하는지 검증한다.
- `test_bandit_service_fixed_allocator_matches_legacy_mix_order`
  - fixed allocator가 기존 mix weight 순서대로 timeline을 구성하는지 검증한다.
- `test_bandit_service_thompson_uses_global_plus_user_posteriors`
  - Thompson allocator가 global posterior와 user posterior를 함께 반영하는지 검증한다.
- `test_bandit_service_thompson_applies_global_posterior_weight`
  - global posterior weight 설정이 arm score에 반영되는지 검증한다.
- `test_bandit_service_thompson_applies_minimum_per_path_guardrail`
  - Thompson allocator에서 path별 최소 노출 guardrail이 적용되는지 검증한다.
- `test_bandit_service_thompson_falls_back_to_fixed_on_state_error`
  - bandit state 조회 오류가 fixed allocator fallback으로 전환되는지 검증한다.
- `test_popular_candidates_are_served_as_path_c`
  - popular 후보가 public path `C`로 응답되는지 검증한다.
- `test_recommendation_response_emits_impression_logs`
  - 추천 응답 생성 시 노출 로그가 item별로 기록되는지 검증한다.

### `tests/test_hourly_pipeline_utils.py`

시간 window와 snapshot key 선택 유틸을 검증한다.

- `test_select_latest_snapshot_on_or_before_filters_future_snapshot`
  - 기준일 이후 snapshot을 제외하고 기준일 이하 최신 snapshot을 선택하는지 검증한다.
- `test_select_latest_snapshot_on_or_before_returns_none_when_no_match`
  - 기준일 이하 snapshot이 없으면 `None`을 반환하는지 검증한다.
- `test_build_incremental_key_uses_window_partition_format`
  - incremental parquet key가 window partition 형식으로 생성되는지 검증한다.
- `test_build_window_context_returns_stable_label`
  - window start/end와 label이 안정적인 포맷으로 생성되는지 검증한다.

### `tests/test_bandit_posterior_utils.py`

추천 bandit posterior 상태 key, posterior 변환, Redis payload 생성을 검증한다.

- `test_build_bandit_state_key_formats_global_and_user_keys`
  - global/user posterior key가 기대 형식으로 생성되는지 검증한다.
- `test_rows_to_global_posteriors_adds_prior_to_alpha_beta`
  - global 집계 row가 path별 posterior record로 변환되는지 검증한다.
- `test_rows_to_user_posteriors_uses_reward_and_failure_counts`
  - user별 reward/impression 집계가 alpha/beta posterior로 변환되는지 검증한다.
- `test_build_bandit_state_payload_includes_metadata`
  - posterior payload에 alpha, beta, path 등 metadata가 포함되는지 검증한다.

### `tests/test_gnn_graph_builder.py`

GNN 학습셋 graph 생성과 node mapping 생성을 검증한다.

- `test_run_graph_building_creates_filtered_graph_and_mapping`
  - cutoff date 이후 뉴스가 제외되고, `HeteroData`와 `node_mapping.pkl`이 기대 구조로 생성되는지 검증한다.

### `tests/test_gnn_training_data_loader.py`

GNN 학습 데이터 로딩, temporal split, serving mapping 보존을 검증한다.

- `test_temporal_link_split_uses_train_only_message_passing`
  - 날짜 기준 train/val/test split과 `train_only` message passing edge 구성이 맞는지 검증한다.
- `test_load_data_from_s3_preserves_filtered_serving_mapping`
  - 저빈도 노드 필터링 후에도 serving용 id mapping과 meta가 필터링 결과에 맞게 보존되는지 검증한다.

### `tests/test_gnn_trainer_gate.py`

GNN evaluation stage의 candidate 승격 gate를 검증한다.

- `test_run_evaluation_stage_applies_candidate_gate`
  - parameterized 3개 case로 실행된다.
  - final score가 threshold 이상이고 missing metric이 0이면 `status=candidate`, `gate_passed=true`가 기록되는지 검증한다.
  - score 미달 또는 missing 발생 시 candidate 승격이 차단되는지 검증한다.

### `tests/test_gnn_serving_deploy.py`

GNN embedding serving deploy 절차를 검증한다.

- `test_run_deploy_inserts_embeddings_and_promotes_model`
  - MLflow artifact의 `node_embeddings.pkl`, `node_mapping.pkl`을 DB insert row로 변환하는지 검증한다.
  - deploy 성공 후 기존 product run은 `legacy`, 새 candidate run은 `product`로 태그 변경되는지 검증한다.

### `tests/test_gnn_baseline_eval.py`

GNN baseline evaluation 유틸의 embedding method 비교 계약을 검증한다.

- `test_embedding_baseline_reports_similarity_separation`
  - positive pair와 random pair의 similarity separation, hit@k metric이 기대대로 계산되는지 검증한다.

### `tests/test_airflow_task_wiring.py`

crawling, ingestion, training 관련 Airflow DAG의 task wiring을 정적으로 검증한다.
Airflow 런타임 실행이 아니라 DAG 소스의 task id, dependency chain, operator/trigger 설정, 컨테이너 환경 변수 연결을 확인한다.

- `test_news_crawling_dag_wires_collect_crawl_and_hourly_trigger`
  - 뉴스 수집 DAG가 Naver 수집, 크롤링, hourly integration trigger 순서로 연결되는지 검증한다.
- `test_hourly_ingestion_dag_keeps_incremental_pipeline_order_and_trigger_conf`
  - hourly ingestion의 bronze, target resolve, refinement, Gemini, filter, DB load, metrics trigger 순서와 trigger conf를 검증한다.
- `test_daily_finalize_dag_keeps_daily_merge_snapshot_and_report_dependencies`
  - daily finalize DAG가 incremental path 수집, refined/analysis daily merge, keyword snapshot, report fan-in 순서를 유지하는지 검증한다.
- `test_recommendation_metrics_dag_wires_sql_snapshots_and_bandit_update`
  - 추천 path metrics DAG가 hourly SQL 집계, path C/A2 snapshot 생성, bandit posterior 갱신 순서를 유지하는지 검증한다.
- `test_gnn_trainset_creation_dag_wires_context_to_graph_builder_container`
  - 학습셋 생성 DAG가 snapshot context를 Docker graph builder task 환경 변수로 전달하는지 검증한다.
- `test_gnn_training_dag_wires_train_evaluate_gate_and_deploy_trigger`
  - GNN training DAG가 train, evaluation, candidate gate, graph2db trigger 순서와 환경 변수를 유지하는지 검증한다.
- `test_graph_to_db_dag_wires_serving_config_to_deploy_container`
  - graph2db DAG가 MLflow artifact, S3, DB 설정을 deploy 컨테이너에 전달하는지 검증한다.

### `tests/test_workflow_contracts.py`

crawling, ingestion, training, metric aggregation workflow의 산출물 계약을 정적으로 검증한다.
상세 설계는 `docs/workflow_test_plan.md`를 기준으로 한다.

- `test_crawling_workflow_trigger_contract_matches_hourly_ingestion_params`
  - `news_dag`가 hourly ingestion DAG에 넘기는 `window_start/window_end` trigger conf와 downstream params가 일치하는지 검증한다.
- `test_ingestion_workflow_artifact_contracts_flow_to_metrics_trigger`
  - hourly ingestion의 bronze, target, refinement, Gemini, DB loading, metrics trigger 산출물 key 계약을 검증한다.
- `test_daily_finalize_workflow_preserves_incremental_to_daily_output_contract`
  - daily finalize가 incremental prefix를 daily parquet output과 report key로 변환하는 계약을 검증한다.
- `test_training_workflow_path_candidate_and_deploy_contracts_align`
  - trainset creation, training, graph2db 사이의 path, candidate version, deploy env 계약을 검증한다.
- `test_metric_aggregation_workflow_consumes_trigger_window_and_updates_bandit_state`
  - recommendation metrics DAG가 trigger window, SQL path, bandit posterior 입력 계약을 유지하는지 검증한다.

### `tests/test_offline_file_artifacts.py`

운영 S3에 접속하지 않고 parquet/file 산출물 계약을 검증한다.

- `test_bronze_window_writer_creates_incremental_parquet_with_expected_schema`
  - bronze incremental writer가 window partition key에 parquet를 저장하고 downstream 필수 컬럼을 유지하는지 검증한다.
- `test_post_gemini_filter_rewrites_incremental_parquets_and_captures_filtered_ids`
  - post-Gemini filter가 refined/stocks/keywords/analysis incremental parquet를 같은 key에 재저장하고 filtered out id를 산출하는지 검증한다.

### `tests/test_offline_db_artifacts.py`

운영 DB에 접속하지 않고 DB row/SQL 산출물 계약을 검증한다.

- `test_news_loader_builds_filtered_news_upsert_rows`
  - refined parquet가 `filtered_news` upsert tuple로 변환되는지 검증한다.
- `test_keyword_loader_builds_master_keyword_upsert_rows`
  - keyword embedding parquet가 `keywords` upsert tuple로 변환되는지 검증한다.
- `test_recommendation_snapshot_sql_files_expose_downstream_contract_columns`
  - recommendation metrics/path snapshot SQL이 downstream이 기대하는 table과 column 계약을 포함하는지 검증한다.

### `tests/live/test_live_readonly_db.py`

운영 또는 스테이징 DB를 read-only로 조회하는 opt-in live test다.
`ALLOW_LIVE_READONLY_TESTS=1` 없이는 DB에 연결하지 않고 skip된다.

- `test_live_schema_exposes_required_tables_and_columns`
  - 운영 DB가 코드가 기대하는 주요 table/column 계약을 제공하는지 검증한다.
- `test_live_news_tables_have_joinable_recent_outputs`
  - news/crawled/filtered 테이블이 join 가능하고 기본 산출물이 존재하는지 검증한다.
- `test_live_recommendation_snapshots_and_bandit_state_have_valid_shape`
  - recommendation metrics, path snapshot, bandit state shape이 유효한지 검증한다.
- `test_live_embedding_serving_table_has_entity_types_and_model_version`
  - serving embedding table에 entity type과 model version이 존재하는지 검증한다.

## 최근 확인 결과

다음 명령으로 추천 API 테스트를 확인했다.

```bash
docker compose exec -T recommend-api python -m pytest tests/test_recommend_api.py
```

결과:

```text
28 passed
```

모듈/GNN 테스트는 다음 명령으로 실행한다.

```bash
docker compose run --rm gnn-test
```

최근 확인 결과:

```text
16 passed, 2 warnings
```

warning 2개는 테스트 실패가 아니다.

- `torch_geometric.distributed` deprecation warning: PyG 내부 import 과정에서 발생한다.
- `graph_builder.py` tensor 생성 warning: `numpy.ndarray` list를 바로 tensor로 만드는 성능 경고다.

DAG task wiring 테스트는 다음 명령으로 단독 확인했다.

```bash
docker compose run --rm dag-wiring-test
```

최근 확인 결과:

```text
7 passed
```

Workflow contract 테스트는 다음 명령으로 단독 실행한다.

```bash
docker compose run --rm workflow-contract-test
```

최근 확인 결과:

```text
5 passed
```

Offline artifact 테스트는 다음 명령으로 단독 실행한다.

```bash
docker compose run --rm offline-artifact-test
```

최근 확인 결과:

```text
5 passed
```

Live read-only 테스트는 opt-in 없이 다음 명령으로 skip 동작을 확인했다.

```bash
docker compose run --rm live-readonly-test
```

최근 확인 결과:

```text
4 skipped
```

Operational health check DAG는 Airflow 컨테이너에서 문법 검증과 DAG 목록 노출을 확인했다.

```bash
docker compose exec -T airflow-scheduler python -m py_compile /opt/airflow/dags/operational_health_check_daily_dag.py
docker compose exec -T airflow-scheduler airflow dags unpause operational_health_check_daily
docker compose exec -T airflow-scheduler airflow dags list
```
