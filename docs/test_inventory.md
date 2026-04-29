# Test Inventory

이 문서는 현재 `tests/` 아래 테스트 파일과 각 테스트가 검증하는 계약을 정리한다.
pytest의 `collected items`는 테스트 파일 개수가 아니라 `def test_*` 함수와 parameterized case 단위다.

## 요약

- 테스트 파일: 7개
- 테스트 함수: 41개
- pytest item 기준 예상 수: 43개
  - `tests/test_gnn_trainer_gate.py`의 parameterized 테스트 1개가 3개 item으로 수집된다.
- `tests/test_recommend_api.py`만 실행하면 현재 28개 item이 수집된다.

## 실행 환경

현재 공식 로컬 검증은 두 컨테이너 경로로 나눈다.

- 추천 API 변경: `recommend-api` 컨테이너에서 API 테스트 실행
- DAG module / GNN 변경: `gnn-worker` 기반 `gnn-test` 서비스에서 module/GNN 테스트 실행

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
  tests/test_gnn_serving_deploy.py
```

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
15 passed, 2 warnings
```

warning 2개는 테스트 실패가 아니다.

- `torch_geometric.distributed` deprecation warning: PyG 내부 import 과정에서 발생한다.
- `graph_builder.py` tensor 생성 warning: `numpy.ndarray` list를 바로 tensor로 만드는 성능 경고다.
