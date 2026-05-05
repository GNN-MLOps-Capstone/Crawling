# Offline Artifact Test Plan

이 문서는 운영 DB, 운영 S3, Redis, MLflow에 접속하지 않고 산출물 계약을 검증하는 테스트 설계다.
목표는 workflow가 남기는 DB row, parquet file, model artifact, Redis/DB payload가 downstream이 소비할 수 있는 형태인지 빠르고 안전하게 확인하는 것이다.

## 원칙

- 운영 리소스에 연결하지 않는다.
- fake object, monkeypatch, tmp path, in-memory payload를 사용한다.
- write 결과는 실제 외부 시스템이 아니라 captured values, tmp parquet, fake S3 storage로 검증한다.
- 테스트는 기본 검증에 넣을 수 있을 만큼 빠르고 deterministic 해야 한다.
- live DB/read-only 운영 검증은 별도 테스트층으로 설계한다.

## 범위

### 1. File / Parquet Artifact

대상:

- `dags/modules/ingestion/writer.py`
- `dags/modules/analysis/news_service.py`
- `dags/modules/analysis/gemini_service.py`
- `dags/modules/analysis/post_gemini_filter.py`
- `dags/news_analysis_daily_finalize_dag.py`
- `dags/modules/dataset/graph_builder.py`

검증할 산출물:

- bronze incremental parquet
  - key: `crawled_news_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
  - 필수 컬럼: `news_id`, `title`, `url`, `pub_date`, `text` 등 downstream refinement가 요구하는 컬럼
- refined incremental parquet
  - key: `refined_news_incremental/window_start=.../window_end=.../data.parquet`
  - 필수 컬럼: `news_id`, `title`, `content`, `pub_date`, `news_embedding` 등 downstream graph/load가 요구하는 컬럼
- Gemini incremental parquet
  - keys:
    - `extracted_keywords_incremental/window_start=.../window_end=.../data.parquet`
    - `extracted_stocks_incremental/window_start=.../window_end=.../data.parquet`
    - `analyzed_news_incremental/window_start=.../window_end=.../data.parquet`
  - 필수 컬럼:
    - keywords: `news_id`, `keyword`
    - stocks: `news_id`, `stock_id`
    - analysis: `news_id`
- daily finalized parquet
  - keys:
    - `refined_news/year=YYYY/month=MM/day=DD/data.parquet`
    - `extracted_keywords/year=YYYY/month=MM/day=DD/data.parquet`
    - `extracted_stocks/year=YYYY/month=MM/day=DD/data.parquet`
    - `analyzed_news/year=YYYY/month=MM/day=DD/data.parquet`
  - dedupe 기준:
    - refined/analyzed: `news_id`
    - keywords: `news_id`, `keyword`
    - stocks: `news_id`, `stock_id`
- GNN trainset artifact
  - `silver/trainset/date=YYYYMMDD/hetero_graph.pt`
  - `silver/trainset/date=YYYYMMDD/node_mapping.pkl`

우선 테스트:

- fake S3에 저장된 parquet를 다시 읽어 schema와 row 수를 검증한다.
- empty input이면 file을 만들지 않는지, 빈 file을 만드는지 정책을 명시한다.
- daily merge가 중복 row를 제거하고 최신 row를 남기는지 검증한다.

### 2. DB Row / SQL Artifact

대상:

- `dags/modules/loading/news_loader.py`
- `dags/modules/loading/keyword_loader.py`
- `dags/modules/loading/stock_loader.py`
- `dags/modules/loading/load_service.py`
- `dags/modules/serving/deploy.py`
- `dags/sql/*.sql`

검증할 산출물:

- `filtered_news`
  - refined parquet row가 DB insert tuple로 바뀌는 구조
- `keywords`
  - keyword text와 embedding/vector 컬럼 계약
- `news_keyword_mapping`
  - `news_id`, `keyword_id`, `extractor_version`, `weight`
- `news_stock_mapping`
  - `news_id`, `stock_id`, `extractor_version`, `weight`
- `test_service_embeddings`
  - `entity_id`, `entity_type`, `entity_name`, `gnn_embedding`, `model_version`
- recommendation snapshot SQL
  - `recommendation_path_c_snapshot`
  - `recommendation_path_a2_snapshot`
- bandit state SQL
  - `recommendation_bandit_state`

우선 테스트:

- `execute_values` 또는 cursor call을 monkeypatch해서 insert tuples를 capture한다.
- SQL 문자열에 target table, conflict key, essential columns가 포함되는지 검증한다.
- SQL 파일은 존재 여부만이 아니라 downstream repository가 기대하는 컬럼을 만드는지 정적으로 검증한다.
- 실제 DB에는 연결하지 않는다.

### 3. Redis / Bandit Payload

대상:

- `dags/modules/serving/bandit_posterior.py`
- `app/services/bandit_service.py`

검증할 산출물:

- posterior key
  - global: `recommend:bandit:global:{path}`
  - user: `recommend:bandit:user:{user_id}:{path}`
- payload JSON
  - `scope`
  - `user_id`
  - `path`
  - `alpha`
  - `beta`
  - `window_start`
  - `window_end`
  - `reward_count`
  - `impression_count`

우선 테스트:

- payload를 JSON으로 parse해 required key와 numeric type을 검증한다.
- API bandit state reader가 같은 payload를 읽을 수 있는지 fake store로 round-trip 검증한다.
- payload 손상 시 fixed allocator fallback이 유지되는지 기존 API 테스트와 연결한다.

### 4. MLflow / Model Artifact

대상:

- `dags/modules/training/trainer.py`
- `dags/modules/serving/deploy.py`

검증할 산출물:

- stage dir
  - `best_model.pt`
  - `final_model.pt`
  - `stage_metadata.json`
  - `config_snapshot.json`
- MLflow artifact
  - `node_embeddings.pkl`
  - `node_mapping.pkl`
- MLflow tags
  - `status`
  - `gate_passed`
  - `candidate_version`

우선 테스트:

- tmp stage dir에 metadata/config/model placeholder를 만들고 evaluation/deploy가 요구하는 key를 검증한다.
- fake MLflow client로 artifact path 선택과 tag transition을 capture한다.
- `node_embeddings.pkl`과 `node_mapping.pkl`을 DB serving row로 변환하는 계약을 유지한다.

## 추천 테스트 파일 구조

```text
tests/
  test_offline_file_artifacts.py
    - parquet key/schema/dedupe
    - fake S3/tmp parquet

  test_offline_db_artifacts.py
    - insert tuple shape
    - SQL target table/column contract
    - execute_values monkeypatch

  test_offline_bandit_artifacts.py
    - posterior key/payload
    - fake store round-trip

  test_offline_model_artifacts.py
    - stage metadata/config
    - MLflow artifact/tag contract
    - serving embedding row shape
```

이미 일부 계약은 기존 테스트가 커버한다.

- `tests/test_gnn_graph_builder.py`
  - `hetero_graph.pt`, `node_mapping.pkl`
- `tests/test_gnn_serving_deploy.py`
  - `node_embeddings.pkl`, `node_mapping.pkl` -> `test_service_embeddings` row
- `tests/test_bandit_posterior_utils.py`
  - posterior key/payload

새 테스트는 기존 테스트를 중복하지 않고 ingestion/daily finalize/SQL snapshot/loader 산출물 쪽을 우선 보강한다.

## 우선순위

1. `test_offline_file_artifacts.py`
   - ingestion incremental parquet와 daily finalized parquet schema/dedupe 검증
2. `test_offline_db_artifacts.py`
   - loader insert tuple과 recommendation snapshot SQL column contract 검증
3. `test_offline_bandit_artifacts.py`
   - batch posterior payload와 API reader round-trip 검증
4. `test_offline_model_artifacts.py`
   - trainer stage artifact와 MLflow deploy artifact 계약 보강

## 실행 방식

초기에는 별도 서비스로 분리한다.

```bash
docker compose run --rm offline-artifact-test
```

서비스는 테스트 의존성이 충분한 기존 `gnn-test` target을 재사용할 수 있다.
운영 DB/S3/Redis/MLflow 연결 정보는 주입하지 않는다.

현재 구현된 초기 테스트:

- `tests/test_offline_file_artifacts.py`
- `tests/test_offline_db_artifacts.py`

최근 확인 결과:

```text
5 passed
```

## 제외 범위

- 운영 DB read-only 검증
- 로컬 compose PostgreSQL/MinIO/Redis/MLflow에 실제 접속하는 통합 테스트
- Airflow `TaskInstance.run()` 또는 `dag.test()` 기반 runtime 검증
- 대량 데이터 성능 검증

위 항목은 별도 live/integration test plan에서 다룬다.
