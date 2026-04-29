# News Integration Hourly DAG And Module Design

## 0. Document Scope

- 기준일: `2026-03-10`
- 이 문서는 hourly 전환을 위한 구체적인 DAG/모듈 변경안을 정리한다.
- 방향 전제는 아래와 같다.
  - 입력 단위는 `time window`
  - hourly 산출물은 `incremental 전용 경로`
  - `gnn_trainset_creation`은 daily finalized 결과만 사용
  - window 기준 컬럼은 `created_at`
  - Bronze도 `windowed` 적재로 전환
  - Bronze는 incremental 원본만 유지
  - hourly 구간은 하나의 DAG 안에서 Bronze 적재와 integration을 함께 수행
  - hourly DAG 내부 전달은 XCom의 경량 메타데이터만 사용
  - `stock snapshot`은 당장 변경하지 않음

## 1. Target DAG Topology

### 1.1 Recommended DAG Split

- `news_dag`
  - 뉴스 수집 및 크롤링
- `news_ingestion_integration_hourly`
  - `window_start`, `window_end` 범위의 Bronze 적재
  - 같은 `window_start`, `window_end` 범위만 후처리
  - incremental silver 저장 + DB load
- `news_analysis_daily_finalize_dag`
  - 매일 1회 D-1 대상 incremental 결과를 모아 daily silver 확정본 생성
  - keyword snapshot 생성
- `gnn_trainset_creation`
  - 매일 04:00 KST 실행
  - daily finalize 이후 확정본과 D-1 이하 snapshot 사용

### 1.2 Dependency Principle

- hourly DAG는 trainset을 직접 트리거하지 않는다.
- daily finalize와 trainset은 시간 순서 또는 Dataset dependency로만 연결한다.
- trainset은 incremental 결과를 직접 읽지 않는다.
- hourly DAG 내부 task 간 전달은 XCom으로 하되, 기사 본문이나 대용량 dataframe은 전달하지 않는다.

## 2. Proposed New And Changed DAGs

### 2.1 신규 `news_ingestion_integration_hourly`

목표:
- Bronze 적재와 incremental integration을 하나의 hourly DAG로 통합

권장 변경:
- 새 DAG 파일로 추가
  - 예: `dags/news_ingestion_integration_hourly_dag.py`
- 기존 `news2minio_daily`와 `news_analysis_integration_v2`는 당분간 유지하되, 신규 흐름이 안정화되면 대체 대상이다

권장 파라미터:
- `window_start`
- `window_end`

권장 동작:
- DB에서 `created_at` KST 기준 `window_start <= created_at < window_end` 범위만 조회
- Bronze도 incremental 전용 경로로 저장하는 쪽을 기본안으로 둔다
- 권장 경로 예시:
  - `crawled_news_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
- Bronze daily 재구성은 기본 범위에서 제외한다

주의:
- Bronze 적재 결과와 같은 window context를 후속 task가 그대로 재사용해야 한다.
- XCom에는 아래 값만 넣는다.
  - `window_start`
  - `window_end`
  - `window_label`
  - Bronze object key 목록
  - 대상 `news_id` 목록
- 기사 본문 dataframe은 XCom에 넣지 않는다.

권장 task 구성:

1. `prepare_context`
- `data_interval_start`, `data_interval_end`에서 window 계산
- AWS/DB 연결 정보 준비
- `window_label` 생성

2. `bronze_ingest_window`
- `created_at` 기준 window 범위 기사 조회
- Bronze incremental 경로 저장
- 저장된 object key 목록 반환

3. `resolve_target_news`
- `crawled_news`에서 `window_start <= created_at < window_end`
- 상태 조건 예시
  - `pending`
  - `failed_retryable`
  - 필요 시 `processing` 재복구 대상
- 출력
  - `news_ids`
  - 최소 필드 dataframe 또는 경량 dict

4. `refinement_incremental`
- 대상 `news_ids`만 refinement
- 결과 저장
  - `refined_news_incremental/window_start=.../window_end=.../data.parquet`

5. `gemini_incremental`
- incremental refined output만 읽어 stocks/keywords/analysis 생성
- 결과 저장
  - `extracted_stocks_incremental/...`
  - `extracted_keywords_incremental/...`
  - `analyzed_news_incremental/...`

6. `load_incremental_to_db`
- hourly incremental 결과를 읽어 DB upsert
- keyword master snapshot sync는 하지 않음

7. `report_window_status`
- 해당 window 범위에서 처리 전/후 상태 집계

### 2.3 신규 `news_analysis_daily_finalize_dag`

파일 예시:
- `dags/news_analysis_daily_finalize_dag.py`

입력:
- `target_date`

권장 schedule:
- 한국시간 매일 `03:00`
- `03:00 KST` 시점에 D-1 데이터를 닫고 late arrival는 보정하지 않음

권장 task 구성:

1. `collect_incremental_paths`
- D-1에 해당하는 incremental 경로 목록 수집

2. `merge_refined_daily`
- incremental refined 파일을 merge
- `news_id` 기준 dedupe
- daily 확정 경로 저장
  - `refined_news/year=YYYY/month=MM/day=DD/data.parquet`

3. `merge_analysis_daily`
- `extracted_keywords`, `extracted_stocks`, `analyzed_news`도 동일 방식으로 merge

4. `build_keyword_snapshot`
- daily `extracted_keywords` 확정본 기준으로 snapshot 생성

5. `finalize_report`
- 누락 파일, 빈 결과, row count 보고

## 3. Module Change Plan

### 3.1 Ingestion Reader

현재:
- `read_daily_news(target_date, db_info)`

권장 추가:
- `read_news_by_time_window(window_start, window_end, db_info)`

역할:
- KST 기준 `window_start <= created_at < window_end`
- 반환 컬럼은 현재와 동일하게 유지
  - `news_id`
  - `text`
  - `pub_date`

변경 원칙:
- 기존 `read_daily_news`는 유지
- 새 함수만 추가해서 daily/houly 공존 가능하게 한다

### 3.2 Analysis Input Resolver

신규 모듈 예시:
- `dags/modules/analysis/window_selector.py`

역할:
- DB에서 window 범위 대상 기사 조회
- 상태 조건 필터링
- `news_id`, `text`, `pub_date`, `filter_status` 등 최소 필드 반환

필요 이유:
- 현재 `news_service`는 Bronze daily parquet 전체를 읽는다
- hourly 전환에서는 먼저 “이번 window에 처리할 대상”을 고르는 계층이 필요하다

### 3.3 Refinement Service

현재:
- `run_refinement_process(updated_dates, aws_info, db_info, config_path)`

권장 추가:
- `run_refinement_process_for_window(window_start, window_end, aws_info, db_info, config_path)`
- 또는 더 범용적으로
  - `run_refinement_process_for_records(records, aws_info, db_info, output_key, config_path)`

권장 동작:
- 대상 dataframe 또는 records를 직접 받아 처리
- 결과 저장 경로를 호출부에서 주입
- hourly일 때는 incremental 경로로 저장
- daily finalize는 이 함수를 사용하지 않고 이미 저장된 incremental을 merge

핵심 수정 포인트:
- 입력을 S3 daily key에서 직접 읽는 구조를 분리
- 처리 로직과 입출력 경로 결정을 분리

### 3.4 Gemini Service

현재:
- `run_gemini_service(updated_dates, aws_info, config_path, stock_map, db_info)`

권장 추가:
- `run_gemini_service_for_parquet(input_bucket, input_key, output_keys, aws_info, config_path, stock_map, db_info)`
- 또는
  - `run_gemini_service_for_df(df, output_keys, aws_info, config_path, stock_map, db_info)`

권장 동작:
- hourly incremental refined 파일만 입력으로 사용
- output key를 호출부에서 명시적으로 전달
- 결과를 incremental 경로에 저장

### 3.5 Load Service

현재:
- `run_db_loading(targets, snapshot_path, aws_info, pg_info)`

권장 변경:
- 기존 함수는 daily 유지
- 신규 함수 추가
  - `run_incremental_db_loading(target, aws_info, pg_info)`

hourly incremental에서 필요한 동작:
- `filtered_news` upsert
- `analyzed_news` 반영
- `news_keyword_mapping` 반영
- `news_stock_mapping` 반영

hourly incremental에서 제외할 동작:
- `sync_master_keywords`

이유:
- keyword master는 daily finalize에서 확정본 기준으로만 갱신해야 trainset과 기준일이 맞는다

### 3.6 Daily Finalize Merge Service

신규 모듈 예시:
- `dags/modules/analysis/daily_finalize_service.py`

필요 함수 예시:
- `list_incremental_keys(prefix, target_date, aws_info)`
- `merge_incremental_parquets(keys, dedupe_key, aws_info)`
- `write_daily_partition(df, base_prefix, target_date, aws_info)`

dedupe 기준:
- `refined_news`: `news_id`
- `analyzed_news`: `news_id`
- `extracted_keywords`: `news_id, keyword`
- `extracted_stocks`: `news_id, stock_id`

## 4. Proposed Data Contract

### 4.1 Hourly Context

권장 context 예시:

```python
{
    "window_start": "2026-03-10T10:00:00+09:00",
    "window_end": "2026-03-10T11:00:00+09:00",
    "window_label": "202603101000_202603101100",
    "aws": {...},
    "db": {...}
}
```

### 4.2 Incremental Output Keys

권장 key 예시:

```text
refined_news_incremental/window_start=202603101000/window_end=202603101100/data.parquet
extracted_keywords_incremental/window_start=202603101000/window_end=202603101100/data.parquet
extracted_stocks_incremental/window_start=202603101000/window_end=202603101100/data.parquet
analyzed_news_incremental/window_start=202603101000/window_end=202603101100/data.parquet
```

이 규칙의 장점:
- 경계가 명확하다
- 재실행 배치 식별이 쉽다
- target date 기준 path filtering이 단순하다

### 4.3 XCom Payload Principle

허용:
- `window_start`
- `window_end`
- `window_label`
- object key 문자열 리스트
- `news_id` 리스트

비허용:
- 기사 본문 dataframe
- parquet binary
- 대용량 분석 결과 전체

## 5. `gnn_trainset_creation` Protection Plan

### 5.1 What Must Not Change

- 스케줄은 매일 `04:00 KST` 유지
- 기본 `target_date`는 예약 실행 시 D-1 유지
- 입력은 finalized daily silver 기준 유지

### 5.2 Required Code Change

현재 문제:
- keyword snapshot을 전체 최신 기준으로 고른다

권장 수정:
- `target_date` 이하 snapshot만 후보로 삼는다
- 후보 중 가장 최신을 선택한다

추가 권장:
- stock snapshot은 현행 정책을 유지하고 후순위로 둔다

### 5.3 Why This Is Enough

- hourly incremental은 trainset이 읽는 daily 확정본을 직접 건드리지 않는다
- daily finalize가 완료된 뒤에만 trainset이 확정본을 읽는다
- 따라서 trainset은 hourly 처리 여부와 독립적으로 안정성을 유지할 수 있다

## 6. Implementation Sequence

1. `read_news_by_time_window` 추가
2. window 범위 대상 기사 선택 모듈 추가
3. Bronze incremental writer 추가
4. refinement/gemini를 record 또는 parquet 입력 기반 함수로 분리
5. incremental DB load 함수 추가
6. `news_ingestion_integration_hourly_dag` 추가
7. daily finalize merge service 추가
8. `news_analysis_daily_finalize_dag` 추가
9. `gnn_trainset_creation` snapshot 선택 기준 보강

## 7. Post-Implementation Checks

- 인접한 두 hourly window가 중복 처리하지 않는지 확인
- window 재실행 시 같은 incremental 경로가 예측 가능하게 재생성되는지 확인
- daily finalize 결과 row count가 incremental 합계와 일치하는지 확인
- daily finalize 이후 keyword snapshot이 D-1 기준으로만 생성되는지 확인
- `gnn_trainset_creation`이 finalized daily output만 읽는지 확인

## 8. Failure And Retry Policy

- hourly DAG 재실행은 같은 `window_start/window_end`를 기준으로 같은 incremental key를 다시 생성하는 것을 원칙으로 한다.
- Bronze 적재 성공 후 후속 task 실패 시, 재실행은 같은 window context를 재사용한다.
- `resolve_target_news`는 `failed_retryable`와 해당 window 범위의 `processing` 레코드를 재처리 대상에 포함할 수 있어야 한다.
- daily finalize는 필수 incremental 산출물 누락 시 실패해야 한다.
- daily finalize 실패 시 `gnn_trainset_creation`은 실행되면 안 된다.
