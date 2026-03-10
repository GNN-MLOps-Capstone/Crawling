# News Integration Hourly Migration Plan

## 0. Document Scope

- 기준일: `2026-03-10`
- 이 문서는 `news_analysis_integration_v2`를 더 짧은 주기로 운영하기 위한 구조 변경 방향을 정리한다.
- 목표는 `news_dag` 및 `news2minio` 이후에 더 자주 후속 처리를 수행하되, 기존 일 단위 산출물과 `gnn_trainset_creation`의 안정성을 깨지 않는 것이다.
- 현재 코드를 기준으로 분석한 계획 문서이며, 구현 전 설계 결정을 포함한다.
- 본 문서의 결론은 아래 두 가지다.
  - hourly 처리 입력은 `time window` 기준으로 설계한다.
  - hourly 산출물은 기존 daily parquet를 덮어쓰지 않고 `incremental 전용 경로`로 분리 저장한다.
- 추가로 아래 운영 결정을 확정한다.
  - `time window` 기준 컬럼은 `created_at`으로 고정한다.
  - Bronze 적재도 daily 조회가 아니라 `windowed` 방식으로 전환한다.
  - Bronze는 incremental 원본만 유지하고, daily Bronze 재구성은 기본 범위에서 제외한다.
  - hourly 구간의 Bronze 적재와 integration 처리는 하나의 DAG로 합친다.
  - hourly DAG 내부에서는 `window_start`, `window_end`, object key 같은 경량 메타데이터만 XCom으로 전달한다.
  - `stock snapshot`은 당장 기준일 정책을 바꾸지 않고 후순위로 둔다.

## 1. Current State Summary

### 현재 전제

- `news_dag`는 현재 한국시간 기준 `3시간` 주기 수집 DAG다.
- `news2minio_daily`는 현재 한국시간 기준 `02:00`에 하루치 데이터를 Bronze MinIO로 적재한다.
- `news_analysis_integration_v2`는 하루 단위 날짜 리스트를 받아 `refinement -> gemini -> keyword snapshot -> db load`를 수행한다.
- `gnn_trainset_creation`은 한국시간 기준 `04:00`에 D-1 기준 trainset을 생성한다.

### 현재 구조의 핵심 특징

- Bronze 적재가 `crawled_news/year=.../month=.../day=.../data.parquet` 형태의 일 단위 누적 파일이다.
- Silver 산출물도 `refined_news`, `extracted_keywords`, `extracted_stocks`, `analyzed_news` 모두 일 단위 `data.parquet` 파일 하나를 전제로 한다.
- `keyword_embeddings`는 날짜별 스냅샷 파일을 만든다.
- `gnn_trainset_creation`은 silver 전체 히스토리와 최신 keyword snapshot을 사용해 D-1 기준 그래프를 생성한다.

## 2. Why Simple Rescheduling Is Unsafe

### 문제 1. 일 단위 overwrite 구조

- 현재 `news_analysis_integration_v2`는 날짜별 결과를 같은 daily parquet 경로에 다시 쓴다.
- 따라서 DAG 주기만 `1시간`으로 낮추면, 같은 날짜의 일부 데이터만 처리한 결과가 기존 일 단위 결과를 덮어쓸 수 있다.
- 즉, “짧은 주기 실행”과 “현재 silver 저장 구조”가 직접적으로 충돌한다.

### 문제 2. 실제 처리 단위가 날짜

- 현재 DAG 입력은 `updated_dates` 중심이다.
- Bronze 읽기, refinement, Gemini 분석, keyword snapshot, DB load 모두 사실상 날짜 파티션 전체를 기준으로 설계돼 있다.
- 이 구조에서는 하루 동안 같은 날짜를 여러 번 다시 읽고 다시 분석하게 되어 비용이 커진다.

### 문제 3. keyword snapshot의 비용과 정합성

- `keyword_embeddings/date=YYYYMMDD/...` 스냅샷은 날짜 단위 누적 레지스트리다.
- 이를 시간 단위로 자주 만들면 비용이 크고, trainset 생성 시 어떤 snapshot이 기준인지 해석이 모호해진다.

### 문제 4. gnn_trainset_creation과의 결합

- `gnn_trainset_creation`은 `target_date`와 별개로 keyword snapshot을 “전체 최신” 기준으로 고르고 있다.
- hourly snapshot이 생기면 `target_date`와 맞지 않는 snapshot이 선택될 가능성이 있다.
- 그 결과 D-1 trainset이 D-day 이후 snapshot을 참조하는 식의 정합성 문제가 생길 수 있다.

## 3. Recommended Target Architecture

### 방향 요약

- `news_analysis_integration_v2`를 그대로 hourly로 줄이지 않는다.
- 대신 “hourly 통합 DAG”와 “일 마감용 DAG”를 분리한다.
- `gnn_trainset_creation`은 여전히 하루 한 번, D-1 기준으로 동작하도록 유지한다.
- hourly 배치 경계는 `window_start`, `window_end`로 정의한다.
- hourly silver 결과는 daily 결과와 분리된 incremental 경로에 저장한다.

### 권장 구조

#### 3.1 Ingestion Layer

- `news_dag`는 수집/크롤링만 담당한다.
- hourly Bronze 적재와 후속 integration은 하나의 DAG 안에서 수행한다.
- 별도 `news2minio` hourly DAG를 두지 않고, hourly DAG의 앞단 task로 Bronze 적재를 포함한다.
- Bronze 적재는 날짜 전체 조회 대신 `time window` 기준 적재로 전환한다.
- `time window` 경계 판정 기준 컬럼은 `created_at`으로 고정한다.
- Bronze는 incremental 경로를 원본 저장 형태로 사용한다.
- Bronze daily parquet 재구성은 기본 범위에 포함하지 않는다.

#### 3.2 Hourly Unified Integration DAG

- 새 hourly DAG 하나로 Bronze 적재와 integration을 함께 수행한다.
- 입력은 `window_start`, `window_end` 두 개의 KST 시각으로 고정한다.
- 역할은 아래로 제한한다.
  - Bronze incremental 적재
  - 아직 처리되지 않은 기사 식별
  - refinement 수행
  - Gemini 분석 수행
  - DB upsert 수행
- 이 DAG에서는 `keyword snapshot`을 만들지 않는다.
- 이 DAG는 날짜 전체가 아니라 “이번 `time window`에 들어온 신규 기사”만 처리해야 한다.
- DAG 내부 task 간에는 XCom으로 아래 경량 값만 전달한다.
  - `window_start`
  - `window_end`
  - `window_label`
  - Bronze object key 목록
  - 대상 `news_id` 목록
- 기사 본문 dataframe이나 대용량 결과는 XCom에 넣지 않는다.
- hourly 결과는 아래와 같은 incremental 전용 경로에 저장한다.
  - `refined_news_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
  - `extracted_keywords_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
  - `extracted_stocks_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
  - `analyzed_news_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`

#### 3.3 Daily Finalize DAG

- 하루 한 번 D-1 기준으로 실행한다.
- D-1 데이터는 한국시간 `03:00` 시점에 닫는 것으로 본다.
- `03:00 KST` 이후에 유입된 D-1 late arrival는 trainset 기준 데이터에 보정하지 않는다.
- 역할은 아래로 제한한다.
  - D-1에 해당하는 incremental silver 산출물 수집
  - incremental 결과를 daily parquet로 재구성
  - D-1 기준 keyword snapshot 생성
  - trainset 생성이 읽을 daily snapshot 확정
- 이 단계가 끝난 후 `gnn_trainset_creation`이 실행되도록 유지한다.

#### 3.4 Trainset DAG

- `gnn_trainset_creation`은 계속 한국시간 `04:00`에 실행한다.
- 입력 기준일은 D-1 유지가 적절하다.
- 다만 keyword snapshot 선택은 “전체 최신”이 아니라 “`target_date` 이하 최신”으로 제한해야 한다.

## 4. Change Strategy

### 단계 1. Hourly 전환 전에 고쳐야 할 전제

- `news_analysis_integration_v2`의 처리 단위를 `updated_dates` 중심에서 `time window` 중심으로 분리한다.
- 표준 입력 파라미터는 아래 두 개로 고정한다.
  - `window_start`
  - `window_end`
- 두 값은 모두 KST 기준 문자열 또는 timezone-aware datetime으로 다룬다.
- window 포함 판정은 `created_at` 기준 `window_start <= created_at < window_end`로 고정한다.
- 배치 경계가 명확해야 `news2minio -> integration` 연쇄 실행 시 중복 범위와 누락 범위를 통제할 수 있다.
- hourly DAG 내부에서는 같은 window context를 XCom으로 재사용한다.

### 단계 2. Silver 저장 구조 분리

- hourly DAG는 기존 daily parquet를 직접 덮어쓰지 않는다.
- hourly 산출물은 무조건 incremental 전용 경로에 저장한다.
- 경로 규칙은 배치 경계를 포함해야 한다.
- 권장 예시는 아래와 같다.
  - `refined_news_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
  - `extracted_keywords_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
  - `extracted_stocks_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
  - `analyzed_news_incremental/window_start=YYYYMMDDHHmm/window_end=YYYYMMDDHHmm/data.parquet`
- 이후 daily finalize가 D-1 대상 incremental 결과를 모아 기존 daily 경로를 재구성한다.
- 이 방식은 다음 장점이 있다.
  - hourly 재실행 시 daily 확정본을 손상시키지 않는다.
  - 어떤 배치가 어떤 결과를 만들었는지 역추적이 쉽다.
  - D-1 마감 전후를 분리해 운영할 수 있다.

### 단계 3. Hourly DAG에서 keyword snapshot 제거

- keyword embedding registry는 비용이 큰 누적 산출물이다.
- hourly DAG에서는 생성하지 않는다.
- keyword snapshot은 daily finalize에서만 만든다.

### 단계 4. DB 적재는 유지하되 입력 범위만 축소

- 현재 DB 적재 로직은 상당 부분 idempotent하다.
- `filtered_news`는 upsert 구조다.
- `news_keyword_mapping`, `news_stock_mapping`은 해당 `news_id` 기준 delete 후 insert 구조다.
- 따라서 hourly 배치 입력을 `window_start ~ window_end` 범위로 정확히 좁히면 DB 계층은 비교적 안전하게 재사용할 수 있다.
- 다만 “window 범위에 속한 기사만 읽기”가 각 모듈에서 일관되게 보장되어야 한다.

### 단계 5. Trainset 보호 조치

- `gnn_trainset_creation`의 keyword snapshot 선택 로직을 `target_date` 이하 최신으로 변경한다.
- `stock snapshot`은 당장 변경하지 않고 현행 정책을 유지한다.
- trainset 생성은 daily finalize 완료 후에만 실행되게 유지한다.
- hourly DAG는 `trainset/date=...` 경로를 건드리지 않는다.

## 5. Recommended Implementation Order

1. `gnn_trainset_creation`의 snapshot 선택 로직부터 날짜 기준으로 고정한다.
2. `news_analysis_integration_v2`를 바로 수정하지 말고, Bronze 적재를 포함하는 hourly unified DAG 설계를 먼저 만든다.
3. hourly DAG 입력 단위를 `time window` 기반으로 처리하는 공용 모듈을 `dags/modules/`에 추가한다.
4. refinement, Gemini, load 단계가 “부분 배치 입력”을 받을 수 있게 모듈 시그니처를 확장한다.
5. hourly 결과를 daily parquet와 분리된 경로에 저장한다.
6. daily finalize DAG에서 incremental 결과를 일 단위 결과로 합친다.
7. daily finalize 이후 keyword snapshot을 생성한다.
8. `gnn_trainset_creation`은 기존 `04:00 KST` 스케줄을 유지한다.

## 6. Scope Recommendation

### 이번 변경 범위로 적절한 것

- hourly 전환을 위한 설계 문서화
- `gnn_trainset_creation` 보호 조건 반영
- Bronze 적재를 포함하는 hourly unified DAG 추가
- daily finalize DAG 추가
- `time window` 기준 공용 reader/writer 추가
- incremental silver 경로 규칙 추가

### 이번 변경 범위에 넣지 않는 것이 좋은 것

- 기존 `news_analysis_integration_v2`를 큰 폭으로 재활용하려는 시도
- keyword snapshot을 hourly로 생성하는 구조
- trainset 생성 시점을 hourly와 연결하는 구조

## 7. Post-Change Verification Checklist

### 7.1 Airflow Wiring

- hourly unified DAG가 의도한 주기로 실행되는지 확인
- hourly unified DAG 안에서 Bronze 적재 후 후속 integration task가 같은 window context를 공유하는지 확인
- Bronze 적재가 `created_at` 기준 window 범위로 수행되는지 확인
- daily finalize DAG와 `gnn_trainset_creation`의 실행 순서가 의도대로 유지되는지 확인

### 7.2 Data Boundary

- hourly DAG가 신규 기사만 처리하는지 확인
- `window_start`, `window_end` 경계가 인접 배치와 겹치거나 비지 않는지 확인
- 이미 `passed`, `filtered_out`, `skipped` 처리된 뉴스를 다시 비효율적으로 읽지 않는지 확인
- 재시도 시 동일 `news_id`가 중복 적재되지 않는지 확인

### 7.3 Silver Output

- hourly DAG가 기존 daily parquet를 손상시키지 않는지 확인
- incremental 전용 경로를 도입한 경우 경로 규칙이 일관적인지 확인
- 하나의 hourly window가 하나의 incremental 산출물 세트로만 기록되는지 확인
- daily finalize 이후 D-1 parquet가 완전한 일 단위 데이터로 재구성되는지 확인

### 7.4 DB Load

- `filtered_news` upsert가 정상 동작하는지 확인
- `news_keyword_mapping`, `news_stock_mapping`가 대상 `news_id` 범위에서만 교체되는지 확인
- Gemini 요약/감성 업데이트가 부분 배치에서도 정상 반영되는지 확인

### 7.5 Keyword Snapshot

- keyword snapshot이 daily finalize에서만 생성되는지 확인
- snapshot 날짜와 파일 경로가 D-1 기준으로 고정되는지 확인
- trainset 생성 시 참조하는 snapshot이 `target_date` 이하 최신인지 확인

### 7.6 Trainset Safety

- `gnn_trainset_creation`이 hourly DAG 실행 여부와 무관하게 매일 `04:00 KST`에만 수행되는지 확인
- D-1 기준 trainset 경로만 갱신되는지 확인
- trainset 생성 결과가 전일 데이터 기준으로 재현 가능한지 확인

### 7.7 Operational Checks

- hourly DAG 1회 실행 시간이 주기보다 충분히 짧은지 확인
- `max_active_runs`, 재시도, 외부 API 호출량이 운영 가능한 수준인지 확인
- Ollama/Gemini 호출량 증가로 인한 병목이나 rate limit 문제가 없는지 확인

### 7.8 Failure And Retry

- 같은 `window_start/window_end` 재실행 시 같은 incremental key를 재생성하는지 확인
- hourly DAG 중간 실패 시 대용량 payload 없이 XCom context만 재사용되는지 확인
- `processing` 상태로 남은 뉴스가 다음 재실행에서 복구 대상에 포함되는지 확인
- daily finalize에서 필수 incremental 산출물 누락 시 실패하도록 동작하는지 확인
- finalize 실패 시 `gnn_trainset_creation`이 실행되지 않는지 확인

## 8. Fixed Decisions

- `time window` 기준 컬럼은 `created_at`으로 고정한다.
- Bronze 적재는 `windowed` 방식으로 전환한다.
- Bronze는 incremental 원본만 유지하고 daily Bronze 재구성은 하지 않는다.
- hourly 구간의 Bronze 적재와 integration은 하나의 DAG로 합친다.
- hourly DAG 내부의 window 전달은 XCom의 경량 메타데이터로 처리한다.
- D-1 daily finalize cutoff는 `03:00 KST`로 고정하고 late arrival 보정은 하지 않는다.
- `stock snapshot`은 현행 정책을 유지하고, keyword snapshot 정합성 보강보다 우선하지 않는다.

## 9. Recommended Next Step

- 다음 구현 단계에서는 아래 순서가 맞다.
  - `created_at` 기준 `time window` reader와 context 생성 로직 구현
  - Bronze 적재를 포함하는 hourly unified DAG 구현
  - incremental silver 경로 규칙 확정
  - daily finalize가 incremental을 daily parquet로 합치는 방식 확정
- 그 다음 `gnn_trainset_creation`의 snapshot 선택 로직을 먼저 안전하게 고정하고, 이후 hourly DAG와 finalize DAG를 분리 구현하는 순서가 가장 리스크가 낮다.
