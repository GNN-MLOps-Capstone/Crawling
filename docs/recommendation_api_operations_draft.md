# Recommendation API Implementation And Operations Draft

## 0. Document Scope

- 기준일: `2026-03-09`
- 이 문서는 추천 API에서 "무엇을 구현해야 하는가"를 단계별 체크리스트로 정리한 실행 문서다.
- 상위 목표와 아키텍처 배경은 [recommendation_api_design_draft.md](/home/dobi/Crawling/docs/recommendation_api_design_draft.md)를 기준으로 본다.
- 현재 구현 단계는 `LLM Ranker`를 제거하고, `A1(온보딩) / A2(로그) / B(속보) / C(인기 탐색)` 구조를 기준으로 재정렬하는 것이다.
- 이 문서 기준 `Path C`는 FastAPI 내부 실시간 계산이 아니라, Airflow가 약 `10분`마다 생성하는 snapshot을 읽는 방식으로 정리한다.
- 이 저장소의 추천 API 검증 기준은 기본적으로 `Docker compose` 환경이다.
- 로컬 셸에 `pytest`, `psycopg2` 같은 패키지가 없더라도, `recommend-api` 컨테이너가 떠 있으면 먼저 컨테이너 내부에서 검증한다.
- 따라서 검증 가능 여부를 판단할 때는 "호스트 로컬 Python 환경"보다 "`docker compose exec recommend-api ...` 실행 가능 여부"를 우선 확인한다.

## 1. Current Baseline

### 현재 코드 상태

- 엔드포인트는 `POST /recommend/news`다.
- 응답은 `news_id + path` 목록과 cursor를 반환한다.
- 요청마다 세션 캐시를 우선 확인하고, miss 시 추천 세션을 새로 생성한다.
- 추천 계산에 필요한 사용자 신호는 외부 `context`보다 `user_id` 기준 내부 조회를 기본으로 하도록 정리 중이다.
- 현재 구현 기준 기본 동작은 `A1(온보딩) + A2(로그) + B(속보) + C(인기 탐색)` 멀티 패스 믹싱이다.
- `로그 기반 후보(A2)`는 최근 행동 로그 조회, 엔터티 복원, decay, Max-Sim scoring까지 반영돼 있다.
- `인기 탐색 후보(C)`는 앞으로 Airflow snapshot 기반 read-only path로 전환한다.
- 추천 응답 시 impression 로그를 자동 적재하고, `POST /recommend/news/click`로 click 로그를 path와 함께 적재한다.

### 현재 계약

- 요청 필드
  - `user_id`: 필수, `integer`
  - `limit`: 필수, `1 <= limit <= 100`
  - `cursor`: optional
  - `request_id`: optional
  - `context`: optional debug override, schema-fixed
- 응답 필드
  - `request_id`
  - `items[].news_id`
  - `items[].path`
  - `next_cursor`
  - `meta.source`
  - `meta.fallback_used`
  - `meta.fallback_reason`
- 추가 이벤트 엔드포인트
  - `POST /recommend/news/click`

## 2. Phase 1A Checklist

1A 단계 목표는 "`user_id` 기반 내부 신호 조회를 전제로 `A1(온보딩)`과 `B(속보)`를 먼저 안정화하고, `C(인기 탐색)`는 snapshot read 경로까지 포함한 멀티 패스 서빙 뼈대"를 올리는 것이다.

### 2.1 Progress Snapshot

- 완료
  - 추천 요청이 `context builder -> retrieval -> mix -> session cache` 흐름으로 동작한다.
  - `POST /recommend/news` 계약, cursor pagination, 세션 캐시 기반 page slice가 유지된다.
  - `A1/A2/B/C` 후보를 섞어 반환하는 멀티 패스 서빙 경로가 있다.
  - prefetch, batch rollover, latest fallback, `A1 실패 -> B fallback`이 코드에 반영돼 있다.
  - `user_id` 기준 내부 온보딩/행동 신호 조회 스켈레톤이 코드에 반영돼 있다.
  - 내부 조회한 온보딩/행동 신호가 personalized retrieval scoring까지 연결돼 있다.
  - `meta.source`가 `multipath_cold` / `multipath_warm` 수준으로 구분된다.
  - debug override용 `context` 스키마가 명시적으로 고정됐다.
  - impression 자동 로그와 click 로그 적재 경로가 코드와 테스트에 반영돼 있다.
  - Docker 기준 추천 API 테스트, 앱 레벨 HTTP 확인, Redis-backed session cache 구성을 확인했다.
  - stale session은 기존 `served_ids`를 유지한 채 unseen 후보만 재생성하는 restore 정책으로 고정됐다.
  - 첫 page window에서 `breaking/popular`가 완전히 사라지지 않도록 mix guardrail을 둘 방향이 정리돼 있다.
- 부분 완료
  - 로그 필드는 남아 있지만, `impression/click -> recent_actions` 데이터 루프는 아직 닫히지 않았다.
  - 요청 계약은 아직 `context` optional 형태를 유지하지만, 기본 경로는 `user_id(integer)` 기반 내부 조회이고 `context`는 debug override 용도로만 쓰인다.
  - `Path C`는 문서상 snapshot 방식으로 전환 방향이 확정됐지만, API와 배치 구현은 아직 같이 맞춰야 한다.
- 미완료
  - 1A 완료 기준 전부를 만족하는 end-to-end 검증은 아직 아니다.

### 2.2 Request Flow And Contract

- [x] 기존 `POST /recommend/news` 엔드포인트 유지
- [x] 응답 shape를 현재 계약과 호환되게 유지
- [x] `request_id` 단위 추천 세션 생성 및 재사용
- [x] cursor는 세션 포인터 역할로만 사용
- [x] cursor/request_id 불일치 시 400 에러 반환
- [x] `meta.source`, `meta.fallback_used`, `fallback_reason` 응답 유지
- [x] 요청 계약을 `user_id` 중심 최소 입력 형태로 정리
- [x] 외부 `context`를 제거할지, 내부 debug override로만 남길지 결정

### 2.3 User Signal Loading Skeleton

- [x] 내부적으로 `profile`, `recent_actions`, `session_signals` 정규화 구조를 유지
- [x] `A1`과 이후 `A2`가 서로 다른 필드를 읽을 수 있게 구조 분리
- [x] `user_id` 기준으로 온보딩 데이터 조회 경로 정의
- [x] `user_id` 기준으로 최근 행동 로그 조회 경로 정의
- [x] 조회 실패/데이터 없음 시 degrade 코드 체계 정의

### 2.4 Retrieval Baseline

- [x] 후보 조회 결과에 `pub_date` 포함
- [x] 최근 `3일` 기준 base pool 유지
- [x] 최신 기사 정렬 기준 일관성 유지
- [x] 도메인 필터 입력을 retrieval 계층에서 받을 수 있게 유지
- [x] 개인화 경로는 평균 풀링 대신 multi-interest 보존 방향으로 정의
- [x] `A1`은 온보딩에서 선택한 키워드/종목 전체를 입력으로 쓰는 방향 확정
- [x] `A2`는 최근 20개 클릭/유효 읽기 로그를 입력으로 쓰는 방향 확정
- [x] `keyword-keyword`, `stock-stock`, `keyword-stock` pair type별 유사도 보정 필요성 확정
- [x] 온보딩 프로필 기반 boost 규칙 반영
- [x] 내부 조회한 프로필/로그를 retrieval 입력으로 연결

### 2.5 Path A1. Onboarding Baseline

- [x] `profile`이 없어도 넓은 최신 풀에서 baseline 후보 생성
- [x] A1 후보 수 상한 유지
- [x] exclude/seen item 필터 적용
- [x] 온보딩 입력은 선택한 키워드/종목 전체를 그대로 사용하는 방향 확정
- [x] 벡터 매칭은 average pooling이 아니라 Max-Sim 기반으로 정의
- [x] 종목 entity hard match 가산점 적용 방향 확정
- [x] pair type별 similarity calibration 구현
- [x] 실제 온보딩 신호를 반영한 personalized retrieval 구현
- [x] `watchlist(user_id, stock_id)` source 확인
- [x] `user_onboarding_keywords(user_id, keyword_id)` source 확인
- [x] `user_id -> onboarding source` join 구현
- [x] vector lookup 구현

### 2.6 Path B. Breaking Baseline

- [x] 짧은 시간창의 속보 후보 조회 유지
- [x] `stale_cutoff`를 적용할 수 있게 유지
- [x] B 후보 수 상한 유지
- [x] A1과 중복되지 않도록 exclude 처리
- [x] primary path 실패 시 B 중심 fallback 지원

### 2.7 Path C. Popular Exploration Skeleton

- [x] Path C를 정식 경로로 문서에 유지
- [x] `Path C`를 Airflow snapshot 기반 path로 재정의
- [x] snapshot 계산 주기를 약 `10분`으로 두는 방향 확정
- [x] snapshot 이전에 `news_id` 기준 집계 테이블을 먼저 갱신하는 방향 확정
- [x] 집계 테이블 최소 필드(total/path별 impression/click count) 정의
- [x] snapshot 최소 필드(`snapshot_at`, `news_id`, `score`, `rank`, `domain`, `category`) 정의
- [x] recency와 category/domain diversity를 snapshot 계산 단계에서 반영하는 방향 확정
- [x] 세션 캐시에 popular queue를 저장할지 구조 확정
- [ ] 집계 테이블 스키마 확정
- [ ] snapshot 저장 테이블 또는 동등 저장소 스키마 확정
- [ ] Airflow DAG가 집계 테이블을 주기적으로 갱신하도록 구현
- [ ] Airflow DAG가 `Path C` snapshot을 주기적으로 갱신하도록 구현
- [ ] API repository가 최신 snapshot을 read-only 조회하도록 전환
- [ ] snapshot 부재 시 이전 snapshot 사용 규칙 확정

### 2.8 Session Cache And Pagination

- [x] 세션 캐시에 `queue`, `served_ids`, `batch_generation_id` 저장
- [x] 다음 페이지 요청은 캐시된 timeline에서 slice
- [x] prefetch 결과는 현재 batch 소진 전까지 본 timeline에 합치지 않음
- [x] batch rollover 시 prefetched queue를 이어 붙임
- [x] 필요 시 세션 재생성 fallback 지원
- [x] stale session 복원 정책은 기존 served prefix 유지 + unseen tail 재생성으로 고정
- [x] Path C 활성화 시 popular queue 저장 구조 확장
- [ ] popular queue가 snapshot 버전 또는 `snapshot_at`과 함께 저장되도록 계약 확정

### 2.9 Mixing Baseline

- [x] 현재 구현은 고정 mix weight 기반 병합
- [x] path별 큐를 따로 보관하고 최종 timeline은 mix 결과로 생성
- [x] 중복 뉴스 제거 규칙 유지
- [x] MAB 없이도 동작하는 기본 서빙 경로 확보
- [x] 1A 기준 운영 guardrail 문구는 `first page window` 내 `breaking/popular` 최소 노출 규칙으로 구체화
- [ ] snapshot이 stale한 경우 `popular` 비중 축소 또는 비활성화 규칙 확정

### 2.10 Logging And Observability

- [x] request log 스키마 유지
- [x] `latency_ms`, `cache_status`, `fallback_reason`, `batch_generation_id` 로그 유지
- [x] path별 remaining count와 mix ratio 로그 적재
- [x] `context_hash`, `context_present` 로그 적재
- [x] impression/click 로그 실제 적재 경로 검증 필요
- [x] `cold/warm` 분리 집계 기준은 request log의 `user_state` 필드로 적재
- [x] Path C impression/click 집계 필드 정의
- [ ] Airflow 집계 테이블에 total/path별 impression/click count 적재 구현
- [ ] request log에 `popular_snapshot_at` 또는 `popular_snapshot_version` 적재 여부 확정

### 2.11 Validation Status

- [x] 테스트 코드 기준 cursor consistency, fallback, prefetch 시나리오를 작성함
- [x] `pytest tests/test_recommend_api.py` 재실행 확인
- [x] FastAPI 앱 레벨 수동 확인
- [x] Redis 연결이 실제 로컬 환경에서 정상 동작하는지 확인

## 3. Phase 1B Checklist

1B 단계 목표는 "`A2(로그 기반 추천)`를 활성화하고, warm user 대상 개인화 강도를 높이는 것"이다.

### 3.1 Context Contract Finalization

- [x] `context` 허용 필드 목록 확정
- [x] 필드별 optional/required 여부와 기본값 확정
- [x] `context` 버전 관리 또는 schema evolution 방식 결정
- [x] 온보딩 데이터와 로그 데이터가 각각 어떤 필드로 매핑되는지 확정

### 3.2 Path A2. Behavior-Based Retrieval

#### Spec Locked

- [x] `recent_actions`는 `user_id` 기준 최근 20개 클릭/유효 읽기 로그를 기준으로 한다
- [x] 체류 시간 `5초` 미만 로그는 제외한다
- [x] 유니크 키워드 단위로 집계하되 빈도 정보는 유지한다

#### Implementation Checklist

- [x] `user_id -> recent_actions` 조회 경로 확정
- [x] `interaction_events`에서 `content_open/content_leave`와 `content_session_id`로 읽기 세션을 복원
- [x] warm user 활성화 기준 확정
- [x] `content_open`과 `content_leave`를 묶어 dwell time 계산 구현
- [x] `event_ts_client` 기준 최신성 정렬 및 decay 입력 시각 처리 구현
- [x] 빈도 + 최신성 decay 가중치 계산 구현
- [x] pair type별 similarity calibration 구현
- [x] Max-Sim 매칭 구현
- [x] 반복 등장 종목에 대한 entity hard match 구현
- [x] `news_keyword_mapping`, `news_stock_mapping`으로 엔터티 복원 구현
- [x] `interaction_events -> recent_actions` 정제 구현
- [x] 행동 로그 기반 후보 추출 구현
- [x] A2 후보 수 상한 유지
- [x] seen item 필터 적용

### 3.3 Mixing Extension

- [x] `A1/A2/B/C` 병합 규칙 확정
- [x] `A2` 비활성화 시 `A1+B+C`로 자동 degrade
- [x] `meta.source`에 cold/warm source 구분 반영
- [x] `items[].path`를 응답에 포함해 path attribution을 고정
- [x] click/read 로그에 `source_path`를 함께 적재하는 계약 고정
- [x] `fallback_reason`에 `behavior_insufficient`, `profile_missing`, `user_signal_lookup_failed` 등 코드 체계 반영

## 4. Phase 2 Checklist

2단계 목표는 "`MAB`를 붙여 `A1/A2/B/C` mix ratio를 동적으로 조정하는 것"이다.

### 4.1 MAB Introduction

- [x] `MAB`를 기사 ranker가 아니라 `A1/A2/B/C` path blender로 정의
- [x] MAB의 arm을 `A1`, `A2`, `B`, `C`로 고정
- [x] action을 페이지 슬롯별 sequential sampling 기반 allocation으로 정의
- [x] 1차 알고리즘을 경량 `Hierarchical Thompson Sampling`으로 확정
- [x] global prior는 최근 `3일` path별 평균 reward를 사용하고 user posterior의 regularizer로 결합
- [x] primary reward를 `5초 이상 valid dwell`로 확정
- [x] click, short click은 보조 지표로만 유지
- [x] cold/warm은 공통 prior를 쓰고 guardrail만 다르게 적용하는 방향으로 확정
- [ ] 최소 노출 비율 guardrail 확정
- [ ] 충분한 로그 전까지는 fixed mix와 병행 가능한 모드 준비

### 4.2 Serving And Learning Split

- [x] FastAPI는 read-only 서빙 계층으로 고정
- [x] FastAPI는 `Redis` 저장소에서 user별 `alpha`, `beta`를 읽어 sampling만 수행
- [ ] path 비활성화, queue 고갈, fallback 상황에서 arm masking 규칙 확정
- [x] Airflow 배치가 reward 집계와 `alpha`, `beta` 업데이트를 전담하도록 설계
- [x] update 결과 저장소는 `Redis`로 고정
- [ ] 배치가 decay 반영된 `alpha`, `beta`를 덮어쓰는 write contract 정의
- [x] 서빙 요청 경로에서 온라인 학습 연산이 수행되지 않도록 금지 규칙 명시
- [x] 배치 update cadence의 기본값을 `1시간`으로 고정

### 4.3 Popular Exploration Path C

- [x] Path C를 API 실시간 계산이 아니라 Airflow snapshot read path로 확정
- [x] snapshot 입력으로 `news_id` 기준 집계 테이블을 사용한다는 원칙 확정
- [ ] 집계 테이블 이름과 컬럼 정의 확정
- [ ] total/path별 impression/click count 집계 SQL 또는 모듈 구현
- [ ] Bayesian smoothing 또는 동등 보정 공식을 snapshot 계산에서 사용할지 결정
- [ ] snapshot source table 또는 materialized view 이름 확정
- [ ] snapshot 계산 SQL 또는 모듈 구현
- [ ] snapshot refresh cadence를 `10분`으로 구현
- [ ] snapshot에 raw count, smoothed CTR, recency 조합 방식 반영
- [ ] category/domain 편중 방지 규칙을 snapshot 계산 단계에 반영
- [ ] API에서 latest snapshot read + exclude filter만 수행하도록 전환
- [ ] `A1/A2/B/C` mix에서 C의 최소/최대 guardrail 확정
- [ ] snapshot 공백 또는 stale 시 fallback 규칙 구현

## 5. Fallback Matrix

아래 표는 목표 동작 기준이다. `Path C`는 snapshot 기반 path를 전제로 하며, 실패 시 `B+C` 또는 latest fallback으로 degrade 한다.

| 상황 | 기본 대응 |
| --- | --- |
| 온보딩 데이터 없음 | `A1 latest baseline + B` 사용 |
| 최근 행동 로그 부족 | `A2` 비활성화, `A1+B+C` 사용 |
| `A1` 실패 | `B` 중심 fallback |
| `A1/A2` 실패 | `B+C` 중심 fallback |
| `Path C` 최신 snapshot 없음 | 직전 snapshot 사용, 없으면 `B` 비중 확대 |
| `A1/A2/B/C` 모두 실패 | latest fallback |
| 캐시 장애 | in-memory cache 또는 latest fallback |

## 6. Post-Serving Analytics Follow-up

이 섹션은 추천 서빙 핵심 구현과 분리된 후속 운영/분석 작업이다. 현재 단계에서는 API가 집계 가능한 로그 필드를 남기는 것까지를 우선하고, 실제 CTR/latency 집계는 이후 `Airflow` 배치에서 수행하는 방향을 기준으로 둔다.

### 6.1 Aggregate Table First

- [x] CTR을 직접 저장하기보다 `news_id` 기준 count 집계를 먼저 두는 방향 확정
- [x] 집계 테이블은 total/path별 impression/click count를 모두 담는 방향 확정
- [x] `Path C`와 이후 MAB가 같은 집계 테이블을 재사용하는 방향 확정
- [ ] 집계 테이블을 wide table로 둘지 최종 확정
- [ ] `valid_dwell_count`를 같은 테이블에 둘지 별도 집계로 둘지 확정
- [x] MAB용 배치 집계 주기의 기본값은 `1시간`으로 고정

### 6.2 Evaluation Split By User State

- [x] cold/warm 트래픽을 분리해 CTR 비교 가능하도록 로그 적재
- [ ] total CTR과 path별 CTR을 집계 테이블 count로부터 계산 가능하게 구현
- [ ] `C CTR`과 snapshot freshness를 함께 집계
- [ ] `source_path` 기준 valid dwell rate를 path별로 집계
- [ ] global prior 계산용 path별 reward 집계를 배치에서 산출
- [ ] behavior path 사용 세션의 latency 영향 측정

## 7. Validation Plan

### API Tests

- `pytest tests/test_recommend_api.py`
- Docker 우선 실행:
  - `docker compose exec recommend-api python -m pytest tests/test_recommend_api.py`

### Required Scenarios

- cursor 재호출 시 같은 페이지가 유지되는지
- limit mismatch cursor가 400을 반환하는지
- `A1` 실패 시 `B` fallback이 동작하는지
- batch 소진 전에는 prefetch 결과가 섞이지 않는지
- batch rollover 후에는 prefetched queue가 이어지는지

### Validation Policy

- 추천 API 변경 검증은 가능하면 아래 순서를 따른다.
  1. `docker compose ps`로 `recommend-api`, `news-database`, `redis` 상태 확인
  2. `docker compose exec recommend-api python -m pytest tests/test_recommend_api.py`
  3. 필요 시 `docker compose exec recommend-api` 안에서 앱 import 또는 간단한 HTTP 확인
- 호스트 환경에 테스트 도구가 없다는 이유만으로 "검증 불가"로 결론내리지 않는다.
- 최종 응답에서 검증 불가를 적는 경우는 아래처럼 Docker 기준 검증도 실제로 막혔을 때만 해당한다.
  - `recommend-api` 컨테이너 미기동
  - 컨테이너 내부에 테스트 의존성 미설치
  - DB/Redis 등 필수 의존 서비스 미기동

## 8. Open Implementation Decisions

- prefetch 트리거를 primary queue 합산 기준으로만 둘지, path별로 세분화할지
- `user_id` 기반 사용자 신호 조회를 실시간 조회로 둘지, 일부 사전 집계 테이블로 둘지
- 집계 테이블을 wide table(`news_id` + total/path별 count 컬럼)로 둘지, long table(`news_id`, `path`, `metric`)로 둘지
- Path C snapshot 저장소를 일반 테이블, materialized view, key-value cache 중 무엇으로 둘지
- Path C snapshot cadence를 `10분` 고정으로 둘지, 운영 후 `5분` 또는 `30분`으로 조정할지
- `Path C` snapshot에서 raw count, smoothed CTR, recency를 어떤 비율로 섞을지
- pair type별 similarity calibration을 normalization으로 할지, 별도 weight matrix로 할지
- Max-Sim 점수와 entity hard match 점수 비율을 얼마로 둘지
- 최신성 decay 함수를 어떤 형태로 둘지
- A1/A2 점수 normalization을 retrieval 단계에서 할지, mixing 직전에 할지

## 9. Constants And Config

결정된 상수와 튜닝 대상은 한곳에 모아 관리하는 편이 낫다. 문서에는 아래 원칙으로 정리한다.

### 9.1 Spec Constants

- 제품/설계 차원에서 고정된 값은 이 문서에 명시한다.
- 예시
  - `A2 input window = 20`
  - `valid read dwell threshold = 5s`
  - `base pool = 3 days`
  - `pair-type calibration required`

### 9.2 Data Source Contract

- 추천 서버가 `user_id` 기준으로 직접 조회해야 하는 데이터 source는 이 문서 또는 별도 source map 문서에 정리한다.
- 최소 정리 항목
  - A1 온보딩 source
  - A2 최근 행동 로그 source
  - vector lookup source
  - recommendation aggregate source
  - C popularity snapshot source

#### A1 Onboarding Source

- `watchlist`
  - 목적: 사용자 관심 종목 조회
  - key: `user_id`
  - user_id type: `integer`
  - join key: `stock_id`
  - stock_id type: `char(6)`
  - note: `user_id`, `stock_id`는 FK
  - note: 임베딩 조인 시 `test_service_embeddings.entity_id = stock_id::varchar` and `entity_type = 'stock'`
- `user_onboarding_keywords`
  - 목적: 사용자 관심 키워드 조회
  - key: `user_id`
  - user_id type: `integer`
  - join key: `keyword_id`
  - keyword_id type: `integer`
  - note: `user_id`, `keyword_id`는 FK
  - note: 임베딩 조인 시 `test_service_embeddings.entity_id = keyword_id::varchar` and `entity_type = 'keyword'`

#### Vector Lookup Source

- `test_service_embeddings`
  - 목적: 키워드/종목 벡터 조회
  - columns: `entity_id`, `entity_type`, `display_name`, `gnn_embedding`, `model_version`
  - entity_type values: `news`, `keyword`, `stock`
  - key: unique(`entity_id`, `entity_type`)
  - note: FK가 없으므로 `entity_id + entity_type` 조합으로 조심해서 조회해야 한다
  - note: `entity_id` 타입이 `varchar(20)`이므로 `stock_id(char(6))`, `keyword_id(integer)`와 join 시 형변환 규칙을 명확히 해야 한다

#### A2 Behavior Source

- `interaction_events`
  - 목적: 최근 행동 로그 조회
  - key: `user_id`
  - core columns: `user_id`, `event_type`, `news_id`, `content_session_id`, `event_ts_client`, `event_ts_server`
  - event types: `content_open`, `content_leave`
  - note: 같은 `content_session_id`의 `content_open`과 `content_leave`를 묶어 dwell time을 계산한다
  - note: 기본 이벤트 시각은 `event_ts_client`를 사용하고, `event_ts_server`는 보조 검증용으로 사용한다
  - note: recommendation aggregate의 원천 이벤트도 가능한 한 이 테이블을 재사용하는 방향을 우선한다

#### News Entity Reconstruction Source

- `news_keyword_mapping`
  - 목적: `news_id -> keyword_id` 복원
  - columns: `news_id`, `keyword_id`
  - note: 모두 FK
- `news_stock_mapping`
  - 목적: `news_id -> stock_id` 복원
  - columns: `news_id`, `stock_id`
  - note: 모두 FK

#### Recommendation Aggregate Source

- aggregate의 원천 이벤트는 기존 `interaction_events`를 우선 사용한다.
- 별도 raw event table을 추가하기 전에 `interaction_events`에 recommendation 이벤트를 어떻게 적재할지 계약을 먼저 확정한다.
- recommendation aggregate를 위해 최소한 아래 필드 계약이 필요하다.
  - `event_type`
  - `news_id`
  - `user_id`
  - `user_id` type: `integer`
  - `request_id`
  - `position`
  - `event_ts_client` 또는 `event_ts_server`
  - `source_path`
- `source_path`는 `A1`, `A2`, `B`, `C` attribution을 위해 필요하다.
- 저장소명은 미정이지만, Airflow가 약 `10분`마다 `news_id` 기준 aggregate table을 갱신하는 것을 원칙으로 한다.
- 최소 필드
  - `news_id`
  - `snapshot_at`
  - `total_impression_count`
  - `total_click_count`
  - `a1_impression_count`
  - `a1_click_count`
  - `a2_impression_count`
  - `a2_click_count`
  - `b_impression_count`
  - `b_click_count`
  - `c_impression_count`
  - `c_click_count`
- 필요 시 `valid_dwell_count`, `window_start`, `window_end`, `last_event_at`를 추가할 수 있다.
- 이 테이블은 CTR 자체를 저장하기보다 CTR 계산 재료를 저장하는 것을 원칙으로 한다.
- `Path C` snapshot 계산과 이후 MAB reward 계산은 이 테이블을 공통 입력으로 사용한다.

#### Path C Snapshot Source

- 저장소명은 미정이지만, `Path C`는 Airflow가 약 `10분`마다 갱신하는 snapshot 저장소를 읽는 것을 원칙으로 한다.
- 최소 필드
  - `snapshot_at`
  - `news_id`
  - `score`
  - `rank`
  - `domain`
  - `category`
- 계산 입력 source는 `Recommendation Aggregate Source`, `naver_news`, `filtered_news` 등을 기준으로 한다.
- recency 및 domain/category diversity는 snapshot 계산 단계에서 반영한다.
- API는 latest snapshot 조회, exclude 적용, limit slice만 수행한다.

#### Pending Source Details

- `news_keyword_mapping.keyword_id`, `news_stock_mapping.stock_id`도 같은 방식으로 임베딩 테이블과 직접 연결하는지 최종 확인 필요

#### Debug Override Context Contract

- `context.version`
  - 기본값: `1`
  - 목적: debug override schema version
- `context.profile`
  - optional
  - 내부 `profile` 정규화 구조를 override
- `context.recent_actions`
  - optional
  - 내부 `recent_actions` 정규화 구조를 override
- `context.session_signals`
  - optional
  - session 단위 보조 신호를 debug 목적으로 override

### 9.3 Runtime Config

- 실제 코드에서 환경별로 바뀔 수 있는 값은 [config.py](/home/dobi/Crawling/app/core/config.py)에 둔다.
- 예시
  - candidate limit
  - mix weight
  - prefetch low watermark
  - cache TTL

### 9.4 Tuning Registry

- 아직 미결정이지만 실험으로 정해야 하는 값은 이 문서의 `Open Implementation Decisions`에 남긴다.
- 예시
  - Max-Sim score : entity boost 비율
  - decay 함수 형태
  - score normalization 방식
