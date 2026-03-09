# Recommendation API Implementation And Operations Draft

## 0. Document Scope

- 기준일: `2026-03-09`
- 이 문서는 추천 API에서 "무엇을 구현해야 하는가"를 단계별 체크리스트로 정리한 실행 문서다.
- 상위 목표와 아키텍처 배경은 [recommendation_api_design_draft.md](/home/dobi/Crawling/docs/recommendation_api_design_draft.md)를 기준으로 본다.
- 현재 구현 단계는 `LLM Ranker`를 제거하고, `A1(온보딩) / A2(로그) / B(속보) / C(인기 탐색)` 구조를 기준으로 재정렬하는 것이다.

## 1. Current Baseline

### 현재 코드 상태

- 엔드포인트는 `POST /recommend/news`다.
- 응답은 `news_id` 목록과 cursor를 반환한다.
- 요청마다 세션 캐시를 우선 확인하고, miss 시 추천 세션을 새로 생성한다.
- 추천 계산에 필요한 사용자 신호는 외부 `context`보다 `user_id` 기준 내부 조회를 기본으로 하도록 정리 중이다.
- 현재 구현 기준 기본 동작은 `온보딩 기반 후보 + 속보 후보` 믹싱이다.
- `로그 기반 후보(A2)`는 세션/행동 신호를 받는 구조까지 우선 반영하고, 고도화는 후속 단계로 둔다.
- `인기 탐색 후보(C)`는 아키텍처 상 유지되지만 현재 코드에는 아직 활성화되지 않았다.

### 현재 계약

- 요청 필드
  - `user_id`: 필수
  - `limit`: 필수, `1 <= limit <= 100`
  - `cursor`: optional
  - `request_id`: optional
- 응답 필드
  - `request_id`
  - `items[].news_id`
  - `next_cursor`
  - `meta.source`
  - `meta.fallback_used`

## 2. Phase 1A Checklist

1A 단계 목표는 "`user_id` 기반 내부 신호 조회를 전제로 `A1(온보딩)`과 `B(속보)`를 먼저 안정화하고, `C(인기 탐색)`를 이후에 붙일 수 있는 멀티 패스 서빙 뼈대"를 올리는 것이다.

### 2.1 Progress Snapshot

- 완료
  - 추천 요청이 `context builder -> retrieval -> mix -> session cache` 흐름으로 동작한다.
  - `POST /recommend/news` 계약, cursor pagination, 세션 캐시 기반 page slice가 유지된다.
  - `A1(온보딩 latest baseline)`와 `B(속보)` 후보를 섞어 반환하는 기본 서빙 경로가 있다.
  - prefetch, batch rollover, latest fallback, `A1 실패 -> B fallback`이 코드에 반영돼 있다.
  - `user_id` 기준 내부 온보딩/행동 신호 조회 스켈레톤이 코드에 반영돼 있다.
  - `meta.source`가 `multipath_cold` / `multipath_warm` 수준으로 구분된다.
  - Docker 기준 추천 API 테스트가 통과했다.
- 부분 완료
  - 세션 구조는 이미 멀티 패스 확장을 전제로 하지만, 현재 코드 레벨 큐는 `onboarding/behavior/breaking`까지만 저장한다.
  - 로그 필드는 남아 있지만, `impression/click -> recent_actions` 데이터 루프는 아직 닫히지 않았다.
  - `Path C`는 설계상 유지되지만 구현과 세션 구조 반영은 아직 안 됐다.
  - 요청 계약은 아직 `context` optional 형태를 유지하지만, 기본 경로는 `user_id` 기반 내부 조회이고 `context`는 debug override 용도로만 쓰인다.
- 미완료
  - 1A 완료 기준 전부를 만족하는 end-to-end 검증은 아직 아니다.
  - `user_id -> profile/log source lookup` 뒤 personalized retrieval scoring 연결은 아직 없다.

### 2.2 Request Flow And Contract

- [x] 기존 `POST /recommend/news` 엔드포인트 유지
- [x] 응답 shape를 현재 계약과 호환되게 유지
- [x] `request_id` 단위 추천 세션 생성 및 재사용
- [x] cursor는 세션 포인터 역할로만 사용
- [x] cursor/request_id 불일치 시 400 에러 반환
- [x] `meta.source`, `meta.fallback_used`, `fallback_reason` 응답 유지
- [ ] 요청 계약을 `user_id` 중심 최소 입력 형태로 정리
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
- [ ] 온보딩 프로필 기반 boost 규칙은 아직 미구현
- [ ] 내부 조회한 프로필/로그를 retrieval 입력으로 연결

### 2.5 Path A1. Onboarding Baseline

- [x] `profile`이 없어도 넓은 최신 풀에서 baseline 후보 생성
- [x] A1 후보 수 상한 유지
- [x] exclude/seen item 필터 적용
- [x] 온보딩 입력은 선택한 키워드/종목 전체를 그대로 사용하는 방향 확정
- [x] 벡터 매칭은 average pooling이 아니라 Max-Sim 기반으로 정의
- [x] 종목 entity hard match 가산점 적용 방향 확정
- [ ] pair type별 similarity calibration 구현
- [ ] 실제 온보딩 신호를 반영한 personalized retrieval은 아직 미구현
- [x] `watchlist(user_id, stock_id)` source 확인
- [x] `user_onboarding_keywords(user_id, keyword_id)` source 확인
- [x] `user_id -> onboarding source` join 구현
- [ ] vector lookup 구현

### 2.6 Path B. Breaking Baseline

- [x] 짧은 시간창의 속보 후보 조회 유지
- [x] `stale_cutoff`를 적용할 수 있게 유지
- [x] B 후보 수 상한 유지
- [x] A1과 중복되지 않도록 exclude 처리
- [x] primary path 실패 시 B 중심 fallback 지원

### 2.7 Path C. Popular Exploration Skeleton

- [x] Path C를 정식 경로로 문서에 유지
- [ ] popularity signal source 확정
- [ ] recency와 category diversity를 함께 반영한 점수 정의
- [ ] 인기 편중 방지 규칙 정의
- [ ] 세션 캐시에 popular queue를 저장할지 구조 확정

### 2.8 Session Cache And Pagination

- [x] 세션 캐시에 `queue`, `served_ids`, `batch_generation_id` 저장
- [x] 다음 페이지 요청은 캐시된 timeline에서 slice
- [x] prefetch 결과는 현재 batch 소진 전까지 본 timeline에 합치지 않음
- [x] batch rollover 시 prefetched queue를 이어 붙임
- [x] 필요 시 세션 재생성 fallback 지원
- [ ] stale session 복원 정책은 아직 미정
- [ ] Path C 활성화 시 popular queue 저장 구조 확장

### 2.9 Mixing Baseline

- [x] 현재 구현은 고정 mix weight 기반 병합
- [x] path별 큐를 따로 보관하고 최종 timeline은 mix 결과로 생성
- [x] 중복 뉴스 제거 규칙 유지
- [x] MAB 없이도 동작하는 기본 서빙 경로 확보
- [ ] 1A 기준 운영 guardrail 문구는 더 구체화 필요

### 2.10 Logging And Observability

- [x] request log 스키마 유지
- [x] `latency_ms`, `cache_status`, `fallback_reason`, `batch_generation_id` 로그 유지
- [x] path별 remaining count와 mix ratio 로그 적재
- [x] `context_hash`, `context_present` 로그 적재
- [ ] impression/click 로그 실제 적재 경로 검증 필요
- [ ] `cold/warm` 분리 집계 기준은 아직 미확정
- [ ] Path C impression/click 집계 필드 정의

### 2.11 Validation Status

- [x] 테스트 코드 기준 cursor consistency, fallback, prefetch 시나리오를 작성함
- [x] `pytest tests/test_recommend_api.py` 실행 확인
- [ ] FastAPI 앱 레벨 수동 확인
- [ ] Redis 연결이 실제 로컬 환경에서 정상 동작하는지 확인

## 3. Phase 1B Checklist

1B 단계 목표는 "`A2(로그 기반 추천)`를 활성화하고, warm user 대상 개인화 강도를 높이는 것"이다.

### 3.1 Context Contract Finalization

- [ ] `context` 허용 필드 목록 확정
- [ ] 필드별 optional/required 여부와 기본값 확정
- [ ] `context` 버전 관리 또는 schema evolution 방식 결정
- [ ] 온보딩 데이터와 로그 데이터가 각각 어떤 필드로 매핑되는지 확정

### 3.2 Path A2. Behavior-Based Retrieval

#### Spec Locked

- [x] `recent_actions`는 `user_id` 기준 최근 20개 클릭/유효 읽기 로그를 기준으로 한다
- [x] 체류 시간 `10초` 미만 로그는 제외한다
- [x] 유니크 키워드 단위로 집계하되 빈도 정보는 유지한다

#### Implementation Checklist

- [x] `user_id -> recent_actions` 조회 경로 확정
- [x] `interaction_events`에서 `content_view/content_leave`와 `content_session`으로 읽기 세션을 복원하기로 결정
- [ ] warm user 활성화 기준 확정
- [x] `content_view`와 `content_leave`를 묶어 dwell time 계산 구현
- [ ] `event_ts_client` 기준 최신성 정렬 및 decay 입력 시각 처리 구현
- [ ] 빈도 + 최신성 decay 가중치 계산 구현
- [ ] pair type별 similarity calibration 구현
- [ ] Max-Sim 매칭 구현
- [ ] 반복 등장 종목에 대한 entity hard match 구현
- [x] `news_keyword_mapping`, `news_stock_mapping`으로 엔터티 복원 구현
- [x] `interaction_events -> recent_actions` 정제 구현
- [ ] 행동 로그 기반 후보 추출 구현
- [ ] A2 후보 수 상한 유지
- [ ] seen item 필터 적용

### 3.3 Mixing Extension

- [ ] `A1/A2/B/C` 병합 규칙 확정
- [ ] `A2` 비활성화 시 `A1+B+C`로 자동 degrade
- [x] `meta.source`에 cold/warm source 구분 반영
- [ ] `fallback_reason`에 `behavior_insufficient`, `profile_missing`, `user_signal_lookup_failed` 등 코드 체계 반영

### 3.4 Evaluation Split By User State

- [ ] cold/warm 트래픽을 분리해 CTR 비교 가능하도록 로그 적재
- [ ] `A1 CTR`, `A2 CTR`, `B CTR`을 따로 집계
- [ ] behavior path 사용 세션의 latency 영향 측정

## 4. Phase 2 Checklist

2단계 목표는 "`MAB`를 붙여 `A1/A2/B/C` mix ratio를 동적으로 조정하는 것"이다.

### 4.1 MAB Introduction

- [ ] MAB의 arm을 `A1`, `A2`, `B`, `C`로 정의
- [ ] reward 정의를 click 중심으로 확정
- [ ] cold user와 warm user의 bandit 상태를 분리할지 결정
- [ ] 최소 노출 비율 guardrail 확정
- [ ] 충분한 로그 전까지는 fixed mix와 병행 가능한 모드 준비

### 4.2 Popular Exploration Path C

- [ ] CTR 또는 대체 popularity signal source 확정
- [ ] recency 조합 방식 확정
- [ ] category/domain 편중 방지 규칙 확정
- [ ] `A1/A2/B/C` mix에서 C의 최소/최대 guardrail 확정

## 5. Fallback Matrix

아래 표는 `target state` 기준이다. 현재 구현 baseline은 `A1 + B` 중심이며, `C`는 아직 코드에 활성화되지 않았다.

| 상황 | 기본 대응 |
| --- | --- |
| 온보딩 데이터 없음 | `A1 latest baseline + B` 사용 |
| 최근 행동 로그 부족 | `A2` 비활성화, `A1+B+C` 사용 |
| `A1` 실패 | `B` 중심 fallback |
| `A1/A2` 실패 | `B+C` 중심 fallback |
| `A1/A2/B/C` 모두 실패 | latest fallback |
| 캐시 장애 | in-memory cache 또는 latest fallback |

## 6. Validation Plan

### API Tests

- `pytest tests/test_recommend_api.py`

### Required Scenarios

- cursor 재호출 시 같은 페이지가 유지되는지
- limit mismatch cursor가 400을 반환하는지
- `A1` 실패 시 `B` fallback이 동작하는지
- batch 소진 전에는 prefetch 결과가 섞이지 않는지
- batch rollover 후에는 prefetched queue가 이어지는지

## 7. Open Implementation Decisions

- prefetch 트리거를 primary queue 합산 기준으로만 둘지, path별로 세분화할지
- MAB를 FastAPI 프로세스 내부 상태로 둘지, 별도 저장소를 둘지
- `user_id` 기반 사용자 신호 조회를 실시간 조회로 둘지, 일부 사전 집계 테이블로 둘지
- Path C를 1B에서 세션 구조까지 먼저 넣을지, 2단계에서 retrieval부터 붙일지
- pair type별 similarity calibration을 normalization으로 할지, 별도 weight matrix로 할지
- Max-Sim 점수와 entity hard match 점수 비율을 얼마로 둘지
- 최신성 decay 함수를 어떤 형태로 둘지
- A1/A2 점수 normalization을 retrieval 단계에서 할지, mixing 직전에 할지

## 8. Constants And Config

결정된 상수와 튜닝 대상은 한곳에 모아 관리하는 편이 낫다. 문서에는 아래 원칙으로 정리한다.

### 8.1 Spec Constants

- 제품/설계 차원에서 고정된 값은 이 문서에 명시한다.
- 예시
  - `A2 input window = 20`
  - `valid read dwell threshold = 10s`
  - `base pool = 3 days`
  - `pair-type calibration required`

### 8.2 Data Source Contract

- 추천 서버가 `user_id` 기준으로 직접 조회해야 하는 데이터 source는 이 문서 또는 별도 source map 문서에 정리한다.
- 최소 정리 항목
  - A1 온보딩 source
  - A2 최근 행동 로그 source
  - vector lookup source
  - C popularity source

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
  - core columns: `user_id`, `event_type`, `news_id`, `content_session`, `event_ts_client`, `event_ts_server`
  - event types: `content_view`, `content_leave`
  - note: 같은 `content_session`의 `content_view`와 `content_leave`를 묶어 dwell time을 계산한다
  - note: 기본 이벤트 시각은 `event_ts_client`를 사용하고, `event_ts_server`는 보조 검증용으로 사용한다

#### News Entity Reconstruction Source

- `news_keyword_mapping`
  - 목적: `news_id -> keyword_id` 복원
  - columns: `news_id`, `keyword_id`
  - note: 모두 FK
- `news_stock_mapping`
  - 목적: `news_id -> stock_id` 복원
  - columns: `news_id`, `stock_id`
  - note: 모두 FK

#### Pending Source Details

- `Path C` popularity source는 아직 미정
- `news_keyword_mapping.keyword_id`, `news_stock_mapping.stock_id`도 같은 방식으로 임베딩 테이블과 직접 연결하는지 최종 확인 필요

### 8.3 Runtime Config

- 실제 코드에서 환경별로 바뀔 수 있는 값은 [config.py](/home/dobi/Crawling/app/core/config.py)에 둔다.
- 예시
  - candidate limit
  - mix weight
  - prefetch low watermark
  - cache TTL

### 8.4 Tuning Registry

- 아직 미결정이지만 실험으로 정해야 하는 값은 이 문서의 `Open Implementation Decisions`에 남긴다.
- 예시
  - Max-Sim score : entity boost 비율
  - decay 함수 형태
  - score normalization 방식
