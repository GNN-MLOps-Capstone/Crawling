# Recommendation API Implementation And Operations Draft

## 0. Document Scope

- 기준일: `2026-03-09`
- 이 문서는 추천 API에서 "무엇을 구현해야 하는가"를 단계별 체크리스트로 정리한 실행 문서다.
- 상위 목표와 아키텍처 배경은 [recommendation_api_design_draft.md](/home/dobi/Crawling/docs/recommendation_api_design_draft.md)를 기준으로 본다.
- 1단계는 우선 `context`가 비어 있어도 Path A와 LLM이 동작하는 Retrieval + LLM 뼈대 구축과, 이후 확정된 `context` 계약을 반영한 개인화 확장으로 나눈다.
- 2단계는 Popularity + MAB 확장으로 구분한다.

## 1. Current Baseline

### 현재 코드 상태

- 엔드포인트는 `POST /recommend/news`다.
- 현재 응답은 최신 뉴스 `news_id` 목록 중심의 mock 단계다.
- 요청마다 DB를 직접 조회한다.
- `context`는 아직 추천 계산에 실질적으로 반영되지 않는다.
- pagination은 `OFFSET` 기반 cursor 방식이다.
- cursor에는 `request_id`, `limit`, `offset`만 들어 있고, 추천 결과 리스트 자체를 캐시하지 않는다.
- `meta.fallback_used` 필드는 있지만, 실제 정책화된 fallback 경로는 아직 없다.
- Redis는 `docker-compose.yaml`에 정의되어 있지만 현재 추천 API 로직에는 연결되지 않았다.

### 현재 계약

- 요청 필드
  - `user_id`: 필수
  - `limit`: 필수, `1 <= limit <= 100`
  - `cursor`: optional
  - `request_id`: optional
  - `context`: optional dict, 현재 `null`은 허용하지 않음
- 응답 필드
  - `request_id`
  - `items[].news_id`
  - `next_cursor`
  - `meta.source`
  - `meta.fallback_used`

## 2. Phase 1A Checklist

1A 단계 목표는 "`context` 계약이 없어도 Path A와 LLM이 빈 입력 기준으로 동작하는 추천 서빙 뼈대"를 먼저 올리는 것이다. 이 단계에서는 `context`가 비어 있는 상태를 기본값으로 두고, 더 넓은 최신 풀을 가져오는 Path A, 더 짧은 시간창의 속보를 가져오는 Path B, LLM 적용 지점, 캐시, fallback, 로그 구조를 먼저 고정한다.

### 2.0 Skeleton-First Principles

- [x] `context`의 실제 값보다 `context`를 받아 처리하는 계층 구조를 먼저 고정
- [x] 추천 요청은 항상 `context builder -> retrieval -> rerank -> mix` 흐름을 타도록 유지
- [x] `context`가 비어 있어도 같은 함수와 같은 prompt builder를 사용하도록 설계
- [x] 온보딩 데이터와 행동 로그는 나중에 `context` 내부 포맷으로 투입할 수 있게 중간 정규화 포맷을 먼저 정의
- [x] 1A에서는 개인화 품질보다 계약 안정성과 확장 용이성을 우선

### 2.1 API Contract And Session

1A 구현 기준 기본 동작은 다음과 같다.
- `context`가 비어 있으면 `context builder`는 최신성 seed와 기본 prompt slot 문구를 생성한다.
- Path A는 최근 `3일` 최신 풀에서 후보를 만들고, rerank는 같은 prompt shape를 유지한 채 heuristic/LLM interface를 통해 실행한다.
- Path B는 더 짧은 속보 시간창에서 별도 후보를 만들고, 추천 세션은 `request_id` 기준 캐시에 저장된다.

- [x] 기존 `POST /recommend/news` 엔드포인트 유지
- [x] 응답 shape를 현재 계약과 호환되게 유지
- [x] `context`는 optional로 유지하되, 비어 있어도 정상 동작하는 계약으로 정리
- [x] `context` 미존재 시에도 Path A와 LLM이 실행되는 기본 동작을 문서화
- [x] 추천 세션은 `request_id` 단위로 path별 queue와 served state를 캐시에 보관하는 방식으로 정의
- [x] cursor는 캐시된 세션 상태에서 다음 page를 생성하기 위한 포인터 역할로 제한
- [x] `context` envelope 초안 정의
- [x] envelope는 `profile`, `recent_actions`, `session_signals` 수준의 상위 필드만 먼저 고정
- [x] 내부 세부 필드는 비어 있거나 누락되어도 통과되도록 처리
- [x] `meta.source` 값 체계 확정
- [x] `meta.fallback_used`와 `fallback_reason` 노출 범위 확정
- [x] cursor와 `request_id`의 순서 불변성 원칙 확정

### 2.2 Context Builder Skeleton

- [x] `raw context`를 내부 추천용 구조로 변환하는 전용 builder 계층 정의
- [x] builder는 비어 있는 입력에서도 최신순 기준 기본 seed를 반환하도록 구현
- [x] 라우터/서비스에서 `context`를 직접 해석하지 않고 builder를 통해서만 접근하도록 정리
- [x] 추후 온보딩 데이터, 클릭 로그, 세션 신호를 builder 입력으로 합성할 수 있게 확장 포인트 확보

### 2.3 Base Pool And Repository Expansion

- [x] 후보 조회 결과에 `pub_date` 추가
- [ ] 필요 시 `keyword_embedding_id` 또는 연관 조인 키 노출
- [x] 최근 `3일` 기준 base pool 쿼리 작성
- [x] base pool 기본 크기 확정
- [x] 최신 기사 정렬 기준 일관성 점검

### 2.4 Path A. Baseline Retrieval Without Context

- [x] 빈 `context` 기준 Path A 입력 생성 규칙 정의
- [x] `context`가 없을 때 Path A 후보는 더 넓은 최신 풀에서 최신순 기준으로 조회하도록 고정
- [x] Path A의 최신 풀 시간 범위와 base pool 관계 명확화
- [x] 최신순 baseline은 이후 온보딩/로그 기반 신호가 들어와도 기본 fallback으로 유지
- [x] 최신순 기준 후보 상한과 정렬 tie-break 규칙 구현
- [x] Path A 후보 수를 `50`개로 고정
- [x] seen item 필터 적용

### 2.5 Path B. Breaking News Retrieval

- [x] 최근 `1~2시간` 기사 추출 쿼리 구현
- [x] Path B는 Path A보다 더 짧은 시간창의 속보 풀로 역할을 고정
- [x] 최신성 중심 정렬 기준 확정
- [x] stale item cut-off 확정
- [x] Path B 후보 수를 `30`개로 고정

### 2.6 LLM Reranking Skeleton

- [x] 적용 대상을 초기에는 `50`개 규모의 Path A 후보 세트로 한정
- [x] 후보 압축 포맷 정의
- [x] 빈 `context`를 전제로 한 기본 prompt 계약 정의
- [x] `context`가 없어도 무의미한 출력이 아니라 일반 뉴스 추천 점수가 나오도록 prompt 작성
- [x] prompt는 `user interests`, `recent reads`, `avoidances` 같은 슬롯 구조로 먼저 설계
- [x] 값이 없을 때는 기본 문구를 넣어 prompt shape가 크게 바뀌지 않도록 유지
- [x] `relevance`, `novelty`, `clarity`, `final` 점수 파싱 구현
- [x] timeout 처리 구현
- [x] parse failure 처리 구현
- [x] partial failure 처리 구현
- [x] LLM 장애 시 heuristic 또는 원정렬 fallback 구현

### 2.7 Fixed Mixing And Serving

- [x] Path A와 Path B 병합 규칙 확정
- [x] Path A는 넓은 최신성, Path B는 속보성 보강 역할로 mix 정책 문서화
- [ ] 최종 mix는 MAB가 결정하고, 운영 문서에는 초기 prior와 guardrail만 정의
- [x] 중복 뉴스 제거 규칙 구현
- [x] path별 queue에서 현재 mix policy에 맞춰 page를 합성하는 규칙 확정
- [x] 한 번 생성된 queue와 served state는 세션 동안 재계산하지 않고 캐시 결과를 우선 사용
- [x] `meta.source` 출력 정책 반영

### 2.8 Caching, Consistency, And Fallback

- [x] Redis 연결
- [x] cache key 구조 확정
- [x] cache value 구조 확정
- [x] 같은 `request_id` 기준 session cache 저장
- [x] 세션 캐시에는 `A/B/C queue`, `served_ids`, `current_mix_policy`, `batch_generation_id`, 생성 시각, 마지막 노출 시각 저장
- [x] 다음 페이지 요청은 DB 재조회 대신 캐시된 path queue를 이용해 page를 합성하도록 구현
- [x] prefetch는 전체 잔량이 아니라 path별 low watermark 기준으로 트리거되도록 설계
- [x] 1A 기본 prefetch 조건은 `Path A remaining <= 20`
- [x] 필요 시 Path별 replenishment와 batch 전체 rollover 중 무엇을 우선할지 정책화
- [x] 현재 batch가 완전히 소진되면 prefetch된 다음 batch 또는 보충 queue로 전환하도록 구현
- [x] prefetch된 batch 생성 시 이미 노출한 `news_id`는 제외
- [x] Redis 장애 시 latest fallback 구현
- [x] 빈 `context` 상태에서도 Path A 실패 시 Path B 중심 fallback 구현
- [ ] DB 지연 또는 실패 시 stale cache fallback 검토

### 2.9 Logging And Evaluation Baseline

- 추천 로그는 별도 DB 적재 대신 애플리케이션 구조화 로그를 기준 저장 경로로 사용한다.

- [x] request log 스키마 확정
- [x] impression log 스키마 확정
- [x] click log 스키마 확정
- [x] `context` 원문 전체가 아니라 정규화 결과 또는 hash 중심으로 로그 적재
- [ ] 추후 `recent_actions`를 만들 수 있도록 request-impression-click 연결 키 유지
- [x] `session_id` 또는 동등한 세션 식별 키 적재 여부 확정
- [x] `latency_ms`, `cache_status`, `fallback_reason` 구조화 로그 추가
- [x] `prefetch_triggered`, `prefetch_status`, `batch_generation_id` 로그 추가
- [x] path별 remaining count와 prefetch trigger path 로그 추가
- [x] `context_present` 또는 동등한 personalizable 상태 플래그 로그 추가
- [ ] `CTR@5/10/20`, `Latency p50/p95`, `Fallback Rate` 계산 가능 여부 확인
- [x] Path A/B mix ratio 로그 적재

## 3. Phase 1B Checklist

1B 단계 목표는 "확정된 `context` 계약을 Path A retrieval과 LLM prompt에 반영해 개인화 강도를 높이는 것"이다. 이 단계부터는 1A에서 만든 기본 Path A/LLM 뼈대를 유지한 채, `context`가 있을 때만 추가 신호와 boost를 얹고 없을 때는 1A 기본 모드로 그대로 동작하도록 확장한다.

### 3.1 Context Contract Finalization

- [ ] `context` 허용 필드 목록 확정
- [ ] 필드별 optional/required 여부와 기본값 확정
- [ ] `context` 버전 관리 또는 schema evolution 방식 결정
- [ ] 비어 있는 `context`, 부분적으로만 채워진 `context` 처리 규칙 확정
- [ ] `context` 검증 실패 시 degrade 정책 확정
- [ ] 온보딩 데이터와 로그 데이터가 각각 어떤 필드로 매핑되는지 확정

### 3.2 Path A. Personalized Retrieval

- [ ] 개인화 입력 파싱 규칙 확정
- [ ] 온보딩 데이터 source 연결
- [ ] 최근 읽은 기사 기반 입력 source 연결
- [ ] 노출/클릭 로그를 `recent_actions` 또는 동등 구조로 집계하는 경로 확정
- [ ] 뉴스 키워드/종목 임베딩 조회 경로 확정
- [ ] 1A 기본 Path A 입력 위에 `context` 신호를 누적하는 방식으로 확장
- [ ] `context` 기반 필터 또는 boost 규칙 추가
- [ ] 유사도 기반 후보 추출 구현
- [ ] Path A 후보 수를 `50`개로 유지
- [ ] seen item 필터 적용

### 3.3 LLM Prompt Personalization

- [ ] Path A 후보 압축 포맷에 `context` 반영 방식 추가
- [ ] `context` 주입 prompt 템플릿 확정
- [ ] `context` 없음/부분 입력 시 1A 기본 prompt로 degrade 되도록 구현
- [ ] 온보딩 선호와 행동 로그를 prompt 슬롯에 어떻게 우선순위 반영할지 확정
- [ ] 개인화 점수와 기본 점수 결합 규칙 확정
- [ ] prompt 길이 상한과 truncation 규칙 확정

### 3.4 Serving And Fallback Extension

- [ ] `context` 존재 시 Path A + Path B 병합 규칙 확정
- [ ] `context` 부재 시에도 Path A + Path B 기본 모드가 유지되도록 구현
- [ ] 개인화 경로 실패 시 Path B 중심 fallback 구현
- [ ] personalized batch도 세션 캐시와 prefetch 규칙을 동일하게 따르도록 유지
- [ ] `meta.source`에 baseline/personalized source 구분 반영
- [ ] `fallback_reason`에 `context_missing`, `context_invalid` 등 코드 체계 반영

### 3.5 Evaluation Split By Context Availability

- [ ] `context` 있음/없음 트래픽을 분리해 CTR 비교 가능하도록 로그 적재
- [ ] personalized path 사용 세션의 latency/timeout 영향 측정
- [ ] `context` 없음 세션에서 품질 저하 없이 1A baseline 유지 확인

## 4. Phase 2 Checklist

2단계 목표는 "인기 탐색 경로와 MAB를 붙여 mix ratio를 동적으로 조정하는 것"이다.

### 4.1 Path C. Popular Exploration Retrieval

- [ ] CTR 또는 대체 popularity signal source 확정
- [ ] 인기 기사 조회 쿼리 구현
- [ ] 카테고리 편중 방지 기준 확정
- [ ] 초기 로그 부족 시 popularity fallback rule 구현

### 4.2 MAB Reward Loop

- [ ] Path별 클릭 보상 정의
- [ ] reward 집계 단위 확정
- [ ] 클릭 로그에서 Path attribution 유지
- [ ] MAB 알고리즘 초기안 확정
- [ ] MAB의 초기 prior, 최소/최대 guardrail, 비활성화 시 fallback ratio 정의
- [ ] 경로별 mix ratio 동적 조정 구현

### 4.3 Serving Integration

- [ ] Path C를 최종 후보 병합에 포함
- [ ] `A/B/C` 최종 mix는 MAB가 결정하고, 수동 고정 비율은 fallback 용도로만 유지
- [ ] exploration slot 성능 모니터링 추가
- [ ] MAB 비활성화 시 고정 비율 fallback 준비

## 5. Fallback Matrix

| Failure Case | Recommended Behavior |
| --- | --- |
| Redis 장애 | DB/직계산 기반 최신 결과로 degrade |
| 개인화 입력 없음 | 넓은 최신 풀의 Path A와 속보 풀의 Path B를 함께 서빙 |
| `context` 계약 미확정 | 1A 범위로 고정하고 최신순 기준 Path A/LLM을 사용 |
| `context` 검증 실패 | 1A 기본 prompt/retrieval로 degrade하고 `fallback_reason` 기록 |
| prefetch 실패 | 현재 batch를 끝까지 사용하고, 소진 시 동기 재생성 또는 정책 fallback |
| Path A retrieval 실패 | Path B 중심 mix, 2단계부터는 Path B/C 중심 mix |
| LLM timeout 또는 parse 실패 | Path A 원정렬 또는 heuristic 정렬 사용 |
| popularity 집계 부족 | latest 기반 탐색 슬롯 대체 |
| DB 지연/실패 | stale cache 우선, 없으면 정책 fallback |

## 6. Required Log Fields

- request log 최소 필드
  - `timestamp`
  - `request_id`
  - `user_id`
  - `session_id`
  - `limit`
  - `context_hash`
  - `context_present`
  - `context_version`
  - `latency_ms`
  - `cache_status`
  - `batch_generation_id`
  - `prefetch_triggered`
  - `prefetch_status`
  - `prefetch_path`
  - `remaining_a`
  - `remaining_b`
  - `remaining_c`
  - `fallback_used`
  - `fallback_reason`
  - `path_mix`
  - `mix_policy_source`
  - `llm_used`
- impression log 최소 필드
  - `request_id`
  - `impression_id`
  - `user_id`
  - `news_id`
  - `rank`
  - `path_source`
  - `score_final`
  - `served_at`
  - `experiment_bucket`
- 클릭 로그 최소 필드
  - `request_id`
  - `impression_id`
  - `news_id`
  - `clicked_at`
  - `path_source`

## 7. Delivery Plan

아래 일정은 `2026-03-09` 기준으로 다시 잡은 5주 계획이다.

| 기간 | 목표 |
| --- | --- |
| `2026-03-09` ~ `2026-03-15` | Phase 1A 계약 정리, 애플리케이션 로그 기반 로그/평가 스키마, base pool 및 repository 확장 |
| `2026-03-16` ~ `2026-03-22` | Phase 1A baseline Path A retrieval, Path B, LLM skeleton, 세션 캐시 구현 |
| `2026-03-23` ~ `2026-03-29` | Phase 1B context 계약 반영, Path A personalization boost, personalized prompt 확장 |
| `2026-03-30` ~ `2026-04-05` | Path C 구현, MAB reward loop, 통합 E2E |
| `2026-04-06` ~ `2026-04-12` | QA, fallback 검증, 릴리즈 준비 |

## 8. Validation Plan

### API And Logic Tests

- `context`가 비어 있어도 Path A + Path B 결과가 정상 반환되는지
- `context`가 비어 있거나 미확정이어도 API 계약이 깨지지 않는지
- `context builder`가 빈 입력, 부분 입력, 잘못된 입력에서 모두 안정적으로 동작하는지
- Phase 1A에서 빈 `context` 기준 Path A가 `50`개 최신 후보를, Path B가 `30`개 속보 후보를 안정적으로 확보하는지
- cursor pagination이 캐시된 path queue와 served state 기준으로 안정적으로 다음 page를 생성하는지
- `Path A remaining <= 20`일 때 prefetch가 트리거되는지
- 전체 잔량이 많아도 특정 path가 병목이면 prefetch가 트리거되는지
- 현재 batch 소진 후 다음 batch 또는 보충 queue로 자연스럽게 전환되는지
- Phase 1B에서 `context`가 있을 때 Path A가 추가 개인화 신호를 반영하는지
- Phase 2에서 Path C가 최소 후보 수를 확보하는지
- 차단 도메인과 seen item이 필터링되는지
- Path 병합 시 중복 뉴스가 제거되는지
- cursor pagination 시 동일 `request_id` 기준 순서가 유지되는지

### LLM And Fallback Tests

- LLM timeout 시 응답이 실패하지 않고 degrade 되는지
- LLM 응답 파싱 실패 시 fallback reason이 기록되는지
- `context` 검증 실패 시 1A 기본 prompt/retrieval로 안전하게 degrade 되는지
- prefetch 실패 시 현재 batch 서빙이 유지되고, 소진 시 정책 fallback이 동작하는지
- Redis 미사용 또는 장애 상태에서도 기본 응답이 가능한지

### Evaluation Readiness

- `CTR@5/10/20`
- `First Click Rate`
- `Mix Ratio(A/B/C)`
- `Mix Policy Source(MAB/Prior/Fallback)`
- `Path-wise CTR`
- `Latency p50/p95`
- `Cache Hit Rate`
- `Fallback Rate`
- `LLM Timeout/Error Rate`
- 온보딩 필드와 행동 로그를 `context`로 재구성 가능한지

## 9. Open Technical Decisions

- `context` 계약을 요청 payload에 어느 수준까지 직접 둘지
- `context builder`를 요청 경로에서만 계산할지, 미리 집계된 사용자 상태를 읽어올지
- 다음 batch를 동기 생성과 비동기 prefetch 중 어떤 우선순위로 둘지
- Path별 replenishment와 batch 전체 rollover 중 어느 전략을 우선할지
- MAB 초기 prior와 guardrail을 어느 수준으로 둘지
- 온보딩 데이터의 최소 수집 항목을 어디까지 둘지
- 클릭만 볼지, 노출과 체류까지 함께 `recent_actions`에 넣을지
- Path A 임베딩 저장 형식을 어디에 둘지
- keyword 메타를 어떤 테이블 조합에서 가장 안정적으로 읽을지
- popularity source가 준비되기 전 Path C를 어떤 규칙으로 대체할지
- LLM 호출을 요청 경로에서 동기 처리할지, 사전 계산 또는 배치 캐시와 섞을지
- MAB의 최소 구현을 epsilon-greedy로 시작할지, Thompson Sampling까지 바로 갈지
- request log와 click log의 익명화 수준을 어디까지 강제할지
