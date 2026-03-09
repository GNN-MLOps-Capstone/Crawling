# Recommendation API Product And Architecture Draft

## 0. Document Scope

- 기준일: `2026-03-09`
- 이 문서는 추천 API에서 "무엇을 만들 것인가"를 정리하는 상위 스펙 문서다.
- 구현 작업, 테스트 순서, 운영 체크리스트는 [recommendation_api_operations_draft.md](/home/dobi/Crawling/docs/recommendation_api_operations_draft.md)에서 관리한다.
- 현재 코드와 문서가 다르면, 구현 전까지의 실제 동작은 코드 기준으로 본다.

## 1. Background And Problem

- 기존 구상은 멀티 패스 리트리벌 뒤에 `LLM Ranker`를 두는 하이브리드 구조였다.
- 하지만 현재 단계에서 핵심 리스크는 재정렬 정밀도보다도 `cold start 처리`, `행동 로그 루프`, `세션 일관성`, `fallback 안정성`이다.
- 따라서 v1은 LLM 재정렬을 제거하고, 추천 후보 경로를 더 분명하게 나눈 뒤, 서빙 단계에서 경로 믹싱과 데이터 루프를 먼저 안정화하는 쪽으로 정리한다.
- 새 방향은 `온보딩 기반 추천`, `로그 기반 추천`, `최신 속보`, `인기 탐색`을 병렬로 만들고, 이후 `MAB`가 이를 동적으로 믹싱하는 구조다.

## 2. Product Goal

### 현재 상태

- 엔드포인트는 `POST /recommend/news`다.
- FastAPI 추천 API는 이미 존재한다.
- 현재는 개인화 없이 최신 뉴스 `news_id`를 cursor 기반으로 반환하는 mock 단계에서 확장 중이다.
- 세션 캐시, fallback, 구조화 로그는 들어가고 있으나 경로 정의와 품질 측정은 더 정리할 필요가 있다.

### v1 목표

- v1의 목표는 "실제로 서빙 가능한 멀티 패스 추천 파이프라인"이다.
- 추천 응답은 아래 성격의 후보를 함께 다뤄야 한다.
  - 온보딩 기반 추천
  - 로그 기반 추천
  - 최신 속보 후보
  - 인기 탐색 후보
- 추천 계산에 필요한 사용자 신호는 외부에서 완성된 `context`를 전달받기보다, 추천 서버가 `user_id` 기준으로 내부 DB/로그 저장소를 조회해 조합하는 방식을 기본으로 한다.
- 최종 서빙은 단일 최신순이 아니라 경로별 후보를 혼합하는 구조를 가져야 한다.
- 특히 개인화 경로인 `A1`, `A2`는 관심사를 평균 내어 희석하지 않고, 다중 관심사를 보존하는 구조를 가져야 한다.
- 신규 사용자와 warm user 모두 자연스러운 결과를 받아야 한다.

### 단계별 목표

- 1단계
  - 1A. `온보딩 기반 경로(A1)`와 `최신 속보 경로(B)` 구축
  - 1A. 세션 캐시와 cursor slice 구조 구축
  - 1A. 구조화 로그와 fallback 체계 고정
  - 1B. `로그 기반 경로(A2)` 추가
  - 1B. `user_id` 기준 온보딩/행동 로그 내부 조회 경로 고정
- 2단계
  - `MAB`를 `A1/A2/B/C` 믹싱에 도입
  - `Path C(인기 탐색)` 품질과 가드레일 보강
  - 클릭 로그 기반 보상 계산과 guardrail 운영

### v1 비목표

- LLM 기반 재정렬 서빙
- 대규모 행동 로그를 전제로 한 GNN 학습 파이프라인
- LightGBM 기반 학습 랭커 서빙
- 완성형 MLOps 자동화
- 고도화된 장기 사용자 프로파일링

## 3. Target Recommendation Architecture

### 3.1 Stage 1. Base Pool

- 목적: 전체 뉴스에서 추천 연산 대상이 될 최신 기사 집합을 먼저 좁힌다.
- 기본 방향
  - 최근 며칠 이내 기사만 대상으로 한다.
  - 초기 구현에서는 최근 `3일` 이내 기사 약 `3,000`건을 기준 pool로 본다.
- 기대 효과
  - 리트리벌 비용을 줄인다.
  - stale item 비중을 제어한다.

### 3.2 Stage 2. Multi-Path Retrieval

- 목적: 서로 다른 추천 목적을 가진 후보 바구니를 병렬로 만든다.

| Path | 목적 | 기본 로직 | 데이터 소스 |
| --- | --- | --- | --- |
| Path A1 | 온보딩 기반 추천 | 가입 시 선택한 관심사(키워드/종목) 집합을 query로 사용하고, 평균 대신 Late Interaction(Max-Sim)으로 매칭 | RDBMS |
| Path A2 | 로그 기반 추천 | 최근 20개 클릭/유효 읽기 로그의 키워드/종목에 대해 시간 감쇠와 Late Interaction(Max-Sim)을 적용 | RDBMS |
| Path B | 최신 속보 | 짧은 시간창의 속보 기사 우선 추출 | RDBMS |
| Path C | 인기 탐색 | CTR, 최근성, 편중 제어를 함께 반영한 인기 기사 후보를 생성 | RDBMS + 집계 로그 |

- `Path C`는 기존 멀티 패스 구조의 정식 경로로 유지한다.
- 1A 초기 구현은 `A1`과 `B`를 우선 올리고, `A2`와 `C`는 내부 데이터 조회 경로와 큐 구조를 먼저 고정한 뒤 후속 단계에서 활성화한다.
- `A1`은 cold start 대응의 기본 exploitation 경로다.
- `A2`는 행동 데이터가 충분한 사용자에게 우선 적용되는 warm path다.
- `B`는 freshness 보강과 탐색 역할을 맡는다.
- `C`는 인기 기반 exploration을 담당하되, recency와 카테고리 편중 제어를 함께 가져가야 한다.

#### User Signal Loading Principle

- 추천 서버는 `user_id`를 기준으로 필요한 사용자 신호를 직접 조회한다.
- 외부 요청의 기본 계약은 최소한의 서빙 파라미터만 유지한다.
  - `user_id`
  - `limit`
  - `cursor`
  - `request_id`
- `profile`, `recent_actions`, `session_signals`는 외부 입력의 필수 계약이 아니라 추천 서버 내부 정규화 결과로 본다.
- 필요 시 디버깅/override 용 보조 입력은 둘 수 있지만, 기본 경로는 `user_id -> profile/log lookup -> normalized user signals -> retrieval`이다.
- 초기 source map은 아래를 기준으로 한다.
  - 온보딩 종목: `watchlist(user_id, stock_id)`
  - 온보딩 키워드: `user_onboarding_keywords(user_id, keyword_id)`
  - 벡터 조회: `test_service_embeddings(entity_id, entity_type, gnn_embedding)`
  - 행동 로그: `interaction_events(user_id, event_type, news_id, content_session, event_ts_client, ...)`
  - 뉴스-엔터티 매핑: `news_keyword_mapping(news_id, keyword_id)`, `news_stock_mapping(news_id, stock_id)`

#### 상세 스펙: A1 & A2 Logic Definition

- 공통 벡터 전략
  - `No Average Pooling`: 유저의 관심사 벡터들을 하나로 평균 내지 않는다.
  - `Late Interaction / Max-Sim`: 유저가 가진 여러 관심사 벡터 각각에 대해, 후보 기사의 키워드 벡터 중 가장 유사한 벡터를 찾아 점수를 매긴 뒤 합산한다.
  - `Entity Boosting`: 벡터 유사도와 별개로 종목 코드가 일치하면 hard match 가산점을 부여한다.
  - `Type-Aware Similarity Calibration`: `keyword-keyword`, `stock-stock`, `keyword-stock` 쌍은 raw 유사도 분포가 다를 수 있으므로, 동일 스케일로 바로 합산하지 않는다. pair type별 normalization 또는 weight를 적용해 점수를 보정한다.
  - `Diversity Guardrail`: Max-Sim 점수가 높더라도 동일 종목이나 동일 테마로 결과가 과도하게 몰리지 않도록, serving 단계에서 동일 종목/카테고리 반복 노출 상한을 둔다.
- Path A1
  - `Input`: `user_id` 기준으로 조회한 온보딩/설정의 키워드 및 종목 리스트 전체
  - `Source`: `watchlist`, `user_onboarding_keywords`
  - `Embedding Join Rule`: `stock_id`와 `keyword_id`는 문자열 기준으로 `test_service_embeddings.entity_id`와 비교할 수 있지만, 반드시 `entity_type='stock'` 또는 `entity_type='keyword'`를 함께 지정한다.
  - `Weight`: 사용자가 직접 선택한 신호이므로 모든 입력에 높은 초기 가중치를 부여한다.
  - `Entity Boosting`: 사용자가 명시적으로 선택한 종목과 일치하는 기사에는 stronger hard match를 부여한다.
- Path A2
  - `Input Scope`: `user_id` 기준으로 조회한 최근 20개 클릭/유효 읽기 로그만 사용한다.
  - `Source`: `interaction_events`
  - `Noise Filter`: 체류 시간 `10초` 미만 로그는 제외한다.
  - `Event Definition`: `content_view`를 뉴스 진입, `content_leave`를 뉴스 이탈로 보고, 같은 `content_session` 값으로 연결한다.
  - `Dwell Time`: `content_view.event_ts_client`와 `content_leave.event_ts_client` 차이로 계산한다.
  - `Time Reference`: 최신성 계산과 정렬에는 `event_ts_client`를 우선 사용하고, `event_ts_server`는 보조 검증용으로 사용한다.
  - `Log Structure`: 로그 1개는 `{ timestamp, news_id, extracted_keywords[], extracted_stock_entities[] }`를 기본 단위로 본다.
  - `Grouping`: 20개 로그에서 추출된 키워드를 유니크 키워드 단위로 압축하되, 원시 출현 빈도는 유지한다.
  - `Weighting`: 빈도 `log(1+N)`와 최신성 decay를 결합해 각 키워드 및 종목의 가중치를 계산한다.
  - `Matching`: 가중치가 적용된 키워드/종목 집합을 query로 사용하되, pair type별 score calibration 이후 Max-Sim 검색을 수행한다.
  - `Entity Boosting`: 로그에서 반복적으로 등장한 종목에만 hard match 가중치를 부여한다.
  - `Entity Reconstruction`: `news_id`로 `news_keyword_mapping`, `news_stock_mapping`을 조회해 키워드/종목 엔터티를 복원한다.
  - `Embedding Join Rule`: 복원된 `keyword_id`, `stock_id`도 문자열 기준으로 `test_service_embeddings.entity_id`와 비교하되, 각각 `entity_type='keyword'`, `entity_type='stock'`를 함께 지정한다.

### 3.3 Stage 3. Mixing And Exploration

- 목적: 최종 노출 리스트를 exploitation과 freshness가 공존하는 형태로 구성한다.
- 기본 방향
  - `A1`은 cold start와 온보딩 기반 개인화의 기본 경로다.
  - `A2`는 warm user에서 우세한 개인화 경로다.
  - `B`는 freshness 보강과 exploration을 담당한다.
  - `C`는 popularity 기반 exploration과 안전한 다양성 보강을 담당한다.
- 최종 노출 비율은 장기적으로 `MAB`가 결정한다.
- 다만 초기 운영에서는 아래를 먼저 둔다.
  - 고정 또는 준고정 mix ratio
  - 최소/최대 노출 가드레일
  - `A2` 활성화 조건
  - `C` 편중 방지 규칙
- 즉, 1단계의 목표는 "MAB-ready한 서빙 구조"이고, 2단계에서 실제 bandit 자동화를 붙인다.

### 3.4 Stage 4. Session Cache And Refill

- 목적: 같은 세션 안에서 추천 순서를 고정하고, 페이지 이동 시 재계산 비용을 줄인다.
- 기본 방향
  - 첫 요청에서 `A1/A2/B/C` path별 candidate queue를 생성한다.
  - 세션 캐시에는 `request_id` 기준으로 `onboarding/behavior/breaking/popular queue`, `served_ids`, `current_mix_policy`, `batch_generation_id`를 저장한다.
  - 다음 페이지 요청은 현재 mix policy에 따라 각 queue에서 필요한 개수만큼 꺼내 page를 합성한다.
  - prefetch는 전체 잔량이 아니라 primary path 병목 기준으로 판단한다.
- 기대 효과
  - 같은 세션 안에서 결과 순서가 안정적으로 유지된다.
  - 페이지 이동 시 재조회 부담을 줄인다.
  - 특정 path만 먼저 바닥나서 mix가 무너지는 문제를 줄일 수 있다.

## 4. Serving Principles

### 4.1 Response Shape

- 응답은 기존 `POST /recommend/news` 계약을 유지한다.
- 최소 응답 필드
  - `request_id`
  - `items[].news_id`
  - `next_cursor`
  - `meta.source`
  - `meta.fallback_used`
- 요청 필드는 `user_id` 중심으로 최소화하고, 추천 서버가 내부적으로 사용자 신호를 조회하는 구조를 기본으로 한다.

### 4.2 Session And Pagination

- cursor 기반 pagination은 유지한다.
- 클라이언트는 같은 `request_id` 기준으로 다음 페이지를 요청한다.
- 추천 결과는 `request_id` 단위 세션 캐시에 path별 queue와 served state로 저장된다.
- cursor는 현재 세션 상태에서 다음 page를 생성하기 위한 포인터 역할을 한다.

### 4.3 Refill Principle

- 추천 세션은 "매 페이지 재계산"이 아니라 "배치 생성 후 소진" 구조를 따른다.
- 현재 batch가 남아 있으면 캐시된 path queue를 우선 사용한다.
- prefetch는 primary queue 잔량 기준으로 판단한다.
- prefetch 실패 시 현재 batch 서빙은 유지하고, 소진 시점에 재생성 또는 fallback으로 degrade 한다.

### 4.4 Bandit Principle

- 최종 노출 슬롯의 Path별 배분은 `MAB`가 결정하는 것을 원칙으로 한다.
- 사람이 직접 고정하는 값은 최종 mix가 아니라 다음 항목이다.
  - 초기 prior
  - 최소/최대 가드레일
  - 비활성화 시 fallback 비율
- 초기에는 `A1/A2/B/C` 경로를 모두 MAB 대상 후보로 보되, 아래 규칙을 둔다.
  - `A2`는 행동 데이터가 충분할 때만 활성화
  - cold user는 `A1 + B` 중심
  - `A1/A2` 모두 약하면 `B`와 `C` 비중 확대

### 4.5 Fallback Principle

- 입력 부족은 실패가 아니다.
- 온보딩 데이터가 없으면 `A1`은 latest baseline으로 degrade 한다.
- 최근 행동 로그가 없거나 약하면 `A2`는 비활성화하고 `A1 + B`로 간다.
- `A1/A2`가 모두 실패하면 `B + C` 중심 fallback을 사용한다.
- 리트리벌, MAB, 캐시에 문제가 생겨도 앱 화면에는 fallback 결과가 반환되어야 한다.
- fallback은 빈 화면보다 `latest/breaking/popular` 조합을 우선한다.

### 4.6 Cold Start Principle

- 신규 사용자에게도 결과가 나와야 한다.
- 방금 발행된 신규 기사도 병목 없이 후보에 들어올 수 있어야 한다.
- 사용자 로그가 없어도 `A1`과 `B`만으로 기본 서빙이 가능해야 한다.

## 5. Evaluation And Success Criteria

### 5.1 Latency And Stability

- 목표는 추천 API가 앱에서 체감 가능한 속도로 응답하는 것이다.
- 캐시 hit 구간에서는 `0.2초` 수준 응답을 지향한다.
- miss 구간은 더 느릴 수 있지만, 앱 사용성에 문제 없는 수준으로 통제해야 한다.

### 5.2 Online Metrics

- 아래 지표는 구조화 로그를 기반으로 일 단위 집계와 모니터링이 가능해야 한다.
  - `CTR@5/10/20`
  - `First Click Rate`
  - `Clicks Per Session`
  - `Mix Ratio(A1/A2/B/C)`
  - `Path-wise CTR`
  - `Cold vs Warm CTR`
  - `Latency p50/p95`
  - `Cache Hit Rate`
  - `Prefetch Success Rate`
  - `Fallback Rate`
  - `Bandit Allocation Share`

### 5.3 Offline Evaluation

- 최신순 baseline 대비 품질 차이를 점검할 수 있어야 한다.
- 오프라인 점검에서는 아래 항목을 함께 본다.
  - 다양성
  - 중복률
  - 최신성
  - 안정성

### 5.4 Qualitative Success

- 특정 카테고리 편중만 심한 결과가 아니라, 온보딩 기반 추천, 로그 기반 추천, 최신 속보, 인기 탐색이 의도한 비율로 함께 노출되어야 한다.
- 사용자는 추천 피드가 "전부 최신 기사만 나열된 화면" 또는 "내 취향만 과도하게 반복되는 화면"처럼 느껴지지 않아야 한다.

## 6. Open Product Decisions

- prefetch 임계치를 남은 몇 개 기준으로 둘지
- 초기 MAB를 path 단위로만 둘지, 슬롯 단위까지 세분화할지
- 추천 서버가 조회할 사용자 신호 source를 실시간 조회로 둘지, 일부 사전 집계 테이블로 둘지
- `C`의 popularity 정의에 recency와 diversity penalty를 어떻게 섞을지
- `keyword-keyword`, `stock-stock`, `keyword-stock` pair type별 similarity calibration을 어떤 방식으로 둘지
- Max-Sim 연산에서 `keyword vector score`와 `entity hard match score`의 가중치 비율을 어떻게 둘지
- 최신성 decay 함수를 `exp`, `half-life`, 구간 감쇠 중 어떤 형태로 둘지
- `A1`과 `A2` score를 serving 전에 어떤 방식으로 normalization 할지
- 캐시 miss 시 새 세션 발급과 기존 세션 복원 중 무엇을 우선할지
