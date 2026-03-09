# Recommendation API Product And Architecture Draft

## 0. Document Scope

- 기준일: `2026-03-09`
- 이 문서는 추천 API에서 "무엇을 만들 것인가"를 정리하는 상위 스펙 문서다.
- 구현 작업, 테스트 순서, 운영 체크리스트는 [recommendation_api_operations_draft.md](/home/dobi/Crawling/docs/recommendation_api_operations_draft.md)에서 관리한다.
- 현재 코드와 문서가 다르면, 구현 전까지의 실제 동작은 코드 기준으로 본다.

## 1. Background And Problem (Why We Pivot)

- 기존 구상은 GNN, LightGBM 기반 랭킹 모델과 이를 뒷받침하는 대규모 행동 로그, 학습 파이프라인, 운영 자동화를 전제로 했다.
- 하지만 현재 상태에서는 클릭, 체류시간, 노출 로그가 충분히 적재되지 않았고, 남은 개발 기간 안에 이를 안정적으로 구축해 E2E 검증까지 마치는 것은 과도한 범위다.
- 따라서 v1은 "무거운 학습 파이프라인"보다 "짧은 기간 안에 실제로 서빙 가능한 추천 구조"에 집중한다.
- 새 방향은 데이터 의존도를 낮추면서 본문 전체 비교의 노이즈를 줄이기 위해, 정제된 종목/키워드 임베딩 기반 멀티 패스 리트리벌, LLM 랭킹, MAB 기반 믹싱을 결합한 하이브리드 구조다.
- 동시에 초반부터 온라인 지표와 오프라인 점검 기준을 함께 설계해, 추천 품질 개선 여부를 관측 가능하게 만든다.

## 2. Product Goal

### 현재 상태

- 엔드포인트는 `POST /recommend/news`다.
- FastAPI 추천 API는 이미 존재한다.
- 현재는 개인화 없이 최신 뉴스 `news_id`를 cursor 기반으로 반환하는 mock 단계다.
- 현재 cursor는 `request_id/offset/limit`만 유지하고, 추천 결과 리스트 자체를 캐시하지 않는다.

### v1 목표

- v1은 "완전한 ML 개인화 시스템"이 아니라 "실제로 서빙 가능한 하이브리드 추천 파이프라인" 구축이 목표다.
- 추천 응답은 아래 성격의 후보를 함께 다뤄야 한다.
  - 개인화 후보
  - 최신 속보 후보
- 최종 서빙 직전에는 단일 최신순이 아니라 경로별 후보를 혼합하는 구조를 가져야 한다.
- 장애나 입력 부족 시에도 앱 화면에는 자연스러운 fallback 결과가 나와야 한다.

### 단계별 목표

- 1단계
  - 1A. `context`가 비어 있어도 동작하는 Path A/LLM 뼈대 구축
  - 1A. 세션 캐시와 cursor slice 구조 구축
  - 1A. Path B 최신 속보 경로와 fallback 구축
  - 1B. 확정된 `context` 계약을 Path A와 LLM prompt에 반영
  - 1B. 온보딩/행동 로그를 `context`로 연결
- 2단계
  - Path C 인기 탐색 경로 보강
  - 클릭 로그 기반 MAB 도입
  - 경로별 mix ratio 동적 조정

### v1 비목표

- 대규모 행동 로그를 전제로 한 GNN 학습 파이프라인
- LightGBM 기반 학습 랭커 서빙
- 완성형 MLOps 자동화
- 모든 Path에 대한 LLM 재정렬
- 고도화된 장기 사용자 프로파일링

## 3. Target Recommendation Architecture

추천 파이프라인은 후보를 하나의 검색 방식으로 뽑는 대신, 성격이 다른 여러 경로에서 확보한 뒤 서빙 직전에 혼합하는 구조를 따른다.

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
- 기본 경로는 아래 세 가지다.

| Path | 목적 | 기본 로직 | 데이터 소스 |
| --- | --- | --- | --- |
| Path A | 기본 추천 + 개인화 확장 | `context`가 없으면 더 넓은 최신 풀에서 최신순 후보를 만들고, `context`가 있으면 여기에 개인화 신호를 누적 | RDBMS + 이후 임베딩 저장소 |
| Path B | 최신 속보 | Path A보다 더 짧은 시간창의 속보 기사 우선 추출 | RDBMS |
| Path C | 인기 탐색 | 앱 내 CTR이 높은 타 카테고리 기사 추출 | RDBMS + 집계 로그 |

- 1단계 초기 목표는 Path A `50개`, Path B `30개` 수준의 후보를 확보하는 것이다.
- `context`가 없는 1A에서는 Path A가 "넓은 최신성", Path B가 "짧은 속보성" 역할을 맡는다.
- Path A는 본문 전체가 아니라 정제된 키워드/종목 표현을 우선 사용하되, 1A에서는 최신순 baseline으로 먼저 시작할 수 있다.

### 3.3 Stage 3. LLM As A Ranker

- 목적: Path A의 개인화 후보를 더 정밀하게 재정렬한다.
- 적용 범위
  - 초기에는 Path A 후보 `50`개 전체를 재정렬 대상으로 본다.
  - Path B, Path C는 초기에는 LLM 재정렬 없이 유지한다.
- 기본 로직
  - Path A에서 이미 압축된 후보 리스트를 LLM에 전달한다.
  - 후보별 `relevance`, `novelty`, `clarity`, `final` 점수를 산출한다.
  - 최종 정렬은 애플리케이션이 `final` 점수 기준으로 수행한다.
- `context`가 없을 때도 같은 prompt shape를 유지하되, 빈 슬롯에는 기본 문구를 넣어 일반 추천 점수를 계산하게 한다.
- 목적은 "모든 경로를 LLM으로 처리"하는 것이 아니라, 비용과 지연을 통제하면서 Path A 후보의 정밀도를 높이는 것이다.

### 3.4 Stage 4. Mixing And Exploration

- 목적: 최종 노출 리스트를 개인화와 탐색이 공존하는 형태로 구성한다.
- 기본 방향
  - 1A에서는 Path A가 넓은 최신성 baseline 역할을 한다.
  - 1B부터 Path A는 개인화 exploitation 역할을 강화한다.
  - 1단계에서는 Path B가 속보성 보강 및 exploration 역할을 한다.
  - 2단계부터는 Path C와 MAB를 붙여 동적 비율 조정을 시작한다.
- 최종 노출 비율은 사람이 고정하지 않고 MAB가 결정한다.
- 문서에 남는 비율은 MAB의 초기 prior 또는 비활성화 시 fallback guardrail로만 사용한다.
- 초기 prior는 Path A 우세 구조를 두되, Path B와 Path C가 학습 가능한 최소 슬롯은 확보하는 방향이 적절하다.

### 3.5 Stage 5. Session Cache And Refill

- 목적: 같은 세션 안에서 추천 순서를 고정하고, 페이지 이동 시 LLM 재호출을 피한다.
- 기본 방향
  - 첫 요청에서 Path A/B와 LLM을 거쳐 path별 candidate queue를 생성한다.
  - 세션 캐시에는 `request_id` 기준으로 `A/B/C queue`, `served_ids`, `current_mix_policy`, `batch_generation_id`를 저장한다.
  - 다음 페이지 요청은 현재 mix policy에 따라 각 path queue에서 필요한 개수만큼 꺼내 page를 합성한다.
  - prefetch는 전체 잔량이 아니라 path별 병목 기준으로 판단한다.
  - 1A 기준 기본 trigger는 `Path A remaining <= 20`이다.
  - 현재 batch가 소진되면 prefetch된 다음 batch 또는 보충 queue로 자연스럽게 전환한다.
- 기대 효과
  - 같은 세션 안에서 결과 순서가 안정적으로 유지된다.
  - 페이지 이동마다 LLM을 다시 호출하지 않아도 된다.
  - 마지막 페이지 이후 응답 지연을 줄일 수 있다.
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

### 4.2 Session And Pagination

- cursor 기반 pagination은 유지한다.
- 클라이언트는 같은 `request_id` 기준으로 다음 페이지를 요청한다.
- 추천 결과는 `request_id` 단위 세션 캐시에 path별 queue와 served state로 저장되는 것을 기본 전제로 한다.
- cursor는 현재 세션 상태에서 다음 page를 생성하기 위한 포인터 역할을 한다.
- 구현 상세는 바뀔 수 있지만, 사용자 입장에서는 페이지 간 결과 순서가 일관되어야 한다.

### 4.3 Refill Principle

- 추천 세션은 "매 페이지 재계산"이 아니라 "배치 생성 후 소진" 구조를 따른다.
- 현재 batch가 남아 있으면 캐시된 path queue를 우선 사용한다.
- prefetch는 전체 잔량이 아니라 path별 병목 기준으로 판단한다.
- 1A 기본 규칙은 `Path A remaining <= 20`일 때 다음 batch 또는 Path A 보충을 시작하는 것이다.
- prefetch에 실패해도 현재 batch 서빙은 유지하고, 소진 시점에 재생성 또는 fallback으로 degrade 한다.

### 4.4 Bandit Principle

- 최종 노출 슬롯의 Path별 배분은 MAB가 결정하는 것을 원칙으로 한다.
- 사람이 직접 고정하는 값은 최종 mix가 아니라 MAB의 초기 prior, 최소/최대 가드레일, 비활성화 시 fallback 비율이다.
- 따라서 retrieval 단계의 후보 수와 serving 단계의 최종 mix를 구분해서 설계해야 한다.

### 4.5 Fallback Principle

- 입력 부족은 실패가 아니다.
- `context`가 없으면 Path A는 더 넓은 최신 풀, Path B는 더 짧은 속보 풀 기준으로 동작한다.
- 개인화 경로가 약하면 우선 최신 경로 비중을 늘린다.
- 이후 2단계에서 인기 경로가 안정화되면 MAB prior와 latest/popular 조합 기반 fallback을 확장한다.
- LLM, MAB, 캐시, 일부 리트리벌 경로에 문제가 생겨도 앱 화면에는 fallback 결과가 반환되어야 한다.
- fallback은 빈 화면보다 latest/popular 조합을 우선한다.

### 4.6 Cold Start Principle

- 신규 사용자에게도 결과가 나와야 한다.
- 방금 발행된 신규 기사도 병목 없이 후보에 들어올 수 있어야 한다.
- 사용자 로그가 없어도 Path A 최신 풀과 Path B 속보 풀만으로 기본 서빙이 가능해야 한다.

## 5. Evaluation And Success Criteria

### 5.1 Latency And Stability

- 목표는 추천 API가 앱에서 체감 가능한 속도로 응답하는 것이다.
- 캐시 hit 구간에서는 `0.2초` 수준 응답을 지향한다.
- miss 구간은 더 느릴 수 있지만, 앱 사용성에 문제 없는 수준으로 통제해야 한다.
- 특히 페이지 이동 요청은 세션 캐시 hit를 기본값으로 두고, 마지막 페이지 인근의 refill 지연을 최소화해야 한다.

### 5.2 Online Metrics

- 아래 지표는 애플리케이션 구조화 로그를 기반으로 일 단위 집계와 모니터링이 가능해야 한다.
  - `CTR@5/10/20`
  - `First Click Rate`
  - `Clicks Per Session`
  - `Mix Ratio(A/B/C)`
  - `Path-wise CTR`
  - `Exploration Slot CTR`
  - `Latency p50/p95`
  - `Cache Hit Rate`
  - `Prefetch Success Rate`
  - `Fallback Rate`
  - `LLM Timeout/Error Rate`

### 5.3 Offline Evaluation

- 초기 로그가 부족하더라도 최신순 baseline 대비 품질 차이를 점검할 수 있어야 한다.
- 오프라인 점검에서는 아래 항목을 함께 본다.
  - 다양성
  - 중복률
  - 최신성
  - 안정성
- 필요 시 AI 보조 평가를 사용하되, baseline 대비 어떤 지표가 개선되었는지 명시적으로 비교한다.

### 5.4 Qualitative Success

- 특정 카테고리 편중만 심한 결과가 아니라, 개인화와 탐색이 의도한 비율로 함께 노출되어야 한다.
- 사용자는 추천 피드가 "모두 같은 기사" 또는 "전부 최신 기사만 나열된 화면"처럼 느껴지지 않아야 한다.

### 5.5 Safety And Reliability

- 예외 상황에서도 앱 화면에 에러나 과도한 지연이 직접 노출되지 않아야 한다.
- 클릭 로그가 적재되고, 이 로그가 이후 믹싱 또는 보상 계산에 반영되는 데이터 루프가 확인되어야 한다.

## 6. Open Product Decisions

- Path A에서 사용할 개인화 입력의 최소 집합을 어디까지 둘지
- `context` 없는 1A baseline에서 Path A의 최신 풀 시간 범위를 어디까지 둘지
- prefetch 임계치를 남은 몇 개 기준으로 둘지
- next batch 생성을 FastAPI 내부 비동기 처리로 둘지, 별도 작업 큐로 분리할지
- Path C의 "인기"를 CTR만으로 볼지, recency를 섞을지
- 초기 MAB의 최소 단위를 Path 단위로 둘지, 슬롯 단위까지 세분화할지
- 캐시 miss 시 새 세션 발급과 기존 세션 복원 중 무엇을 우선할지
- 온라인 지표 대시보드의 최소 공개 범위를 어디까지 둘지
