# Recommendation Server API Draft (FastAPI)

## 1) Scope

- Endpoint: `POST /recommend/news`
- Language/Framework: Python + FastAPI
- Current goal: `filtered_news` 기반 Mock 추천 API 제공
- Future goal: 로그/온보딩/벡터/LLM/MAB로 확장 가능한 계약(Contract) 유지

## 2) API Contract

### Request

```json
{
  "user_id": "string",
  "limit": 20,
  "cursor": "string|null",
  "request_id": "string|null",
  "context": {}
}
```

- Required: `user_id`, `limit`
- Optional: `cursor`, `request_id`, `context`
- `context`는 확장 전용 JSON object (온보딩/행동로그/실험군/디바이스 등)

### Response

```json
{
  "request_id": "string",
  "items": [
    {"news_id": 123},
    {"news_id": 456}
  ],
  "next_cursor": "string|null",
  "meta": {
    "source": "mock_latest",
    "fallback_used": false
  }
}
```

- `items`는 `news_id`만 반환
- `request_id`는 요청 추적/캐시 키로 사용

## 3) Validation Rules

- `limit`: `1 <= limit <= 100` 권장
- `cursor`가 있으면 opaque token decode 후 아래 검증 수행
  - `cursor.v` 지원 버전인지 확인
  - `cursor.limit`과 요청 `limit`이 다르면 `400 Bad Request`
- `context`는 dict(object)만 허용 (`null`은 빈 object로 처리 가능)
- `request_id`가 없으면 서버에서 UUID 생성

### Error Shape (권장)

```json
{
  "error": {
    "code": "INVALID_CURSOR_LIMIT_MISMATCH",
    "message": "cursor.limit must equal request.limit",
    "request_id": "..."
  }
}
```

## 4) Cursor Design

- 형식: base64url(JSON) + (선택) 서명(HMAC)
- opaque token 원칙: 클라이언트가 내부 필드를 해석하지 않음
- 최소 포함 필드
  - `v`: cursor schema version (예: `1`)
  - `limit`
  - `offset` 또는 `(last_pub_date, last_news_id)`
- 권장 payload 예시

```json
{
  "v": 1,
  "limit": 20,
  "offset": 40,
  "request_id": "req_abc123",
  "issued_at": "2026-03-05T12:00:00Z"
}
```

## 5) Mock Recommendation Logic (현재 동작)

- `user_id`는 추천 계산에 사용하지 않고 로그에만 기록
- DB 조회: `filtered_news` 최신순
- `limit`만큼 `news_id` 반환
- 정렬 안정성 확보를 위해 tie-breaker 포함 권장
  - `ORDER BY published_at DESC, news_id DESC`

### SQL Example

```sql
SELECT news_id, published_at
FROM filtered_news
WHERE published_at <= NOW()
ORDER BY published_at DESC, news_id DESC
LIMIT :limit OFFSET :offset;
```

## 6) Caching Strategy (request_id 단위)

- 점수 계산 단위: 후보 100개 선계산 후 캐시
- Cache key: `reco:{user_id}:{request_id}:{model_version}`
- Value 예시
  - `ordered_news_ids`: `[ ... up to 100 ]`
  - `created_at`, `expires_at`
  - `meta`: `source`, `cache_status`
- TTL: 5~10분 (초기 600초 권장)
- 만료 시 재계산
- 동일 `request_id`에서는 순서 불변 보장
  - 최초 계산 결과를 그대로 paging
- 저장소 권장: Redis

## 7) FastAPI Suggested Structure

```text
app/
  main.py
  api/
    recommend.py
  schemas/
    recommend.py
  services/
    recommend_service.py
    cursor_service.py
    cache_service.py
  repositories/
    news_repository.py
  core/
    config.py
    logging.py
```

- Router: `/recommend/news`
- Pydantic model로 request/response 엄격 검증
- Service 계층에서 캐시/DB/후보선정/페이징 분리

## 8) Future-Proofing (context 확장)

- 원칙
  - 기존 필드(`user_id`, `limit`) 유지
  - 신규 입력은 `context` 하위에만 추가
  - 응답 호환성 유지 (`items[].news_id` 유지)
- `context` 예시

```json
{
  "onboarding": {"risk_profile": "balanced"},
  "behavior": {"recent_click_keywords": ["AI", "반도체"]},
  "experiment": {"mab_group": "B"}
}
```

## 9) Target Architecture Mapping (요구사항 반영)

1. Base Pool
- 최근 3일 뉴스 약 3,000건 추출

2. Multi-Path Retrieval
- Path A: 개인화 유사도(벡터)
- Path B: 1~2시간 속보
- Path C: 전체 인기(CTR)
- 후보/서빙 로그 메타 설계:
  - `path_source`, `position`, `request_id`, `impression_id`, `is_fallback`, `latency_ms`, `cache_status`

3. Ranking
- Path A(100개)에 대해서만 LLM scoring 후 상위 50~60

4. Mixing(MAB)
- 예: 20개 결과 = A(14) + B(3) + C(3)
- 클릭 reward로 비율 동적 업데이트

## 10) 해야 할 것 (Execution TODO)

### P0 (Mock API 출시)

- [ ] FastAPI 앱 골격 추가 (`app/main.py`, router 등록)
- [ ] `POST /recommend/news` 스키마/검증 구현
- [ ] cursor encode/decode 유틸 + `limit mismatch -> 400` 처리
- [ ] `filtered_news` 최신순 조회 repository 구현
- [ ] request_id 발급/전달 로직 구현
- [ ] 기본 메타(`source=mock_latest`, `fallback_used=false`) 반환
- [ ] 구조화 로그(요청/응답/latency/request_id/user_id) 추가
- [ ] Dockerfile 추가 (`dockerfile/recommend-api.Dockerfile`)
- [ ] `docker-compose.yaml`에 `recommend-api` 서비스 추가
  - [ ] depends_on: `news-database`, `redis`
  - [ ] healthcheck: `/healthz`
  - [ ] networks: `news-network` (+ 필요 시 `proxy-net`)
- [ ] 컨테이너 실행 커맨드 고정 (`uvicorn app.main:app --host 0.0.0.0 --port 8000`)
- [ ] 단위 테스트
  - [ ] request validation
  - [ ] cursor validation
  - [ ] pagination consistency

### P1 (캐시/안정화)

- [ ] Redis 연결 및 `user_id+request_id+model_version` 키 적용
- [ ] 후보 100개 선계산 후 캐시 저장
- [ ] 동일 request_id 재요청 시 순서 불변성 테스트
- [ ] TTL(5~10분) 정책 및 만료 재계산 경로 구현
- [ ] 장애시 fallback 정책 정의 (`fallback_used=true`)

### P2 (추천 고도화)

- [ ] Base Pool(3일/3000개) 쿼리 최적화 및 인덱스 점검
- [ ] Path A/B/C 후보 추출기 분리
- [ ] LLM ranker 인터페이스 + 점수 스키마(relevance/novelty/clarity/final)
- [ ] MAB mixing 정책 모듈화 + reward 로그 수집
- [ ] 관측성(대시보드)
  - [ ] path별 CTR
  - [ ] cache hit ratio
  - [ ] p95 latency

## 11) Open Decisions

- `limit` 상한값 (20 고정 vs 100 허용)
- cursor에 HMAC 서명 적용 여부
- request_id 생성 주체 (클라이언트 우선 vs 서버 강제)
- model_version 관리 방식 (환경변수 vs DB)
- fallback 시나리오 우선순위 (캐시 실패/DB 실패/랭커 실패)

## 12) Docker Deployment Draft (API)

### Service Name

- `recommend-api` (FastAPI)

### Container Requirements

- Base: `python:3.11-slim` 권장
- App port: `8000`
- Runtime: `uvicorn`
- Readiness/Liveness endpoint: `GET /healthz`

### Environment Variables (예시)

- `RECO_API_PORT=8000`
- `RECO_MODEL_VERSION=v1`
- `RECO_CACHE_TTL_SEC=600`
- `NEWS_DB_HOST=news-database`
- `NEWS_DB_PORT=5432`
- `NEWS_DB_NAME=${DATA_POSTGRES_DB_NAME}`
- `NEWS_DB_USER=${DATA_POSTGRES_USER}`
- `NEWS_DB_PASSWORD=${DATA_POSTGRES_PASSWORD}`
- `REDIS_HOST=redis`
- `REDIS_PORT=6379`
- `REDIS_PASSWORD=${REDIS_PASSWORD}`

### Compose Integration (예시 스펙)

```yaml
recommend-api:
  build:
    context: .
    dockerfile: dockerfile/recommend-api.Dockerfile
  command: uvicorn app.main:app --host 0.0.0.0 --port 8000
  environment:
    RECO_MODEL_VERSION: v1
    RECO_CACHE_TTL_SEC: 600
    NEWS_DB_HOST: news-database
    NEWS_DB_PORT: 5432
    NEWS_DB_NAME: ${DATA_POSTGRES_DB_NAME}
    NEWS_DB_USER: ${DATA_POSTGRES_USER}
    NEWS_DB_PASSWORD: ${DATA_POSTGRES_PASSWORD}
    REDIS_HOST: redis
    REDIS_PORT: 6379
    REDIS_PASSWORD: ${REDIS_PASSWORD}
  depends_on:
    news-database:
      condition: service_healthy
    redis:
      condition: service_healthy
  healthcheck:
    test: ["CMD", "curl", "--fail", "http://localhost:8000/healthz"]
    interval: 30s
    timeout: 5s
    retries: 5
    start_period: 20s
  restart: always
  networks:
    - news-network
    - proxy-net
```
