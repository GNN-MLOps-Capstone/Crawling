# Live Read-Only Test Plan

이 문서는 운영 또는 스테이징 DB를 read-only로 점검하는 live test 설계다.
기본 테스트, offline artifact 테스트, 로컬 통합 테스트와 분리해서 운영 상태 점검 또는 릴리즈 전 검증 용도로만 실행한다.

## 원칙

- 기본 테스트 명령에 포함하지 않는다.
- 명시적 opt-in 환경변수가 없으면 skip한다.
- DB 계정 자체가 read-only여야 한다.
- 테스트 시작 시 transaction read-only와 statement timeout을 설정한다.
- 모든 쿼리는 read-only `SELECT`만 허용한다.
- 대량 scan을 피하고 `LIMIT`, 최근 window, `COUNT(*)` 범위를 제한한다.
- 결과는 운영 데이터의 절대값보다 schema, freshness, shape, non-empty, null/duplicate 이상 여부를 본다.

## 실행 조건

필수 환경변수:

- `ALLOW_LIVE_READONLY_TESTS=1`
- `LIVE_DB_DSN` 또는 분리된 connection 변수
  - `LIVE_DB_HOST`
  - `LIVE_DB_PORT`
  - `LIVE_DB_NAME`
  - `LIVE_DB_USER`
  - `LIVE_DB_PASSWORD`

권장 DB session 설정:

```sql
SET default_transaction_read_only = on;
SET statement_timeout = '5s';
SET lock_timeout = '1s';
SET idle_in_transaction_session_timeout = '10s';
```

권장 pytest marker:

```python
pytestmark = [pytest.mark.live_db, pytest.mark.readonly]
```

권장 실행:

```bash
ALLOW_LIVE_READONLY_TESTS=1 \
LIVE_DB_DSN='postgresql://readonly_user:***@host:5432/dbname' \
python -m pytest tests/live/test_live_readonly_db.py -m 'live_db and readonly'
```

Docker 서비스로 분리할 경우:

```bash
ALLOW_LIVE_READONLY_TESTS=1 docker compose run --rm live-readonly-test
```

현재 구현 파일:

- `tests/live/test_live_readonly_db.py`

현재 구현된 Docker 서비스:

- `live-readonly-test`

opt-in 환경변수가 없을 때 최근 확인 결과:

```text
4 skipped
```

## 테스트 대상

### 1. Core News Tables

대상 table/view:

- `public.naver_news`
- `public.crawled_news`
- `public.filtered_news`
- `public.v_filtered_news_full`
- `public.processed_content_hashes`

검증:

- 필수 table/view가 존재한다.
- 필수 컬럼이 존재한다.
- primary/unique key에 해당하는 컬럼이 null이 아니다.
- 최근 N일 데이터 freshness가 기준 안에 있다.
- `filtered_news.news_id`가 `naver_news.news_id`와 join 가능하다.
- `crawled_news.news_id`가 `naver_news.news_id`와 join 가능하다.
- 최근 데이터에서 `filter_status` 값 분포가 조회 가능하다.

예시 쿼리:

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'filtered_news';
```

```sql
SELECT MAX(pub_date)
FROM public.naver_news;
```

```sql
SELECT COUNT(*)
FROM public.filtered_news fn
LEFT JOIN public.naver_news nn ON nn.news_id = fn.news_id
WHERE fn.news_id IS NOT NULL
  AND nn.news_id IS NULL
LIMIT 1;
```

### 2. Entity Mapping / Embedding Tables

대상 table:

- `public.keywords`
- `public.stocks`
- `public.news_keyword_mapping`
- `public.news_stock_mapping`
- `public.test_service_embeddings`

검증:

- mapping table의 `news_id`가 최근 `filtered_news`와 join 가능하다.
- keyword/stock mapping의 entity id가 master table과 join 가능하다.
- `test_service_embeddings`에 `entity_type in ('news', 'keyword', 'stock')` 값이 존재한다.
- `test_service_embeddings`의 `model_version` 최신값이 비어 있지 않다.
- API repository가 기대하는 embedding join rule이 실제 table에서 성립한다.

예시 쿼리:

```sql
SELECT entity_type, COUNT(*)
FROM public.test_service_embeddings
GROUP BY entity_type;
```

```sql
SELECT model_version, COUNT(*)
FROM public.test_service_embeddings
GROUP BY model_version
ORDER BY model_version DESC
LIMIT 5;
```

### 3. Recommendation Snapshot / Metrics Tables

대상 table:

- `public.recommendation_serves`
- `public.interaction_events`
- `public.recommendation_news_path_metrics`
- `public.recommendation_path_c_snapshot`
- `public.recommendation_path_a2_snapshot`
- `public.recommendation_bandit_state`

검증:

- metrics table에 최근 window row가 존재한다.
- `path` 값이 API public path 계약과 맞다.
  - `A1`, `A2`, `B`, `C`, `TOTAL`
- path C snapshot의 최신 row가 존재하고 `news_ids` array가 null이 아니다.
- path A2 snapshot의 `items`가 JSONB array 형태다.
- bandit state 최신 row가 있고 `alpha`, `beta`가 null이 아니다.
- bandit state의 `state_payload`가 JSON으로 parse 가능한 shape인지 DB JSON 연산자로 확인한다.

예시 쿼리:

```sql
SELECT MAX(bucket_end)
FROM public.recommendation_news_path_metrics;
```

```sql
SELECT path, COUNT(*)
FROM public.recommendation_news_path_metrics
WHERE bucket_end >= now() - interval '24 hours'
GROUP BY path;
```

```sql
SELECT snapshot_at, cardinality(news_ids)
FROM public.recommendation_path_c_snapshot
ORDER BY snapshot_at DESC
LIMIT 1;
```

```sql
SELECT COUNT(*)
FROM public.recommendation_path_a2_snapshot
WHERE jsonb_typeof(items) <> 'array';
```

### 4. Freshness / Lag Checks

대상:

- crawling freshness
- filtered/refined freshness
- recommendation metrics freshness
- embedding model freshness

검증:

- 최근 데이터가 완전히 멈춰 있지 않은지 확인한다.
- freshness threshold는 운영 SLA가 확정되기 전까지 warning 성격으로 둔다.
- hard fail 기준은 너무 공격적으로 잡지 않는다.

초기 권장 기준:

- `naver_news.max(pub_date)`가 최근 3일 이내
- `crawled_news.max(crawled_at)`가 최근 3일 이내
- `recommendation_news_path_metrics.max(bucket_end)`가 최근 2일 이내
- `test_service_embeddings.max(model_version)`은 null 아님

## 테스트 파일 구조

```text
tests/live/
  test_live_readonly_schema.py
    - table/view existence
    - required column existence

  test_live_readonly_news_quality.py
    - news/crawled/filtered join 가능성
    - freshness
    - null/duplicate smoke check

  test_live_readonly_recommendation_quality.py
    - metrics freshness
    - snapshot shape
    - bandit state shape

  test_live_readonly_embedding_quality.py
    - service embedding entity_type/model_version
    - mapping/master join smoke check
```

## Safety Guard 구현 지침

pytest fixture는 다음을 강제한다.

- `ALLOW_LIVE_READONLY_TESTS=1` 없으면 전체 skip
- DB 연결 직후 read-only session 설정
- test query 실행 전 SQL 문자열이 `SELECT`, `WITH`, `SHOW`로 시작하는지 확인
- `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP`, `TRUNCATE`, `CREATE`, `COPY` 포함 시 실패
- statement timeout 설정
- autocommit off + rollback

권장 helper:

```python
FORBIDDEN_SQL = ("insert", "update", "delete", "alter", "drop", "truncate", "create", "copy")

def execute_readonly(cursor, sql, params=None):
    normalized = sql.strip().lower()
    assert normalized.startswith(("select", "with", "show"))
    assert not any(token in normalized for token in FORBIDDEN_SQL)
    cursor.execute(sql, params or ())
    return cursor.fetchall()
```

## 실패 해석

live read-only test 실패는 항상 코드 버그를 의미하지 않는다.
다음 가능성을 분리해서 해석한다.

- 운영 데이터 지연
- 배치 미실행
- 권한/네트워크 장애
- schema migration 미반영
- 실제 코드와 운영 DB schema drift

따라서 결과 보고에는 다음을 포함한다.

- 실패 쿼리 이름
- 기대 계약
- 실제 관측값
- 데이터 기준 시각
- hard fail인지 warning 후보인지

## 제외 범위

- write query
- 운영 table cleanup
- 운영 데이터 생성
- 대량 full scan
- 성능 benchmark
- 운영 Redis/MLflow write 검증

이 테스트는 `offline-artifact-test`와 별도 서비스/marker로 유지한다.
