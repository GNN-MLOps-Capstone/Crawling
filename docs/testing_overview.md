# 테스트 체계 정리

이 문서는 뉴스 수집, 정제/적재, 분석, 추천 모델 학습, 추천 지표 집계 파이프라인에 대해 어떤 테스트를 두었는지 정리한다.
각 테스트가 어떤 범위를 확인하는지, 언제 실행하는지, 운영 DB를 확인할 때 어떤 안전장치를 두었는지를 함께 담았다.

## 요약

현재 테스트는 코드 내부 로직부터 운영 산출물 점검까지 6개 레이어로 나누어져 있다.

| 검증 레이어 | 상태 | 주로 보는 것 |
|---|---|---|
| API / Module | 구성 완료 | 추천 API 응답 계약, GNN 학습/배포 모듈 동작 |
| DAG wiring | 구성 완료 | Airflow task id, dependency, trigger, env 연결 |
| Workflow contract | 구성 완료 | crawling, ingestion, training, metric workflow의 선후관계와 입력·출력 계약 |
| Offline artifact | 구성 완료 | 운영 DB 없이 parquet, SQL, payload, DB row shape 검증 |
| Live read-only | 구성 완료 | 실제 DB schema와 주요 산출물 shape를 read-only로 검증 |
| Operational health check DAG | 구성 완료 | Airflow에서 매일 freshness, snapshot, metrics, embedding 상태 점검 |

이번 테스트 정리에서 가장 중요한 점은 release 검증과 운영 health check를 분리했다는 것이다.
release 검증은 배포 전후에 사람이 실행해 산출물 계약과 실제 DB shape가 맞는지 확인한다. 반면 health check DAG는 Airflow에서 매일 돌면서 데이터 freshness와 주요 산출물 상태를 계속 확인한다.

## 검증 범위

테스트 대상 workflow는 크게 네 영역으로 보면 된다.

| Workflow | 역할 | 주요 산출물/상태 |
|---|---|---|
| Crawling | Naver 뉴스와 본문 수집 | raw/crawled news, bronze parquet |
| Ingestion / Analysis | 수집 뉴스 정제, 종목/키워드/분석 결과 적재 | refined news, mapping tables, daily finalize parquet |
| Training / Serving | GNN 학습 데이터 구성, 후보 모델 평가, serving embedding 배포 | trainset, candidate version, service embeddings |
| Metric Aggregation | 추천 path별 노출/클릭/체류 지표와 bandit 상태 집계 | hourly metrics, path snapshots, bandit posterior state |

## 현재 결과

최근 로컬에서 돌린 검증은 모두 통과했다.

| Test | 최근 결과 |
|---|---:|
| Recommend API | 28 passed |
| DAG Module / GNN | 16 passed, 2 warnings |
| DAG Wiring | 7 passed |
| Workflow Contract | 5 passed |
| Offline Artifact | 5 passed |
| Live Read-Only | 4 passed |

`operational_health_check_daily` DAG는 Airflow에 등록되어 있고, 매일 KST 04:30에 실행된다.

## 안전장치와 남은 리스크

운영 DB를 확인하는 테스트는 read-only를 기본 원칙으로 둔다.

- live test 계정은 DB 권한 자체가 read-only여야 한다.
- 테스트 쿼리는 `SELECT`, `WITH`, `SHOW` 계열만 허용한다.
- `ALLOW_LIVE_READONLY_TESTS=1`과 DB 연결 정보가 없으면 live test는 DB에 연결하지 않는다.
- health check DAG도 write query를 실행하지 않는다.
- 로컬 검증용 credential은 요청이 있을 때만 기존 `.env`에 저장하고, `.env`는 커밋하지 않는다.

다만 아래 항목은 테스트만으로 완전히 없애기 어렵다.

- 외부 뉴스 API와 웹페이지 구조 변화는 운영 관측이 필요하다.
- live read-only test는 schema와 산출물 shape 중심이므로 추천 품질이나 분석 품질 전체를 보장하지는 않는다.
- health check threshold는 운영 데이터가 쌓이면서 지연 허용치와 failure 기준을 조정할 필요가 있다.

현재 로컬 환경은 `.env`에 live read-only DB 검증용 opt-in 값이 들어가 있으므로, `live-readonly-test`는 별도 환경 변수 주입 없이 read-only DB 검증까지 실행된다.

## 테스트 레이어

| Layer | 목적 | 운영 리소스 접속 | 실행 방식 | 주 사용 시점 |
|---|---|---:|---|---|
| Recommend API test | API 요청/응답, pagination, 추천 path, bandit, impression logging 검증 | 아니오 | `recommend-api` 컨테이너 | API 변경 시 |
| DAG module / GNN test | DAG 공용 모듈, GNN graph/training/deploy 로직 검증 | 아니오 | `gnn-test` 서비스 | DAG module, GNN 변경 시 |
| DAG wiring test | DAG task id, dependency, operator/trigger/env 연결 검증 | 아니오 | `dag-wiring-test` 서비스 | DAG 구조 변경 시 |
| Workflow contract test | task/DAG 사이 입력·산출물 key, trigger conf, path 계약 검증 | 아니오 | `workflow-contract-test` 서비스 | workflow 연결 변경 시 |
| Offline artifact test | parquet, DB row tuple, SQL, payload 산출물 계약 검증 | 아니오 | `offline-artifact-test` 서비스 | 산출물 schema/loader/SQL 변경 시 |
| Live read-only test | 운영/스테이징 DB schema와 산출물 shape를 read-only로 검증 | 예, opt-in | `live-readonly-test` 서비스 | 릴리즈 전/후, 장애 복구 후 |
| Operational health check DAG | 운영 freshness, snapshot, metrics, embedding 상태 정기 점검 | 예, Airflow connection | Airflow DAG | 매일 KST 04:30 |

## 실행 명령

### Recommend API

```bash
docker compose exec -T recommend-api python -m pytest tests/test_recommend_api.py
```

최근 확인 결과:

```text
28 passed
```

### DAG Module / GNN

```bash
docker compose run --rm gnn-test
```

최근 확인 결과:

```text
16 passed, 2 warnings
```

### DAG Wiring

```bash
docker compose run --rm dag-wiring-test
```

최근 확인 결과:

```text
7 passed
```

### Workflow Contract

```bash
docker compose run --rm workflow-contract-test
```

최근 확인 결과:

```text
5 passed
```

### Offline Artifact

```bash
docker compose run --rm offline-artifact-test
```

최근 확인 결과:

```text
5 passed
```

### Live Read-Only

`ALLOW_LIVE_READONLY_TESTS=1`과 `LIVE_DB_DSN`이 없으면 운영 DB에 연결하지 않고 skip된다.
현재 로컬 `.env`에는 read-only 계정 정보가 설정되어 있으므로 아래 명령만으로 live 검증이 실행된다.

```bash
docker compose run --rm live-readonly-test
```

최근 확인 결과:

```text
4 passed
```

다른 환경에서 운영/스테이징 DB를 read-only로 점검하려면 `.env` 또는 실행 시점 환경 변수로 opt-in 값을 넣는다.

```bash
ALLOW_LIVE_READONLY_TESTS=1 \
LIVE_DB_DSN='postgresql://readonly_user:***@host:5432/dbname' \
docker compose run --rm live-readonly-test
```

## 실행 기준

### PR / 로컬 개발

- 추천 API만 바뀐 경우:
  - `docker compose exec -T recommend-api python -m pytest tests/test_recommend_api.py`
- DAG module 또는 GNN 코드가 바뀐 경우:
  - `docker compose run --rm gnn-test`
- DAG task 구조, trigger, DockerOperator env가 바뀐 경우:
  - `docker compose run --rm dag-wiring-test`
- task/DAG 사이 산출물 key, trigger conf, path 규칙이 바뀐 경우:
  - `docker compose run --rm workflow-contract-test`
- parquet schema, loader tuple, SQL snapshot, payload 계약이 바뀐 경우:
  - `docker compose run --rm offline-artifact-test`

### 릴리즈 전후 확인

릴리즈 전에는 변경 범위 테스트에 더해 다음을 실행한다.

```bash
docker compose run --rm dag-wiring-test
docker compose run --rm workflow-contract-test
docker compose run --rm offline-artifact-test
```

운영/스테이징 DB와의 계약까지 확인해야 하면 read-only 계정으로 opt-in live test를 실행한다.
현재 로컬 `.env`에는 opt-in 값이 저장되어 있어 아래 명령만 실행하면 된다.

```bash
docker compose run --rm live-readonly-test
```

CI나 다른 장비처럼 `.env`에 live test 값이 없는 환경에서는 실행할 때 값을 주입한다.

```bash
ALLOW_LIVE_READONLY_TESTS=1 \
LIVE_DB_DSN='postgresql://readonly_user:***@host:5432/dbname' \
docker compose run --rm live-readonly-test
```

### 운영 중 확인

- `operational_health_check_daily` DAG가 매일 KST 04:30 실행된다.
- DAG는 `news_data_db`에 read-only query로 접근한다.
- freshness 지연은 기본적으로 warning으로 요약되고, `fail_on_stale=true` param으로 hard fail로 승격할 수 있다.
- `AIRFLOW_HEALTHCHECK_ALERT_EMAILS`가 설정되어 있으면 healthcheck DAG task 실패 시 email alert가 발송된다.

## 영역별로 보는 것

### Crawling

검증 레이어:

- DAG wiring
- Workflow contract
- Offline artifact
- Operational health check

주요 계약:

- `news_dag`가 `news_ingestion_integration_hourly`에 `window_start`, `window_end`를 넘긴다.
- Naver 수집 이후 crawler가 실행되고, hourly ingestion trigger가 이어진다.
- bronze incremental parquet key와 schema가 유지된다.
- 운영에서는 `naver_news`, `crawled_news` freshness와 join 가능성을 본다.

### Ingestion / Analysis

검증 레이어:

- DAG wiring
- Workflow contract
- Offline artifact
- Operational health check

주요 계약:

- `bronze_keys`, `news_ids`, `refined`, `stocks`, `keywords`, `analysis` key가 downstream과 맞는다.
- empty/no-op case가 downstream을 깨지 않는다.
- daily finalize parquet path와 dedupe 기준이 유지된다.
- 운영에서는 `filtered_news`, mapping tables, daily/refined 산출물 상태를 본다.

### Training / Serving

검증 레이어:

- GNN test
- DAG wiring
- Workflow contract
- Offline artifact
- Operational health check

주요 계약:

- trainset path는 `trainset/date=YYYYMMDD/hetero_graph.pt` 규칙을 따른다.
- `candidate_version`은 `v_YYYYMMDD` 규칙을 따른다.
- gate 통과 후 `graph_to_db`에 `model_status=candidate`, `candidate_version`이 전달된다.
- serving embedding row는 `test_service_embeddings`가 API에서 읽을 수 있는 shape를 유지한다.

### Metric Aggregation

검증 레이어:

- DAG wiring
- Workflow contract
- Offline artifact
- Live read-only
- Operational health check

주요 계약:

- hourly metrics SQL, path C snapshot, path A2 snapshot, bandit posterior update 순서가 유지된다.
- path 값은 `A1`, `A2`, `B`, `C`, `TOTAL` 계약을 따른다.
- path C snapshot은 `snapshot_at`, `news_ids`를 제공한다.
- path A2 snapshot은 `user_id`, `items`, `snapshot_at`을 제공한다.
- bandit state는 `scope`, `path`, `alpha`, `beta`, `window_end`를 제공한다.

## 안전 규칙

- 운영 DB에 붙는 테스트는 기본 실행에 포함하지 않는다.
- `live-readonly-test`는 `ALLOW_LIVE_READONLY_TESTS=1`과 DB 연결 정보 없이는 DB에 연결하지 않는다.
- live DB 계정은 read-only 계정이어야 한다.
- 로컬 검증용 read-only credential은 요청이 있을 때만 기존 `.env`에 저장한다.
- `.env`는 git 추적 대상이 아니어야 하며 커밋하지 않는다.
- live query는 `SELECT`, `WITH`, `SHOW`만 허용한다.
- health check DAG는 write query를 실행하지 않는다.
- 대량 full scan이나 cleanup은 테스트 범위가 아니다.

## Detail Toggles

<details>
<summary>Recommend API test</summary>

- 문서: `docs/test_inventory.md`
- 대상: `tests/test_recommend_api.py`
- 목적: API 요청/응답, pagination, 추천 path, bandit, impression logging 계약 검증
- 실행: `docker compose exec -T recommend-api python -m pytest tests/test_recommend_api.py`

</details>

<details>
<summary>DAG module / GNN test</summary>

- 문서: `docs/gnn_test_plan.md`
- 대상: GNN graph builder, training data loader, trainer gate, serving deploy, baseline eval
- 목적: DAG에서 호출되는 학습/평가 모듈의 입력·출력 계약 검증
- 실행: `docker compose run --rm gnn-test`

</details>

<details>
<summary>DAG wiring test</summary>

- 문서: `docs/test_inventory.md`
- 대상: `tests/test_airflow_task_wiring.py`
- 목적: DAG task id, dependency, operator, trigger, env wiring 검증
- 실행: `docker compose run --rm dag-wiring-test`

</details>

<details>
<summary>Workflow contract test</summary>

- 문서: `docs/workflow_test_plan.md`
- 대상: `tests/test_workflow_contracts.py`
- 목적: crawling, ingestion, training, metric aggregation workflow의 선후관계와 산출물 key 계약 검증
- 실행: `docker compose run --rm workflow-contract-test`

</details>

<details>
<summary>Offline artifact test</summary>

- 문서: `docs/offline_artifact_test_plan.md`
- 대상: `tests/test_offline_file_artifacts.py`, `tests/test_offline_db_artifacts.py`
- 목적: 운영 리소스 없이 parquet, DB row tuple, SQL, payload shape 검증
- 실행: `docker compose run --rm offline-artifact-test`

</details>

<details>
<summary>Live read-only test</summary>

- 문서: `docs/live_readonly_test_plan.md`
- 대상: `tests/live/test_live_readonly_db.py`
- 목적: 운영/스테이징 DB schema와 주요 산출물 shape를 read-only로 검증
- 실행: `docker compose run --rm live-readonly-test`
- 전제: `ALLOW_LIVE_READONLY_TESTS=1`과 `LIVE_DB_DSN` 또는 분리 connection 변수가 설정되어야 한다.

</details>

<details>
<summary>Operational health check DAG</summary>

- 문서: `docs/operational_health_check_dag.md`
- 대상: `dags/operational_health_check_daily_dag.py`
- 목적: 운영 freshness, snapshot, metrics, embedding 상태를 Airflow에서 정기 점검
- 실행: Airflow DAG `operational_health_check_daily`, 매일 KST 04:30
- 알림: `.env`의 `AIRFLOW_HEALTHCHECK_ALERT_EMAILS`로 실패 email 수신자 설정

</details>

## Reference Documents

- `docs/test_inventory.md`
  - 현재 테스트 파일과 검증 계약 목록
- `docs/gnn_test_plan.md`
  - GNN module/test 실행 전제
- `docs/workflow_test_plan.md`
  - workflow contract test 설계
- `docs/offline_artifact_test_plan.md`
  - 운영 리소스 미접속 산출물 테스트 설계
- `docs/live_readonly_test_plan.md`
  - opt-in live read-only test 설계
- `docs/operational_health_check_dag.md`
  - 정기 health check DAG 설명
