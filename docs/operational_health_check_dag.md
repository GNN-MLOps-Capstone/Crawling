# Operational Health Check DAG

이 문서는 정기 운영 상태 점검용 `operational_health_check_daily` DAG를 설명한다.
이 DAG는 릴리즈 검증용 `live-readonly-test`와 다르게 Airflow에서 매일 자동 실행되는 health check다.

## DAG

- 파일: `dags/operational_health_check_daily_dag.py`
- DAG ID: `operational_health_check_daily`
- 스케줄: 매일 KST 04:30
- Connection: `news_data_db`
- 실행 방식: read-only `SELECT`
- 생성 시 paused 상태: `False`

## 목적

- 운영 주요 table/column 계약이 유지되는지 확인한다.
- news/crawled/filtered 산출물이 존재하고 join 가능한지 확인한다.
- recommendation metrics, path snapshot, bandit state shape이 유효한지 확인한다.
- serving embedding table에 `news`, `keyword`, `stock` embedding과 `model_version`이 있는지 확인한다.

## Task

- `check_schema_contracts`
  - 주요 table의 required columns를 `information_schema.columns`로 확인한다.
- `check_news_outputs`
  - `naver_news`, `crawled_news`, `filtered_news` join 가능성과 기본 freshness를 확인한다.
- `check_recommendation_outputs`
  - recommendation path 값, path C/A2 snapshot, bandit state shape을 확인한다.
- `check_embedding_serving_table`
  - `test_service_embeddings` entity type별 row와 model version 존재를 확인한다.
- `summarize_health`
  - task별 결과를 summary log로 남긴다.

## Failure Policy

Hard fail:

- 필수 table/column 누락
- orphan row 존재
- 필수 산출 table이 비어 있음
- recommendation path 값이 계약 밖임
- path snapshot shape 오류
- bandit state shape 오류
- serving embedding type 누락

Warning:

- news freshness 지연
- metrics freshness 지연

`fail_on_stale=true` param으로 freshness warning도 hard fail로 승격할 수 있다.

## Params

- `news_freshness_days`
  - 기본값: `3`
- `metrics_freshness_days`
  - 기본값: `2`
- `fail_on_stale`
  - 기본값: `false`

## 한계

- 운영 DB에 read-only로 직접 접속한다.
- 쿼리는 bounded aggregate 중심이지만, 운영 데이터 규모에 따라 비용이 달라질 수 있다.
- 상세 원인 분석이나 복구는 수행하지 않는다.
- DAG 내부 validation task를 대체하지 않고, 전체 운영 상태 감시를 담당한다.

## 확인 결과

Airflow 컨테이너에서 문법 검증과 DAG 목록 노출을 확인했다.

```bash
docker compose exec -T airflow-scheduler python -m py_compile /opt/airflow/dags/operational_health_check_daily_dag.py
docker compose exec -T airflow-scheduler airflow dags unpause operational_health_check_daily
docker compose exec -T airflow-scheduler airflow dags list
```

`airflow dags list`에서 `operational_health_check_daily`가 `is_paused=False`로 노출된다.
