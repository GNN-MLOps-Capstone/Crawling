# GNN Test Plan

이 문서는 GNN 학습/서빙 파이프라인의 자동 테스트 범위와 목적을 정리한다.
상세 검증 계약은 `tests/`의 테스트 코드가 기준이고, 이 문서는 왜 해당 테스트가 필요한지와 실행 전제를 설명한다.

## 범위

- 학습셋 생성: `dags/modules/dataset/graph_builder.py`
- 학습 전처리: `dags/modules/training/data_loader.py`
- 학습 평가/게이트: `dags/modules/training/trainer.py`
- 서빙 적재: `dags/modules/serving/deploy.py`

## 테스트 파일

- `tests/test_gnn_graph_builder.py`
  - cutoff date 기준으로 미래 뉴스가 제외되는지 검증
  - `HeteroData`와 `node_mapping.pkl`이 기대 구조로 생성되는지 검증

- `tests/test_gnn_training_data_loader.py`
  - temporal split이 train/val/test를 날짜 기준으로 정확히 분리하는지 검증
  - `train_only` message passing 정책이 val/test에 반영되는지 검증
  - 저빈도 노드 제거 후 serving mapping이 깨지지 않는지 검증

- `tests/test_gnn_trainer_gate.py`
  - evaluation stage의 candidate 승격 조건을 검증
  - 점수, missing 여부, threshold 조합에 따라 `status=candidate` 태그가 붙는지 검증

- `tests/test_gnn_serving_deploy.py`
  - MLflow artifact의 `node_embeddings.pkl`, `node_mapping.pkl`을 읽어
    `test_service_embeddings` 적재용 row로 바꾸는 과정이 맞는지 검증
  - 배포 성공 후 MLflow status tag가 `product/legacy`로 갱신되는지 검증

## 실행 전제

- 이 테스트들은 일반 추천 API 테스트보다 의존성이 많다.
- GNN 테스트는 운영 GNN 작업 이미지에 가까운 `gnn-test` 서비스에서 실행한다.
- `gnn-test`는 `dockerfile/gnn.Dockerfile`의 `gnn-test` target을 사용한다.
- Airflow 컨테이너나 Jupyter 컨테이너는 이 테스트의 공식 실행 환경이 아니다.
- 저장소 규칙상 기본 검증 명령은 아래와 같다.

```bash
docker compose run --rm gnn-test
```

이 명령은 GNN 테스트와 함께 GNN 런타임에서 확인해야 하는 DAG module 유틸 테스트도 실행한다.

```text
tests/test_hourly_pipeline_utils.py
tests/test_bandit_posterior_utils.py
tests/test_gnn_graph_builder.py
tests/test_gnn_training_data_loader.py
tests/test_gnn_trainer_gate.py
tests/test_gnn_serving_deploy.py
```

최근 확인 결과는 `15 passed, 2 warnings`이다.
warning은 PyG 내부 deprecation warning과 `graph_builder.py`의 tensor 생성 성능 warning이며, 테스트 실패 조건은 아니다.

## 원칙

- 외부 S3, MLflow, PostgreSQL에 직접 붙지 않는다.
- 단위 테스트에서는 fake object와 monkeypatch를 우선 사용한다.
- 그래프 크기는 작게 유지하고, 회귀 위험이 큰 계약만 검증한다.
- 운영 정책은 문서가 아니라 테스트 함수명과 assertion으로 고정한다.
