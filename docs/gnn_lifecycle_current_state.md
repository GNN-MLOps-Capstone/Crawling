# GNN 모델 데이터 수집-학습 라이프사이클 (현재 DAG 기준)

## 1) 한눈에 보는 현재 파이프라인

`news_dag` (3시간 주기, 수집/크롤링)  
-> `news_analysis_integration_v2` (매일 02:00, 정제/분석/적재)  
-> `gnn_trainset_creation` (TriggerDagRun)  
-> `gnn_training_pipeline` (Dataset 트리거: `s3://silver/trainset`)  
-> `graph_to_db` (후보 모델 게이트 통과 시 TriggerDagRun)

핵심 포인트:
- 재학습은 "성능 저하 감지 기반"이 아니라, 상류 데이터 파이프라인 결과를 따라 실행되는 구조다.
- 학습 후 모델 배포는 무조건이 아니라 게이트 통과(`candidate`) 시에만 진행된다.

---

## 2) 단계별 상세

## 2-1. 데이터 수집 (Raw/Bronze 소스)

### A. 뉴스 수집
- DAG: `news_dag`
- 스케줄: `0 */3 * * *`
- 동작:
1. Naver API 기반 뉴스 수집 (`collect_naver_news`)
2. 수집 URL 도메인 필터링 + 본문 크롤링 (`news_crawler`)
3. 결과를 PostgreSQL(`crawled_news`) 중심으로 저장

### B. Bronze 적재 (보조 파이프라인)
- DAG: `news2minio_daily`
- 스케줄: 매일 02:00
- 동작: PostgreSQL의 일 단위 뉴스를 MinIO `bronze/crawled_news`로 적재
- 비고: GNN 학습의 직접 트리거 체인에는 현재 핵심 의존으로 보이지 않음.

---

## 2-2. 분석/정제 및 Silver 데이터 생성

- DAG: `news_analysis_integration_v2`
- 스케줄: `0 2 * * *` (catchup=True, max_active_runs=1)
- 주요 태스크:
1. `refinement`: 뉴스 정제
2. `gemini_analysis`: 종목/키워드 추출, 분석
3. `keyword_snapshot`: 키워드 임베딩 스냅샷 생성/갱신
4. `db_loading`: 분석 결과를 DB에 반영
5. `resolve_trainset_target_date`: 학습셋 생성 기준 날짜 결정
6. `trigger_gnn_trainset_creation`: `gnn_trainset_creation` 실행

생성/사용되는 핵심 데이터:
- `silver/refined_news/...`
- `silver/extracted_keywords/...`
- `silver/extracted_stocks/...`
- `silver/keyword_embeddings/date=.../keyword_embeddings.parquet`

---

## 2-3. GNN 학습셋 생성

- DAG: `gnn_trainset_creation`
- 스케줄: 없음(`schedule_interval=None`), 상위 DAG에서 트리거
- 입력:
1. target_date (`news_analysis_integration_v2`에서 전달)
2. MinIO 최신 키워드 스냅샷 (`keyword_embeddings`)
3. MinIO 최신 종목 임베딩 (`stock_embeddings`)
4. DB 키/메타 정보 (`keywords`, `stocks`)
5. Silver의 정제/추출 데이터 (`refined_news`, `extracted_*`)

- 처리(`modules.dataset.graph_builder.run_graph_building`):
1. 노드 구성: news/keyword/stock
2. 엣지 구성: news-keyword, news-stock
3. cutoff date 기준으로 뉴스 히스토리 필터링
4. HeteroData 그래프 생성 후 저장

- 출력:
- `silver/trainset/date=YYYYMMDD/hetero_graph.pt`
- `silver/trainset/date=YYYYMMDD/node_mapping.pkl`
- Dataset outlet 발행: `s3://silver/trainset`

---

## 2-4. GNN 학습 및 실험 기록

- DAG: `gnn_training_pipeline`
- 스케줄: Dataset 기반 (`schedule=[Dataset("s3://silver/trainset")]`)
- 입력:
1. trainset 경로 (`trainset/date=YYYYMMDD/hetero_graph.pt`)
2. 학습 설정 (`dags/modules/training/config.json`)
3. gate_threshold (기본 0.25)

- 처리(`modules.training.trainer.run_training_pipeline`):
1. 그래프 로드 + 전처리(저빈도 노드 필터링, 고립 뉴스 제거, PMI 엣지 추가)
2. RandomLinkSplit(train/val/test)
3. 학습 및 평가(Explicit/Implicit/Golden)
4. MLflow 로깅(파라미터, 지표, 아티팩트)
5. 게이트 판정:
   - 기준: `holdout_final_final_score > gate_threshold`
   - missing=0 조건 필요
6. 통과 시 run tag `status=candidate`, `candidate_version=v_YYYYMMDD` 부여

- 출력:
- MLflow artifact: `gnn_model.pt`, `node_embeddings.pkl`, `node_mapping.pkl` 등
- Dataset outlet 발행: `s3://gold/gnn`

---

## 2-5. 서빙용 DB 반영

- DAG: `graph_to_db`
- 트리거: `gnn_training_pipeline` 내부 `check_candidate_tagged` 통과 시 TriggerDagRun
- 동작:
1. MLflow에서 `status=candidate`(및 candidate_version) run 탐색
2. `node_embeddings.pkl` + `node_mapping.pkl` 다운로드
3. PostgreSQL `test_service_embeddings` upsert

---

## 3) 운영 관점 현재 상태 (요청사항 반영)

## 3-1. 재학습 트리거

현재 상태:
- "자동 재학습 자체"는 존재한다.
- 단, 트리거 기준은 성능/드리프트 이벤트가 아니라 데이터 파이프라인 완료 이벤트다.
  - `news_analysis_integration_v2` -> `gnn_trainset_creation`
  - `s3://silver/trainset` Dataset update -> `gnn_training_pipeline`

없는 것:
- 운영 성능 저하를 감지해 재학습을 시작하는 정책형 트리거
- 예: 클릭률/정답률 하락, 분포 드리프트, 실패율 급증 등 기반 트리거

## 3-2. 성능 모니터링

현재 상태:
- 학습 시점 오프라인 지표는 MLflow에 상세 기록됨.
- 게이트 로직으로 후보 승격 제어도 일부 존재.

없는 것:
- 배포 후 온라인 성능 모니터링 대시보드/알람
- 주기적 재평가 DAG(운영 데이터 기반 추론 품질 점검)
- candidate -> product 승격 자동화 정책

---

## 4) 현재 라이프사이클 요약 결론

현 구조는 "데이터 수집/분석 파이프라인에 결합된 배치형 학습-배포 체인"이다.
- 강점: 데이터 갱신 시 학습까지 자동 연결, MLflow 기반 실험 추적, 후보 게이트 존재
- 공백: 운영 성능 감시와 그에 연동된 정책형 재학습 루프 부재

즉, 현재는 "학습 자동화"는 되어 있으나 "운영 성능 기반의 MLOps 폐루프"는 아직 미구현 상태다.
