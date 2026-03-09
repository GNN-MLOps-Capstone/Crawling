# GNN Training Temporal Split 전환 설계서

## 1) 확인된 현재 상태 (코드 근거)

### 1.1 `pub_date`는 원천 데이터(`refined_news`)에는 존재함
- `refined_news` 저장 시 `pub_date` 컬럼을 명시적으로 저장한다.
- 근거: [`dags/modules/analysis/news_service.py:263`](/home/dobi/Crawling/dags/modules/analysis/news_service.py:263), [`dags/modules/analysis/news_service.py:273`](/home/dobi/Crawling/dags/modules/analysis/news_service.py:273)

### 1.2 `gnn_trainset_creation`에서 날짜 컷오프는 적용됨
- 그래프 빌드 시 `df_news['pub_date'] <= target_date` 조건으로 뉴스를 자른다.
- 근거: [`dags/modules/dataset/graph_builder.py:63`](/home/dobi/Crawling/dags/modules/dataset/graph_builder.py:63) ~ [`dags/modules/dataset/graph_builder.py:66`](/home/dobi/Crawling/dags/modules/dataset/graph_builder.py:66)

### 1.3 하지만 `hetero_graph.pt` 안에는 `pub_date`가 보존되지 않음
- `HeteroData`에 `news.x`, `keyword.x`, `stock.x`, `edge_index`만 넣고 저장한다.
- `data['news'].pub_date` 또는 timestamp 속성 저장 코드가 없다.
- 근거: [`dags/modules/dataset/graph_builder.py:117`](/home/dobi/Crawling/dags/modules/dataset/graph_builder.py:117) ~ [`dags/modules/dataset/graph_builder.py:135`](/home/dobi/Crawling/dags/modules/dataset/graph_builder.py:135), [`dags/modules/dataset/graph_builder.py:150`](/home/dobi/Crawling/dags/modules/dataset/graph_builder.py:150)

### 1.4 현재 train/val/test는 랜덤 링크 분할
- 학습 분할은 `RandomLinkSplit`으로 수행한다.
- 근거: [`dags/modules/training/data_loader.py:262`](/home/dobi/Crawling/dags/modules/training/data_loader.py:262)
- 기본 비율은 `val_ratio=0.2`, `test_ratio=0.2`.
- 근거: [`dags/modules/training/config.json:19`](/home/dobi/Crawling/dags/modules/training/config.json:19), [`dags/modules/training/config.json:20`](/home/dobi/Crawling/dags/modules/training/config.json:20)

### 1.5 누수 가능성이 큰 지점
- 분할 전에 전체 그래프에서 아래를 수행함:
1. 저빈도 노드 제거
2. 고립 뉴스 제거
3. 전역 PMI edge 생성
- 근거: [`dags/modules/training/data_loader.py:197`](/home/dobi/Crawling/dags/modules/training/data_loader.py:197) ~ [`dags/modules/training/data_loader.py:219`](/home/dobi/Crawling/dags/modules/training/data_loader.py:219)

즉, 랜덤 분할 이전에 미래 시점의 공기여도가 구조에 반영될 수 있어, 시계열 일반화 평가 관점에서 데이터 누수 위험이 있다.

## 2) 목표

- 랜덤 링크 분할을 중단하고, `pub_date` 기반 temporal split으로 전환한다.
- 노이즈 제거 목적의 전처리(빈도 필터/PMI)는 split 이전 전역 데이터에 대해 수행한다(의도적 누수 허용).
- 기존 엔진(`train_one_epoch`, `evaluate`)이 기대하는 인터페이스(`edge_label_index`, `edge_label`)는 유지한다.

## 3) 설계 원칙

1. Split 기준은 뉴스 시간(`news.pub_ts`)으로 고정한다.
2. 한 edge의 시각은 source news 노드의 시각으로 정의한다.
3. 구조 생성(필터링/PMI)은 컷오프까지의 전체 데이터에 대해 먼저 수행한다(노이즈 제거 우선).
4. val/test는 라벨만 미래 구간에서 뽑는다(라벨 split은 시간 기준 강제).
5. 모든 분할/음수샘플링은 seed 고정으로 재현 가능하게 한다.

## 4) 변경 범위 (파일별)

### 4.1 `dags/modules/dataset/graph_builder.py`

필수 변경:
1. `pub_date`를 epoch day 또는 unix timestamp(`torch.long`)로 변환해 `data['news'].pub_ts`로 저장.
2. `node_mapping.pkl`의 `news.meta`에 최소한 `news_id -> pub_date`를 저장.

이유:
- 학습 단계에서 S3의 `hetero_graph.pt`만으로 temporal split 가능해야 한다.
- 운영 중 split 재현/디버깅을 위해 매핑에도 날짜 메타가 필요하다.

주의:
- 현재 `df_news`는 `pub_date` 정렬 후 index를 만들므로, `pub_ts`는 그 순서와 정확히 align되어야 한다.

### 4.2 `dags/modules/training/data_loader.py`

핵심 변경:
1. `RandomLinkSplit` 대체 함수 신규 구현(예: `temporal_link_split`).
2. `preprocess_data(...)`를 `split_mode` 분기형으로 변경:
- `split_mode=random`면 기존 동작 유지(롤백 안전장치)
- `split_mode=temporal`면 신규 분할 실행
3. 분할 순서 재배치:
- (기존) 전체 그래프 전처리 -> 랜덤 split
- (변경) 전체 그래프 전처리(빈도/PMI) -> temporal split으로 train/val/test edge 분리

세부 구현 포인트:
1. temporal mask 생성
- 대상 edge type: `('news','has_keyword','keyword')`, `('news','has_stock','stock')`
- `src_news_idx`로 `data['news'].pub_ts[src_news_idx]`를 조회해 edge timestamp 구성
2. 경계 정의
- 옵션 A: 비율 기반(시간 축 quantile)
- 옵션 B: 날짜 기반(`train_end_date`, `val_end_date`)
- 운영 안정성 기준으로 옵션 B 권장
3. split 결과 구성
- `train_data[et].edge_index`: train positive edges
- `val_data[et].edge_index`: 기본은 전처리 완료된 전체 그래프 사용(노이즈 제거 우선 정책)
- `test_data[et].edge_index`: 기본은 전처리 완료된 전체 그래프 사용(노이즈 제거 우선 정책)
- 각 split에 `edge_label_index`/`edge_label` 생성(positive+negative)
4. negative sampling
- split별로 목적지 타입별 샘플링
- seed 고정
5. 노드 필터/PMI
- temporal split 이전 전체 컷오프 그래프 기준으로 계산
- 필터로 제거된 노드에 걸리는 split 라벨은 제외 또는 remap

주의:
- 현재 엔진은 `data[et].edge_label_index`와 `edge_label`을 직접 사용하므로, 필드 shape/타입을 현재와 동일하게 맞춰야 함.

### 4.3 `dags/modules/training/trainer.py`

변경:
1. `preprocess_data` 호출 시 split 관련 config 전달.
2. MLflow에 split 설정을 파라미터/태그로 로그(`split_mode`, `train_end_date`, `val_end_date` 등).

이유:
- 실험 비교 시 random vs temporal 결과를 명확히 구분해야 한다.

### 4.4 `dags/modules/training/config.json`

신규 권장 키:
```json
{
  "training": {
    "split_mode": "temporal",
    "split_policy": "date",
    "train_end_date": "2026-01-31",
    "val_end_date": "2026-02-15",
    "negative_ratio": 1.0,
    "temporal_message_passing": "full_graph"
  }
}
```

설명:
- `split_mode`: `random | temporal`
- `split_policy`: `date | ratio`
- `temporal_message_passing`: `full_graph | train_only` (현재 정책은 `full_graph`)

### 4.5 `dags/gnn_training_dag.py` (선택)

선택 변경:
- Airflow params에 split 관련 값을 노출하면 운영자가 DAG 실행 시 경계를 바꿀 수 있다.
- 단, 운영 복잡도 증가를 피하려면 config.json 고정값부터 적용하는 것이 안전하다.

## 5) 구현 순서 (권장)

1. `graph_builder.py`에 `news.pub_ts` 저장 추가
2. trainset 재생성 (`gnn_trainset_creation`)
3. `data_loader.py`에 전처리 유지 + temporal split 구현
4. `trainer.py`/`config.json` 연결
5. 회귀 테스트 및 누수 점검

## 6) 검증 체크리스트

### 6.1 데이터 무결성
- `hetero_graph.pt` 로드 후 `data['news'].pub_ts` 존재 여부
- `len(data['news'].pub_ts) == data['news'].num_nodes`

### 6.2 시간 경계 검증
- train positive edge의 최대 시각 <= `train_end_date`
- val positive edge는 `(train_end_date, val_end_date]`
- test positive edge는 `> val_end_date`

### 6.3 허용 누수 범위 검증
- 전처리(빈도/PMI)가 split 이전 전체 컷오프 데이터에 대해 수행되는지 확인
- 라벨 분리만큼은 시간 경계(`train_end_date`, `val_end_date`)를 정확히 지키는지 확인
- 실험 메타에 `temporal_message_passing=full_graph`가 기록되는지 확인

### 6.4 실험 추적
- MLflow run에 split 파라미터/태그가 모두 기록되는지 확인

## 7) 리스크 및 대응

1. 리스크: temporal 분할 후 train edge 수 급감
- 대응: 기간 재설정, 최소 edge 수 가드(미달 시 fail-fast)

2. 리스크: 필터링 후 val/test 라벨이 과도하게 제거
- 대응: 노드 필터 임계치 완화(`min_freq_keyword`, `min_freq_stock`) 또는 평가 집합 최소량 체크

3. 리스크: 기존 성능 지표와 단절
- 대응: `split_mode=random` fallback 유지로 병행 비교 기간 운영

## 8) 결론

- `pub_date`는 원천에는 있으나 현재 학습 입력(`hetero_graph.pt`)에는 보존되지 않는다.
- 현재는 랜덤 링크 분할 + 분할 전 전체 통계 사용 구조라 시계열 누수 위험이 실질적으로 존재한다.
- 정책상 전처리 누수는 허용하고, 라벨 split만 temporal로 강제한다.
- 핵심 수정 포인트는 `graph_builder.py`(시간 정보 보존)와 `data_loader.py`(전처리 유지 + temporal split)이며, `trainer.py/config.json`은 연결 및 운영 제어를 담당한다.
