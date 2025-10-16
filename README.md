## 🎯 프로젝트 개요
## 🛠 기술 스택
## 📁 프로젝트 구조
## 🏗 데이터 파이프라인
## 🚀 핵심 기능
## 💻 개발 가이드

---

### 브랜치 전략
```
main (프로덕션)
↓
develop (개발)
↓
feature/기능명 (기능 개발)
```

**브랜치 정책**

main : 배포/운영용 안정 브랜치 (보호 설정, 직접 커밋 금지)

develop : 개발 통합 브랜치 (모든 기능 브랜치 머지 대상)

feature/* : 개별 기능/작업 단위 브랜치

### 커밋 컨벤션

feat: 새로운 기능

fix: 버그 수정

refactor: 코드 리팩토링

style: 코드 포맷팅

docs: 문서 수정

test: 테스트 코드

build: 빌드, 의존성 코드

chore: 기타 작업

예시:
```
git commit -m "feat: 주가 예측 기능 추가"
git commit -m "fix:  Airflow 버그 수정"
git commit -m "refactor: 필터링 코드 수정"
```

### 🔁 PR 규칙

**PR 대상**
- `feature/*` → `develop`

**PR 제목**
- 커밋 컨벤션과 동일하게 작성 (예: `feat: 뉴스 적재 DAG 추가`)

**본문**
- 목적

변경 이유 요약 (1줄)

- 주요 변경사항

핵심 변경 2~3개 bullet

- 예시:
```
## 주요 변경사항
- Airflow DAG에서 Postgres 연결 추가
- ingest_news_csv 태스크 분리 (create_table / copy_csv)
- QC 로그 파일 자동 저장 기능 추가
```

- 테스트

[ ]로컬 실행 확인
[ ]유닛테스트 통과

or

로컬에서 실행해봤고 정상 동작함 ✅

**리뷰**

모든 PR은 최소 1명 리뷰 후 머지 (자기 코드 단독 머지 금지)

300 lines 이상 변경 시, 해당 기능 담당자 또는 PM이 승인 필수

리뷰 목적은 코드 품질 검증보다 **공유 및 확인 중심**











