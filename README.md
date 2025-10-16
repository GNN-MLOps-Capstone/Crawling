## 🎯 프로젝트 개요
## 🛠 기술 스택
## 📁 프로젝트 구조
## 🏗 데이터 파이프라인
## 🚀 핵심 기능
## 💻 개발 가이드

---

### 브랜치 전략

main (프로덕션)
↓
develop (개발)
↓
feature/기능명 (기능 개발)

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






