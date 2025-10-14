# 1. 사용할 Airflow 공식 이미지를 기반으로 시작합니다.
# 버전을 사용자의 환경에 맞게 지정해주세요.
FROM apache/airflow:3.1.0

# 2. apt-get 업데이트 및 curl 설치
USER root
RUN apt-get update && apt-get install -y curl && apt-get clean
USER airflow

# 3. uv 설치 스크립트를 실행합니다.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# 4. uv를 PATH에 추가하여 바로 사용할 수 있게 합니다.
ENV PATH="/home/airflow/.local/bin:$PATH"

# 5. 스크립트 실행에 필요한 라이브러리 목록 파일을 복사합니다.
COPY requirements.txt .

# 6. install package
RUN uv pip install --no-cache-dir -r requirements.txt
