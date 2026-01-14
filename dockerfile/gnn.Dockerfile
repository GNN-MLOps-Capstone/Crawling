# 1. Base Image: PyTorch 공식 Runtime 이미지
FROM pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime

# 2. 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    git \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 3. 작업 디렉토리 설정
WORKDIR /app

# 4. PyTorch Geometric (PyG) 및 의존성 설치
RUN pip install --no-cache-dir \
    pyg_lib \
    torch_scatter \
    torch_sparse \
    torch_cluster \
    torch_spline_conv \
    torch_geometric \
    -f https://data.pyg.org/whl/torch-2.8.0+cu129.html

# 5. 기타 필수 라이브러리 설치 (Airflow 파이프라인용)
# s3fs: MinIO 연동
# pandas, pyarrow: 데이터 처리
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    s3fs \
    pyarrow \
    pendulum \
    boto3 \
    mlflow \
    psycopg2-binary

# 6. PYTHONPATH 설정
ENV PYTHONPATH=/app