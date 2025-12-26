# GPU 지원을 위해 NVIDIA CUDA 공식 이미지를 베이스로 사용
FROM nvidia/cuda:12.8.1-cudnn-devel-ubuntu24.04

# 기본 유틸리티 설치
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    software-properties-common \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Python 3.13 설치
RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y python3.13 python3.13-venv python3.13-dev

# python3.13을 기본 python 명령어로 설정
RUN ln -sf /usr/bin/python3.13 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.13 /usr/bin/python

# pip 설치 (ensurepip 사용)
RUN python -m ensurepip --upgrade && \
    python -m pip install --upgrade pip

# PyTorch 설치
RUN pip install --no-cache-dir \
    torch==2.8.0 \
    torchvision==0.23.0 \
    torchaudio==2.8.0 \
    --index-url https://download.pytorch.org/whl/cu128

# PyG 의존성 설치
RUN pip install --no-cache-dir \
    pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv \
    -f https://data.pyg.org/whl/torch-2.8.0+cu128.html

# PyG 메인 라이브러리 설치
RUN pip install --no-cache-dir torch_geometric

# 제약 조건 파일 생성
RUN echo "torch==2.8.0" > /tmp/constraints.txt

# Jupyter 및 필수 라이브러리 설치
RUN pip install --no-cache-dir jupyterlab notebook
COPY /requirements/requirements_jupyter.txt /tmp/requirements_jupyter.txt
RUN pip install --no-cache-dir --ignore-installed \
    -r /tmp/requirements_jupyter.txt \
    -c /tmp/constraints.txt

# 작업 디렉토리 설정
WORKDIR /home/jovyan/work

# Jupyter 실행 (외부 접속 허용, 루트 권한 허용)
CMD jupyter lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --NotebookApp.token="${JUPYTER_TOKEN}"