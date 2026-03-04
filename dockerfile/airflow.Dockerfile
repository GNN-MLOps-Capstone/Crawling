FROM apache/airflow:2.10.5

# 파일명을 명시해서 복사
COPY requirements/requirements_airflow.txt /requirements_airflow.txt

# 복사한 파일로 설치
RUN pip install --no-cache-dir -r /requirements_airflow.txt

USER root
# 가상환경이 위치할 폴더 생성 및 소유권 변경
RUN mkdir -p /opt/airflow/venv_nlp && chown -R airflow: /opt/airflow/venv_nlp

USER airflow

COPY --chown=airflow:0 requirements/requirements_nlpvenv.txt /tmp/requirements_nlpvenv.txt
COPY --chown=airflow:0 init-scripts/setup_nlp_venv.sh /tmp/setup_nlp_venv.sh

RUN chmod +x /tmp/setup_nlp_venv.sh && /bin/bash /tmp/setup_nlp_venv.sh