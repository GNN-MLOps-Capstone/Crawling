FROM apache/airflow:2.10.5

# 파일명을 명시해서 복사
COPY requirements_airflow.txt /requirements_airflow.txt

# 복사한 파일로 설치
RUN pip install --no-cache-dir -r /requirements_airflow.txt