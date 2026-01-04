-- MLflow를 위한 전용 데이터베이스 생성
CREATE DATABASE mlflow_db;

-- 권한 부여 (유저에게 DB 모두 접근 권한 부여)
GRANT ALL PRIVILEGES ON DATABASE mlflow_db TO current_user;