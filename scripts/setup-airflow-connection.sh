#!/bin/bash

echo "🔗 Setting up Airflow connection to News Data Database..."

# .env에서 비밀번호 읽기
if [ -f infrastructure/.env ]; then
    source infrastructure/.env
else
    echo "❌ infrastructure/.env not found!"
    exit 1
fi

# Airflow scheduler 컨테이너 이름 찾기
SCHEDULER_CONTAINER=$(docker ps --filter "name=scheduler" --format "{{.Names}}" | head -1)

if [ -z "$SCHEDULER_CONTAINER" ]; then
    echo "❌ Airflow scheduler container not found!"
    echo "Please start Airflow first."
    exit 1
fi

# Connection 추가
docker exec $SCHEDULER_CONTAINER airflow connections add news_data_db \
    --conn-type postgres \
    --conn-host news-data-postgres \
    --conn-login dobi \
    --conn-password "$DATA_DB_PASSWORD" \
    --conn-port 5432 \
    --conn-schema newscapstone \
    --conn-description "News capstone data storage database" \
    2>/dev/null && echo "✅ Connection created" || echo "⚠️  Connection already exists"

# Connection 확인
echo ""
echo "📋 Verifying connection:"
docker exec $SCHEDULER_CONTAINER airflow connections get news_data_db

echo ""
echo "✅ Connection setup complete!"
echo "   Connection ID: news_data_db"
echo "   Database: newscapstone"
echo "   User: dobi"
echo "   You can now use this in your DAGs"
