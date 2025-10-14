#!/bin/bash

echo "🗄️  Starting News Data Database..."

# 네트워크 생성 (Airflow와 같은 네트워크 사용)
docker network create ml-engineer-network 2>/dev/null || echo "✓ Network already exists"

# 데이터베이스 시작
cd infrastructure
docker-compose -f docker-compose.db.yaml up -d
cd ..

echo "⏳ Waiting for database to be ready..."
sleep 10

# 데이터베이스 상태 확인
docker exec news-data-postgres pg_isready -U dobi -d newscapstone

# 테이블 확인
echo ""
echo "📋 Database tables:"
docker exec news-data-postgres psql -U dobi -d newscapstone -c "\dt"

echo ""
echo "✅ News Data Database is ready!"
echo ""
echo "📊 Connection info:"
echo "   Host: localhost"
echo "   Port: 5433"
echo "   Database: newscapstone"
echo "   User: dobi"
echo "   Password: (check infrastructure/.env file)"
