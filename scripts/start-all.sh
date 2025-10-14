#!/bin/bash

echo "🚀 Starting ML Engineer Infrastructure..."

# 1. 네트워크 생성
echo "📡 Creating network..."
docker network create ml-engineer-network 2>/dev/null || echo "✓ Network exists"

# 2. 데이터베이스 시작
echo ""
echo "🗄️  Starting News Data Database..."
cd infrastructure
docker-compose -f docker-compose.db.yaml up -d
cd ..
sleep 10

# 3. Airflow 시작
echo ""
echo "✈️  Starting Airflow..."
docker-compose up -d
sleep 20

# 4. Connection 설정
echo ""
./scripts/setup-airflow-connection.sh

# 5. 상태 확인
echo ""
echo "📊 Services Status:"
echo ""
echo "News Data Database:"
docker ps --filter "name=news-data-postgres" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "Airflow Services:"
docker ps --filter "name=airflow" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "✅ All services started!"
echo ""
echo "📍 Access points:"
echo "   - Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "   - News DB: localhost:5433 (dobi/check infrastructure/.env)"
echo ""
echo "🔧 Useful commands:"
echo "   - View Airflow logs: docker-compose logs -f"
echo "   - View DB logs: docker logs news-data-postgres -f"
echo "   - Stop all: ./scripts/stop-all.sh"
echo "   - Test connection: ./scripts/test-connection.sh"
echo ""
echo "🧪 Test the setup:"
echo "   ./scripts/test-connection.sh"
