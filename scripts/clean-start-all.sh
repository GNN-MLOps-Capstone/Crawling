#!/bin/bash
# scripts/clean-start.sh

echo "🧹 Cleaning up existing resources..."

# 모든 컨테이너 중지
docker-compose down 2>/dev/null || true
cd infrastructure && docker-compose -f docker-compose.db.yml down 2>/dev/null || true
cd ..

# 네트워크 삭제
docker network rm ml-engineer-network 2>/dev/null || true

echo "✨ Clean slate ready!"
echo ""
echo "🚀 Starting fresh..."

# 새로 시작
./scripts/start-all.sh
