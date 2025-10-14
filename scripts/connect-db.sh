#!/bin/bash
# scripts/connect-db.sh

echo "🔌 Connecting to News Database..."

docker exec -it news-data-postgres psql -U dobi -d newscapstone

# 나올 때는 \q 입력
