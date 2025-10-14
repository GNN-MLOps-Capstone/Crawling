#!/bin/bash
# scripts/query-db.sh

QUERY=$1

if [ -z "$QUERY" ]; then
    echo "Usage: ./scripts/query-db.sh 'SELECT * FROM naver_news LIMIT 10;'"
    exit 1
fi

docker exec news-data-postgres psql -U dobi -d newscapstone -c "$QUERY"
