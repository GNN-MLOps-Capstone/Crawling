#!/bin/bash

echo "🧪 Testing connection to News Data Database..."

SCHEDULER_CONTAINER=$(docker ps --filter "name=scheduler" --format "{{.Names}}" | head -1)

if [ -z "$SCHEDULER_CONTAINER" ]; then
    echo "❌ Airflow scheduler container not found!"
    exit 1
fi

# Connection 테스트
echo "Testing Airflow connection..."
docker exec $SCHEDULER_CONTAINER airflow connections test news_data_db

echo ""
echo "📊 Database info:"
docker exec news-data-postgres psql -U dobi -d newscapstone -c "
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;
"

echo ""
echo "📈 Row counts:"
docker exec news-data-postgres psql -U dobi -d newscapstone -c "
SELECT
    'naver_news' as table_name,
    COUNT(*) as row_count
FROM naver_news
UNION ALL
SELECT
    'crawled_news' as table_name,
    COUNT(*) as row_count
FROM crawled_news
UNION ALL
SELECT
    'filtered_news' as table_name,
    COUNT(*) as row_count
FROM filtered_news;
"
