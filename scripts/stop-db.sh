# scripts/stop-db.sh
#!/bin/bash

echo "🛑 Stopping News Data Database..."

cd infrastructure
docker-compose -f docker-compose.db.yml down

echo "✅ Database stopped"

