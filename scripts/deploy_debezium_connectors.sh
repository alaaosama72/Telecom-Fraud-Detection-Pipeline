#!/bin/bash

# ========================================
# Deploy Debezium CDC Connector
# ========================================
# This script deploys the PostgreSQL CDC connector to Debezium Connect
#
# Prerequisites:
# 1. Docker containers running (docker-compose up -d)
# 2. PostgreSQL data loaded (python scripts/load_data_to_postgres.py)
# 3. Debezium Connect ready (check http://localhost:8083)
# ========================================

echo "========================================="
echo "🚀 Deploying Debezium CDC Connector"
echo "========================================="

# Wait for Debezium Connect to be ready
echo ""
echo "⏳ Waiting for Debezium Connect to be ready..."
until curl -s http://localhost:8083/ > /dev/null; do
    echo "  Waiting for Debezium Connect..."
    sleep 5
done

echo "✅ Debezium Connect is ready!"
echo ""

# Check existing connectors
echo "📋 Checking existing connectors..."
EXISTING=$(curl -s http://localhost:8083/connectors | grep -o "postgres-telecom-cdc-connector" || true)

if [ -n "$EXISTING" ]; then
    echo "⚠️  Connector already exists. Deleting..."
    curl -X DELETE http://localhost:8083/connectors/postgres-telecom-cdc-connector
    sleep 2
    echo "✅ Old connector deleted"
fi

echo ""
echo "📤 Deploying PostgreSQL CDC connector..."

# Deploy the connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-connectors/postgres-cdc-connector.json

echo ""
echo ""
echo "========================================="
echo "✅ Deployment Complete!"
echo "========================================="

# Wait a moment for connector to initialize
sleep 3

# Check connector status
echo ""
echo "📊 Connector Status:"
STATUS=$(curl -s http://localhost:8083/connectors/postgres-telecom-cdc-connector/status)

# Try to format JSON nicely, but don't fail if tools aren't available
if command -v jq &> /dev/null; then
    echo "$STATUS" | jq .
elif command -v python3 &> /dev/null; then
    echo "$STATUS" | python3 -m json.tool
elif command -v python &> /dev/null; then
    echo "$STATUS" | python -m json.tool
else
    echo "$STATUS"
fi

echo ""
echo ""
echo "========================================="
echo "🔍 Verification Steps"
echo "========================================="
echo ""
echo "1️⃣  Check connector status:"
echo "   curl http://localhost:8083/connectors/postgres-telecom-cdc-connector/status"
echo ""
echo "2️⃣  List CDC topics in Kafka:"
echo "   docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092 | grep telecom"
echo ""
echo "3️⃣  Consume CDC events (customer profiles):"
echo "   docker exec kafka-1 kafka-console-consumer \\"
echo "     --topic telecom.crm_schema.customer_profiles \\"
echo "     --bootstrap-server localhost:9092 \\"
echo "     --from-beginning --max-messages 5"
echo ""
echo "4️⃣  Test UPDATE event:"
echo "   docker exec -it postgres-telecom psql -U postgres -d telecom -c \\"
echo "     \"UPDATE crm_schema.customer_profiles SET credit_score = 800 WHERE customer_id = 'CUST001';\""
echo ""
echo "========================================="
