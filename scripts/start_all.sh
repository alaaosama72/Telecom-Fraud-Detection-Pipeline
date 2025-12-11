#!/bin/bash

# =================================================================
# 🚀 Telecom Fraud Detection System - Master Startup Script
# =================================================================

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Telecom Fraud Detection System...${NC}"
echo "-----------------------------------------------------"

# 0. Environment Setup
mkdir -p logs

# Activate Virtual Environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# 1. Start Infrastructure (Docker)
echo -e "\n${YELLOW}[1/5] Starting Infrastructure (Docker)...${NC}"
docker-compose up -d
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Docker Compose failed to start.${NC}"
    exit 1
fi
echo "✅ Infrastructure is up."

# 2. Wait for Kafka & Initialize Topics
# init_kafka.py has built-in retry logic to wait for brokers
echo -e "\n${YELLOW}[2/5] Waiting for Kafka & Initializing Topics...${NC}"
echo "   (This may take a minute if Kafka is restarting)"
python src/source/init_kafka.py
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to initialize Kafka.${NC}"
    exit 1
fi

# 3. Initialize ClickHouse Database
echo -e "\n${YELLOW}[3/5] Initializing ClickHouse Database...${NC}"
python src/source/init_clickhouse.py
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to initialize ClickHouse.${NC}"
    exit 1
fi

# 4. Start Action Consumer (Background)
echo -e "\n${YELLOW}[4/5] Starting Action Consumer...${NC}"
# Kill existing consumer if running
pkill -f "src/processing/action_consumer.py" 2>/dev/null

python src/processing/action_consumer.py > logs/action_consumer.log 2>&1 &
ACTION_PID=$!
echo "✅ Action Consumer started (PID: $ACTION_PID)"
echo "   Logs: logs/action_consumer.log"

# 5. Start Spark Pipeline (Producer + Spark Job)
echo -e "\n${YELLOW}[5/5] Starting Spark Pipeline...${NC}"
echo "   Launching run_spark.sh..."

# We use the existing run_spark.sh but we need to trap signals to kill the action consumer too
cleanup_all() {
    echo -e "\n${RED}🛑 Stopping all components...${NC}"
    kill $ACTION_PID 2>/dev/null
    # run_spark.sh handles its own cleanup when interrupted, 
    # but since we exec it, this trap might not trigger unless we run it in background and wait.
    # Simpler approach: Just let run_spark.sh take over the foreground.
    # If user Ctrl+C's run_spark.sh, we want to ensure action_consumer is also killed.
}

trap cleanup_all SIGINT SIGTERM

# Run run_spark.sh in background so we can trap signals in this script?
# Actually, run_spark.sh is interactive (tails logs). 
# Let's run it and wait.
bash scripts/run_spark.sh &
SPARK_SCRIPT_PID=$!

wait $SPARK_SCRIPT_PID

# When run_spark.sh exits (user pressed Ctrl+C there), we kill action consumer
echo -e "\n${YELLOW}Shutting down Action Consumer...${NC}"
kill $ACTION_PID 2>/dev/null
echo -e "${GREEN}System Stopped.${NC}"
