#!/bin/bash

# =================================================================
# 🧹 Telecom Fraud Detection System - CLEAN RESET & START
# =================================================================
# WARNING: This will DELETE ALL DATA in Kafka and ClickHouse!
# =================================================================

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}⚠️  WARNING: This will DELETE ALL DATA in Kafka and ClickHouse! ⚠️${NC}"
read -p "Are you sure you want to proceed? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Operation cancelled."
    exit 1
fi

echo -e "\n${YELLOW}[1/2] Stopping containers and removing volumes (Cleaning Data)...${NC}"
# -v flag removes named volumes declared in the `volumes` section of the Compose file
docker-compose down -v

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to stop/clean Docker containers.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ System cleaned successfully.${NC}"

echo -e "\n${YELLOW}[2/2] Starting fresh system...${NC}"
bash scripts/start_all.sh
