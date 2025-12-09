# 🚀 Quick Start Guide - Full Production CDC Setup

## 📋 Prerequisites

- Docker Desktop running
- Python 3.8+ with pandas and psycopg2
- WSL2 (for Spark - optional for now)
- 8GB+ RAM available

---

## 🎯 Step-by-Step Setup

### **Step 1: Start Infrastructure** (5 minutes)

```powershell
cd d:\ITI-Data_Engineer\Projects\Telecom-Fraud-Detection-Pipeline

# Start all services
docker-compose up -d

# Wait for services to initialize (30-60 seconds)
Start-Sleep -Seconds 60

# Verify all containers are running
docker ps
```

**You should see 7 containers:**
- kafka-1, kafka-2, kafka-3
- postgres-telecom
- debezium-connect
- clickhouse
- grafana

---

### **Step 2: Load Data into PostgreSQL** (5 minutes)

```powershell
# Install Python dependencies (if not already installed)
pip install pandas psycopg2-binary

# Load CSV data into PostgreSQL
python  scripts\load_data_to_postgres.py
```

**Expected output:**
```
✅ Inserted 1000 customer profiles
✅ Inserted 1000 device records
✅ Inserted 1000 payment transactions
✅ Inserted 1000 behavior records
✅ Inserted 1000 complaint records
```

**Verify data in PostgreSQL:**
```powershell
docker exec -it postgres-telecom psql -U postgres -d telecom -c "SELECT COUNT(*) FROM crm_schema.customer_profiles;"
```

---

### **Step 3: Deploy Debezium CDC Connector** (3 minutes)

**Windows PowerShell:**
```powershell
# Using curl for Windows (if you have it)
Invoke-RestMethod -Uri "http://localhost:8083/" -Method Get

# Deploy connector
$connector = Get-Content debezium-connectors\postgres-cdc-connector.json | ConvertFrom-Json
$connectorJson = $connector | ConvertTo-Json -Depth 10
Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -Body $connectorJson -ContentType "application/json"
```

**OR in WSL:**
```bash
bash scripts/deploy_debezium_connectors.sh
```

**Verify connector status:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-telecom-cdc-connector/status" -Method Get
```

---

### **Step 4: Verify CDC Topics in Kafka** (2 minutes)

```powershell
# List all topics (you should see CDC topics)
docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Expected CDC topics:
# - telecom.crm_schema.customer_profiles
# - telecom.crm_schema.device_information
# - telecom.billing_schema.payment_transactions
# - telecom.billing_schema.customer_behavior
# - telecom.care_schema.complaints_feedback
```

**Consume a CDC event to see the format:**
```powershell
docker exec kafka-1 kafka-console-consumer `
  --topic telecom.crm_schema.customer_profiles `
  --bootstrap-server localhost:9092 `
  --from-beginning --max-messages 1
```

---

### **Step 5: Test CDC Live Updates** (5 minutes)

**Update a customer record in PostgreSQL:**
```powershell
docker exec -it postgres-telecom psql -U postgres -d telecom
```

**In the PostgreSQL prompt:**
```sql
-- Update customer credit score
UPDATE crm_schema.customer_profiles 
SET credit_score = 800 
WHERE customer_id = 'CUST001';

-- Check the update
SELECT customer_id, credit_score, updated_at 
FROM crm_schema.customer_profiles 
WHERE customer_id = 'CUST001';

-- Exit
\q
```

**Verify CDC captured the UPDATE:**
```powershell
docker exec kafka-1 kafka-console-consumer `
  --topic telecom.crm_schema.customer_profiles `
  --bootstrap-server localhost:9092 `
  --from-beginning --max-messages 10
```

You should see the CDC event with the updated credit_score!

---

### **Step 6: Initialize Kafka Topics for Stream Data** (1 minute)

```powershell
python src\source\init_kafka.py
```

---

### **Step 7: Initialize ClickHouse** (1 minute)

```powershell
python src\source\init_clickhouse.py
```

---

## ✅ Verification Checklist

| Component | Check | Command |
|:----------|:------|:--------|
| **Kafka Cluster** | 3 brokers running | `docker ps | grep kafka` |
| **PostgreSQL** | Database accessible | `docker exec postgres-telecom psql -U postgres -d telecom -c "\dt *.*"` |
| **Debezium** | Connector running | `Invoke-RestMethod http://localhost:8083/connectors` |
| **CDC Topics** | 5 topics exist | `docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092 | grep telecom` |
| **ClickHouse** | Database ready | `Invoke-RestMethod http://localhost:8123/ping` |
| **Grafana** | Dashboard accessible | Open http://localhost:3000 (admin/admin) |

---

## 🎯 What You've Built

### STREAM Layer (Network Events)
- Still uses CSV → Kafka via `producer.py`
- 6 topics: cdr, sdr, location, network, security, usage

### CDC Layer (Database Changes) ✨ NEW
- PostgreSQL databases with 5 tables
- Debezium capturing INSERT/UPDATE/DELETE
- 5 CDC topics with change events

### Architecture
```
Network Events (CSV) → Kafka → Spark Streaming
Database Changes (PG) → Debezium → Kafka → Spark Streaming
Both streams → Enrichment → Fraud Detection → ClickHouse → Grafana
```

---

## 🔥 Next Steps

### **Phase 2: Update Spark to Consume CDC** (Coming Next)

You'll update `stream_processor.py` to:
1. Read CDC topics from Kafka
2. Parse Debezium CDC format
3. Join STREAM data (CDR) with CDC data (customer_profiles)
4. Enhance fraud detection with real credit_score, plan_type, etc.

---

## 🐛 Troubleshooting

### Debezium not connecting to PostgreSQL
```powershell
# Check PostgreSQL is accessible from Debezium
docker exec debezium-connect curl postgres:5432
```

### CDC topics not created
```powershell
# Check Debezium connector logs
docker logs debezium-connect --tail 100
```

### PostgreSQL replication issues
```sql
-- Check replication slots
SELECT * FROM pg_replication_slots;

-- Check publication
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';
```

---

## 📚 Reference

- **PostgreSQL**: `localhost:5432` (postgres/admin123)
- **Debezium API**: `localhost:8083`
- **Kafka Brokers**: `localhost:9092, 9093, 9094`
- **ClickHouse**: `localhost:8123` (admin/admin123)
- **Grafana**: `localhost:3000` (admin/admin)

---

**🎉 Congratulations! You now have a production-grade CDC setup!**
