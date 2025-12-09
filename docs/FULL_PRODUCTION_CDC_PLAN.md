# Full Production CDC Setup - Implementation Plan
## Telecom Fraud Detection Pipeline with Real Debezium CDC

---

## 🎯 Project Goal

Transform the current CSV-based system into a **production-grade architecture** with:
- ✅ Real MySQL & PostgreSQL databases
- ✅ Debezium CDC connectors
- ✅ Confluent Schema Registry
- ✅ Automated database population
- ✅ Stream enrichment with CDC data
- ✅ Production-ready monitoring

**Estimated Time:** 20-30 hours  
**Complexity:** Advanced  
**Result:** Interview/portfolio showcase piece

---

## 📊 Target Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     STREAM LAYER (Real-time Network)                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  CSV Files (Network Events) → Python Producer → Kafka Topics        │
│                                                                       │
│  • telecom-cdr          (Call Detail Records)                        │
│  • telecom-sdr          (Signaling Data)                             │
│  • telecom-location     (Location Updates)                           │
│  • telecom-network      (Network Metrics)                            │
│  • telecom-security     (Security Events)                            │
│  • telecom-service      (Service Usage)                              │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      CDC LAYER (Database Changes)                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  MySQL Database (CRM)                                                │
│    └─ customer_profiles → Debezium → cdc.crm.customer_profiles      │
│    └─ device_information → Debezium → cdc.crm.device_information    │
│                                                                       │
│  PostgreSQL Database (Billing)                                       │
│    └─ payment_transactions → Debezium → cdc.billing.payments        │
│    └─ customer_behavior → Debezium → cdc.billing.behavior           │
│                                                                       │
│  PostgreSQL Database (Care)                                          │
│    └─ complaints_feedback → Debezium → cdc.care.complaints          │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘

                              ↓ ↓ ↓

┌─────────────────────────────────────────────────────────────────────┐
│               Spark Streaming (Enrichment + Detection)              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  • Read STREAM topics (network events)                               │
│  • Read CDC topics (database changes)                                │
│  • Join/Enrich: CDR + Customer Profile                              │
│  • Fraud Detection with enriched data                                │
│  • Write to ClickHouse                                               │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Infrastructure Components

### Docker Compose Services (New)

| Service | Image | Purpose | Port |
|:--------|:------|:--------|:-----|
| **mysql** | mysql:8.0 | CRM & Device Database | 3306 |
| **postgres** | postgres:14 | Billing & Care Database | 5432 |
| **debezium-connect** | debezium/connect:2.5 | CDC Connector | 8083 |
| **schema-registry** | confluentinc/cp-schema-registry:7.5.0 | Avro Schema Management | 8081 |
| **kafka-ui** | provectuslabs/kafka-ui:latest | Kafka Monitoring (Optional) | 8080 |

### Existing Services (Keep)
- Kafka Cluster (3 brokers)
- ClickHouse
- Grafana
- Zookeeper

---

## 📋 Implementation Phases

### **Phase 1: Database Setup (3-4 hours)**

#### 1.1 Update Docker Compose
**File:** `docker-compose-full-cdc.yml`

Add services:
```yaml
services:
  # MySQL for CRM Data
  mysql:
    image: mysql:8.0
    container_name: mysql-crm
    environment:
      MYSQL_ROOT_PASSWORD: admin123
      MYSQL_DATABASE: crm
      MYSQL_USER: debezium
      MYSQL_PASSWORD: dbz123
    ports:
      - "3306:3306"
    volumes:
      - mysql-data:/var/lib/mysql
      - ./init-scripts/mysql:/docker-entrypoint-initdb.d
    command:
      - --server-id=1
      - --log-bin=mysql-bin
      - --binlog-format=ROW
      - --binlog-row-image=FULL
      - --gtid-mode=ON
      - --enforce-gtid-consistency=ON

  # PostgreSQL for Billing & Care
  postgres:
    image: postgres:14
    container_name: postgres-billing
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: admin123
      POSTGRES_DB: billing
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init-scripts/postgres:/docker-entrypoint-initdb.d
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=10
      - -c
      - max_wal_senders=10

  # Debezium Connect
  debezium:
    image: debezium/connect:2.5
    container_name: debezium-connect
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9093,kafka-3:9094
      GROUP_ID: debezium-cluster
      CONFIG_STORAGE_TOPIC: debezium_configs
      OFFSET_STORAGE_TOPIC: debezium_offsets
      STATUS_STORAGE_TOPIC: debezium_status
      CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
      - mysql
      - postgres

  # Schema Registry
  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    container_name: schema-registry
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9093,kafka-3:9094
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    depends_on:
      - kafka-1
```

#### 1.2 Create Database Init Scripts

**File:** `init-scripts/mysql/01-create-tables.sql`
```sql
-- CRM Database Tables
USE crm;

CREATE TABLE customer_profiles (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(255),
    age INT,
    gender VARCHAR(10),
    location VARCHAR(255),
    account_creation_date DATE,
    plan_type VARCHAR(50),
    monthly_spending DECIMAL(10,2),
    credit_score INT,
    avg_payment_delay INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE device_information (
    device_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50),
    device_model VARCHAR(255),
    os_version VARCHAR(50),
    imei VARCHAR(20),
    device_status VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customer_profiles(customer_id)
);

-- Enable binary logging for CDC
SET GLOBAL binlog_format = 'ROW';
```

**File:** `init-scripts/postgres/01-create-tables.sql`
```sql
-- Billing Database
CREATE TABLE IF NOT EXISTS payment_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    transaction_date TIMESTAMP,
    amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_behavior (
    behavior_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    avg_session_duration INT,
    app_usage_hours DECIMAL(5,2),
    favorite_service VARCHAR(100),
    last_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Care Database
CREATE TABLE IF NOT EXISTS complaints_feedback (
    complaint_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    complaint_type VARCHAR(100),
    severity VARCHAR(20),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

-- Enable logical replication for CDC
ALTER SYSTEM SET wal_level = 'logical';
SELECT pg_reload_conf();
```

#### 1.3 Create Data Loader Script

**File:** `scripts/load_data_to_databases.py`
```python
"""Load CSV data into MySQL and PostgreSQL databases"""
import pandas as pd
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values

# MySQL connection
mysql_conn = mysql.connector.connect(
    host='localhost',
    user='debezium',
    password='dbz123',
    database='crm'
)

# PostgreSQL connection
pg_conn = psycopg2.connect(
    host='localhost',
    database='billing',
    user='postgres',
    password='admin123'
)

# Load customer_profiles to MySQL
df = pd.read_csv('customer_profiles.csv')
# Insert into MySQL...

# Load payment_transactions to PostgreSQL
df = pd.read_csv('payment_transactions.csv')
# Insert into PostgreSQL...
```

---

### **Phase 2: Debezium Configuration (3-4 hours)**

#### 2.1 Create MySQL CDC Connector

**File:** `debezium-connectors/mysql-source-crm.json`
```json
{
  "name": "mysql-crm-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz123",
    "database.server.id": "184054",
    "database.server.name": "crm",
    "table.include.list": "crm.customer_profiles,crm.device_information",
    "database.history.kafka.bootstrap.servers": "kafka-1:9092",
    "database.history.kafka.topic": "schema-changes.crm",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "snapshot.mode": "initial"
  }
}
```

#### 2.2 Create PostgreSQL CDC Connector

**File:** `debezium-connectors/postgres-source-billing.json`
```json
{
  "name": "postgres-billing-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "admin123",
    "database.dbname": "billing",
    "database.server.name": "billing",
    "table.include.list": "public.payment_transactions,public.customer_behavior,public.complaints_feedback",
    "plugin.name": "pgoutput",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "snapshot.mode": "initial"
  }
}
```

#### 2.3 Deploy Connectors Script

**File:** `scripts/deploy_cdc_connectors.sh`
```bash
#!/bin/bash

echo "Deploying Debezium CDC Connectors..."

# MySQL Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-connectors/mysql-source-crm.json

# PostgreSQL Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-connectors/postgres-source-billing.json

echo "Connectors deployed! Check status:"
curl http://localhost:8083/connectors
```

---

### **Phase 3: Update Spark Processor (6-8 hours)**

#### 3.1 Add CDC Stream Readers

**File:** `src/processing/stream_processor_with_cdc.py`

Key additions:
```python
# Read CDC topic (Debezium format)
df_customer_cdc = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "crm.customer_profiles") \
    .load()

# Parse Debezium CDC message
# Debezium sends: {"payload": {"after": {...}, "before": {...}, "op": "c"}}
parsed_customers = df_customer_cdc.select(
    from_json(
        col("value").cast("string"),
        customer_cdc_schema
    ).alias("data")
).select("data.payload.after.*")  # Extract only the 'after' state

# Create a streaming table
parsed_customers.createOrReplaceTempView("customer_profiles_stream")
```

#### 3.2 Implement Stream-Stream Joins

```python
# Join CDR stream with customer CDC stream
enriched_cdr = parsed_cdr.alias("cdr").join(
    parsed_customers.alias("customer"),
    col("cdr.customer_id") == col("customer.customer_id"),
    "left"  # Keep all CDR records
).select(
    col("cdr.*"),
    col("customer.credit_score"),
    col("customer.plan_type"),
    col("customer.monthly_spending")
)

# Now use enriched data for fraud detection
fraud_subscription = enriched_cdr.filter(
    (col("monthly_spending") > 400) &
    (col("credit_score") < 600) &  # Real credit score from CRM!
    (col("monthly_call_duration") > 500)
)
```

---

### **Phase 4: Testing & Validation (4-5 hours)**

#### 4.1 Test CDC Data Flow
- Insert record into MySQL
- Verify Kafka topic receives CDC event
- Verify Spark processes the event

#### 4.2 Test Enrichment
- Generate CDR event
- Verify it joins with customer profile from CDC
- Verify fraud detection uses enriched data

#### 4.3 Test UPDATE Events
- Update customer credit_score in MySQL
- Verify Debezium captures UPDATE
- Verify Spark uses new value

---

### **Phase 5: Documentation (3-4 hours)**

Create comprehensive docs:
- `docs/CDC_ARCHITECTURE.md` - Full architecture explanation
- `docs/DEBEZIUM_SETUP.md` - How to deploy connectors
- `docs/DATABASE_SCHEMA.md` - Database ERD and schemas
- `docs/HOW_TO_RUN_PRODUCTION.md` - End-to-end startup guide

---

## 🚀 Execution Plan

### Week 1 (10-12 hours)
- ✅ Phase 1: Set up databases and init scripts
- ✅ Phase 2: Configure and deploy Debezium

### Week 2 (10-12 hours)
- ✅ Phase 3: Update Spark with CDC reading and enrichment
- ✅ Phase 4: Testing and validation

### Week 3 (3-5 hours)
- ✅ Phase 5: Documentation
- ✅ Final end-to-end testing
- ✅ Performance tuning

---

## 🎯 Success Criteria

By the end, you will have:

1. ✅ **Real Databases**: MySQL + PostgreSQL running in Docker
2. ✅ **CDC Capture**: Debezium tracking INSERT/UPDATE/DELETE
3. ✅ **Schema Registry**: Avro schemas managed properly
4. ✅ **Stream Enrichment**: CDR joined with real-time customer data
5. ✅ **Production Patterns**: Exactly how telecom operators build pipelines
6. ✅ **Portfolio Showcase**: Impressive demo for interviews

---

## ❓ Ready to Start?

This is a significant undertaking. I recommend we:

1. **Start with Phase 1** - Set up databases (3-4 hours)
2. **Validate** - Make sure databases are working
3. **Then Phase 2** - Deploy Debezium (3-4 hours)
4. **Validate** - Confirm CDC events flowing to Kafka
5. **Then Phase 3** - Update Spark

Should I proceed with **Phase 1: Database Setup**?

I'll create:
- `docker-compose-full-cdc.yml`
- MySQL init scripts
- PostgreSQL init scripts  
- Data loader Python script

**Ready to begin?** 🚀
