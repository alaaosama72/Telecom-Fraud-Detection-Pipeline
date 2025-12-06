# System Restart Guide - Fresh Dashboard Demo

## Prerequisites Check
```powershell
# Verify Docker containers are running
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Expected: kafka-1, kafka-2, kafka-3, clickhouse, grafana - all UP

---

## Step 1: Clear Old Data (Optional - For Fresh Start)

```powershell
# Clear ClickHouse fraud_alerts table
docker exec clickhouse clickhouse-client --user admin --password admin123 --query "TRUNCATE TABLE telecom_fraud.fraud_alerts"

# Verify it's empty
docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM telecom_fraud.fraud_alerts"
```
Expected: `0`

---

## Step 2: Start Speed Layer (WSL Terminal #1)

Open **WSL** terminal:

```bash
cd /mnt/d/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline
bash scripts/run_spark.sh
```

**You'll see:**
```
Starting Kafka Producer...
Starting Spark Stream Processor...
[Producer] Sent 1000 CDR records...
[Batch 0] Processing 15 alerts...
[Batch 0] Sent alerts to Kafka action topic
```

**Keep this terminal open** - let it run continuously.

---

## Step 3: Start Action Consumer (Windows PowerShell #2)

Open a **new PowerShell** terminal:

```powershell
cd d:\ITI-Data_Engineer\Projects\Telecom-Fraud-Detection-Pipeline
python src/processing/action_consumer.py
```

**You'll see:**
```
Starting Action Consumer on topic: telecom-fraud-actions
Waiting for fraud alerts...
[ACTION] 🚫 BLOCKED User CUST001 | Reason: SIM Box / Gateway Bypass
[ACTION] 🚫 BLOCKED User CUST045 | Reason: Credit Limit Abuse
```

**Keep this terminal open** - watch the blocking messages appear.

---

## Step 4: Watch the Dashboard

Open browser: **http://localhost:3000/d/fraud-detection/telecom-fraud-detection-real-time-dashboard**

**What you'll see (auto-refreshes every 5 seconds):**

1. **Time Series Chart** - Line graph showing fraud alerts over time by type
2. **Gauge** - Total number of alerts in the last hour
3. **Pie Chart** - Distribution of fraud types (24h)
4. **Top 10 Table** - Users with most fraud alerts
5. **Live Feed** - Scrolling table of most recent 100 alerts

---

## Verification Commands (PowerShell #3)

Open a **third terminal** to monitor the system:

```powershell
# Check alert count (should be increasing)
docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM telecom_fraud.fraud_alerts"

# Check most recent alerts
docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT timestamp, caller_number, alert_type FROM telecom_fraud.fraud_alerts ORDER BY timestamp DESC LIMIT 5"

# Check Kafka action topic
docker exec kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic telecom-fraud-actions --from-beginning --max-messages 5
```

---

## Expected Timeline

| Time | What Happens |
|------|--------------|
| **0:00** | Start Spark - Producer begins sending data to Kafka |
| **0:05** | First fraud alerts detected and written to ClickHouse |
| **0:10** | Dashboard starts showing data (first time series points) |
| **0:15** | Action Consumer starts receiving and "blocking" fraudsters |
| **0:30** | Gauge shows ~50-100 alerts, pie chart populating |
| **1:00+** | All panels fully populated with rich data |

---

## Troubleshooting

### Dashboard still shows "No data"
1. Refresh with `Ctrl+F5`
2. Check datasource: http://localhost:3000/connections/datasources/edit/PDEE91DDB90597936
3. Click "Save & test" - should say "Data source is working"

### No alerts being generated
1. Check Spark logs in WSL terminal for errors
2. Verify Kafka topics exist:
   ```powershell
   docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
   ```

### Action Consumer not showing blocks
1. Verify Spark is writing to action topic
2. Check Kafka topic has messages:
   ```powershell
   docker exec kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic telecom-fraud-actions --from-beginning --max-messages 1
   ```

---

## Stopping the System

**Stop Spark (WSL):** Press `Ctrl+C` in the Spark terminal

**Stop Action Consumer (PowerShell):** Press `Ctrl+C` in the consumer terminal

**Stop Docker (if needed):**
```powershell
docker-compose down
```

**Restart Docker later:**
```powershell
docker-compose up -d
```
