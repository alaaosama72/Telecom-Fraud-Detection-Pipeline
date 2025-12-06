# Production Dashboard Enhancement Plan

## Current State
- 5 basic panels: time series, gauge, pie chart, top users, live feed
- Suitable for demo/development
- **Missing critical production metrics**

---

## Required Production Enhancements

### 1. **Business Impact Dashboard** (NEW)

#### Panel: Estimated Fraud Value Blocked
```sql
SELECT 
  sum(duration_min * 0.05) as total_blocked_value  -- Assuming $0.05/minute
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 24 HOUR
```
**Visualization:** Stat panel with $ prefix, green threshold

#### Panel: Fraud Prevention Rate
```sql
SELECT 
  countIf(alert_type != '') as fraud_detected,
  count() as total_calls,
  (fraud_detected / total_calls) * 100 as prevention_rate
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 1 HOUR
```
**Visualization:** Gauge with target threshold

#### Panel: Hourly Trend Comparison (Today vs Yesterday)
```sql
SELECT 
  toHour(timestamp) as hour,
  toDate(timestamp) as date,
  count() as alerts
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 48 HOUR
GROUP BY hour, date
ORDER BY hour
```
**Visualization:** Time series with 2 series overlay

---

### 2. **Operational Health Dashboard** (NEW)

#### Panel: Data Freshness Indicator
```sql
SELECT 
  now() - max(timestamp) as seconds_since_last_alert
FROM telecom_fraud.fraud_alerts
```
**Visualization:** Stat panel
- Green: < 10 seconds
- Yellow: 10-60 seconds  
- Red: > 60 seconds (system may be down)

#### Panel: Alert Processing Rate
```sql
SELECT 
  toStartOfMinute(timestamp) as time,
  count() as alerts_per_minute
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY time
ORDER BY time
```
**Visualization:** Bar chart showing throughput

#### Panel: System SLA - Detection Latency
```sql
SELECT 
  quantile(0.50)(dateDiff('second', timestamp, now())) as p50,
  quantile(0.95)(dateDiff('second', timestamp, now())) as p95,
  quantile(0.99)(dateDiff('second', timestamp, now())) as p99
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 1 HOUR
```
**Visualization:** Stat panel showing P50, P95, P99 latencies

---

### 3. **Advanced Analytics Dashboard**

#### Panel: Fraud Heatmap (Hour x Day of Week)
```sql
SELECT 
  toHour(timestamp) as hour,
  toDayOfWeek(timestamp) as day,
  count() as fraud_count
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 7 DAY
GROUP BY hour, day
```
**Visualization:** Heatmap
- Reveals temporal patterns (e.g., fraud spikes at 2 AM on weekends)

#### Panel: Top Fraud Destination Numbers
```sql
SELECT 
  receiver_number,
  count() as call_count,
  groupArray(distinct caller_number) as unique_callers
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 24 HOUR
  AND receiver_number != 'N/A'
GROUP BY receiver_number
ORDER BY call_count DESC
LIMIT 10
```
**Visualization:** Table
- Identifies fraud "honeypot" numbers

#### Panel: Fraud Pattern Breakdown
```sql
SELECT 
  splitByChar('/', alert_type)[1] as fraud_category,
  count() as count
FROM telecom_fraud.fraud_alerts
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY fraud_category
ORDER BY count DESC
```
**Visualization:** Bar chart (horizontal)

---

### 4. **Alerting Configuration**

#### Critical Alerts
1. **High Fraud Volume**
   - Trigger: > 100 alerts in 5 minutes
   - Action: Page on-call engineer

2. **Data Pipeline Failure**
   - Trigger: No new alerts for 2 minutes
   - Action: Slack notification to DevOps

3. **Specific Fraud Spike**
   - Trigger: SIM Box fraud > 20 in 10 minutes
   - Action: Email security team

4. **System SLA Breach**
   - Trigger: P95 latency > 30 seconds
   - Action: Create incident ticket

#### Alert Channels
- **Slack:** `#fraud-alerts` channel
- **PagerDuty:** Critical incidents
- **Email:** Weekly summary reports
- **Webhook:** Integration with SIEM/SOC tools

---

### 5. **Dashboard Organization**

**Recommended Layout:**

```
Dashboard 1: Real-time Operations (Current)
├── Time Series: Fraud Alerts
├── Gauge: Total Alerts
├── Pie Chart: Fraud Distribution
├── Table: Top Flagged Users
└── Table: Live Feed

Dashboard 2: Business Metrics (NEW)
├── Stat: Total $ Blocked (24h)
├── Gauge: Prevention Rate
├── Time Series: Hourly Comparison
├── Stat: Average Fraud Value
└── Bar Chart: Fraud by Cost Category

Dashboard 3: Operational Health (NEW)
├── Stat: Data Freshness
├── Bar Chart: Processing Rate
├── Stat: P50/P95/P99 Latency
├── Status: System Components
└── Graph: Kafka Lag

Dashboard 4: Analytics & Insights (NEW)
├── Heatmap: Fraud by Time
├── Table: Top Destinations
├── Bar Chart: Pattern Breakdown
├── Geo Map: Fraud by Region (if data available)
└── Table: Anomaly Detection Results
```

---

## Implementation Priority

### **Phase 1: Critical (Week 1)**
✅ Data Freshness Indicator  
✅ Alert Processing Rate  
✅ Alerting Configuration  
✅ Financial Impact Tracking

### **Phase 2: Important (Week 2)**
- SLA Latency Tracking
- Hourly Trend Comparison
- Top Destination Numbers
- Operational Health Dashboard

### **Phase 3: Nice-to-Have (Week 3)**
- Fraud Heatmap
- Pattern Breakdown
- Geographic Analysis
- Advanced Anomaly Detection

---

## Data Model Changes Required

### Additional Columns Needed
```sql
ALTER TABLE telecom_fraud.fraud_alerts
ADD COLUMN fraud_value_estimate Float32 DEFAULT 0,
ADD COLUMN detection_timestamp DateTime DEFAULT now(),
ADD COLUMN action_taken String DEFAULT 'PENDING';
```

### New Tables for Advanced Analytics
```sql
CREATE TABLE telecom_fraud.fraud_metrics (
    metric_date Date,
    total_fraud_blocked UInt32,
    total_value_blocked Float32,
    avg_response_time_sec Float32,
    system_uptime_pct Float32
) ENGINE = MergeTree()
ORDER BY metric_date;
```

---

## Estimated Effort
- **Dashboard Creation:** 2-3 days
- **Alert Configuration:** 1 day
- **Data Model Updates:** 1 day
- **Testing & Validation:** 2 days
- **Documentation:** 1 day

**Total:** ~1-2 weeks for full production-ready deployment

---

## Next Steps
1. Review and approve this plan
2. Decide which dashboards to implement first
3. Set up alert notification channels (Slack, PagerDuty)
4. Implement Phase 1 critical panels
5. Load test with production-volume data
6. Train SOC/operations team
