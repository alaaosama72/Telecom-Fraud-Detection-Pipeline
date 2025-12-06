# Fraud Detection Dashboards - Complete User Guide

## Table of Contents
1. [Dashboard 1: Real-time Operations](#dashboard-1-real-time-operations)
2. [Dashboard 2: Business Metrics](#dashboard-2-business-metrics)
3. [Dashboard 3: Operational Health](#dashboard-3-operational-health)
4. [Dashboard 4: Analytics & Insights](#dashboard-4-analytics--insights)

---

# Dashboard 1: Real-time Operations

**URL:** http://localhost:3000/d/fraud-detection  
**Purpose:** Live fraud monitoring for SOC/Operations teams  
**Refresh Rate:** 5 seconds (real-time)  
**Best For:** Active fraud monitoring, incident response

## Panel 1: Real-time Fraud Alerts (Last Hour)
**Type:** Time Series Chart  
**Time Range:** Last 1 hour

### What It Shows
- Line graph showing fraud alert volume over time
- Each line represents a different fraud type
- Y-axis: Number of alerts
- X-axis: Time (updated every minute)

### Key Insights
✅ **Fraud Spike Detection:** Sudden vertical jumps indicate fraud attack waves  
✅ **Attack Timing:** See when fraudsters are most active  
✅ **Trend Analysis:** Is fraud increasing or decreasing?  
✅ **Pattern Recognition:** Do certain fraud types spike together?

### How to Use
- **Normal:** Steady, low-volume lines
- **Warning:** Gradual upward trend = growing fraud campaign
- **Critical:** Sudden spike = active fraud attack in progress

**Action:** If you see a spike, check Panel 5 (Live Feed) to see which users are involved.

---

## Panel 2: Total Alerts (Last Hour)
**Type:** Gauge  
**Time Range:** Last 1 hour

### What It Shows
- Single number showing total fraud alerts in the past hour
- Color-coded thresholds:
  - 🟢 Green (0-10): Normal activity
  - 🟡 Yellow (10-50): Elevated fraud
  - 🔴 Red (>50): High fraud volume

### Key Insights
✅ **System Load:** Is your fraud detection working hard?  
✅ **Quick Health Check:** Glance at this to know if things are calm or busy  
✅ **Alerting Trigger:** Use this metric to set up automated alerts

### How to Use
- **<10 alerts/hour:** Normal, low fraud activity
- **10-50 alerts/hour:** Moderate - monitor closely
- **>50 alerts/hour:** High volume - investigate immediately

**Action:** If yellow/red, drill down into Panel 3 to see which fraud types are elevated.

---

## Panel 3: Fraud Types Distribution (24h)
**Type:** Pie Chart  
**Time Range:** Last 24 hours

### What It Shows
- Breakdown of fraud alerts by type over the past day
- Each slice = one fraud type
- Size = proportion of total fraud

### Key Insights
✅ **Attack Vector Analysis:** Which fraud methods are fraudsters using most?  
✅ **Defense Priorities:** Focus resources on the largest slices  
✅ **Trend Shifts:** Compare daily to see if attack methods are changing  
✅ **Budget Allocation:** Invest in defenses for the most common fraud types

### How to Use
**Example Scenario:**
- If "SIM Box" is 60% of the pie → Fraudsters are heavily using SIM boxes
- **Action:** Increase monitoring on SIM box indicators, update blocking rules

**Action:** Click a slice to filter other panels to show only that fraud type.

---

## Panel 4: Top 10 Flagged Users (24h)
**Type:** Table  
**Time Range:** Last 24 hours

### What It Shows
- **Columns:**
  - `caller_number`: User's phone number
  - `alert_count`: How many times they triggered fraud alerts
  - `fraud_types`: Which fraud patterns they exhibited

### Key Insights
✅ **Repeat Offenders:** Who are the serial fraudsters?  
✅ **Investigation Priority:** Start with users at the top of this list  
✅ **Account Takeover Detection:** Legitimate users with sudden fraud = possible hack  
✅ **Fraud Rings:** Multiple users with same fraud types = organized group

### How to Use
**Example:**
```
User: +1234567890
Alert Count: 45
Fraud Types: ['SIM Box', 'IRSF']
```
**Analysis:** This user is very active in fraud and using multiple methods.

**Action:** 
1. Copy phone number
2. Investigate in your telecom system
3. Consider permanent block if confirmed fraud

---

## Panel 5: Recent Fraud Alerts (Live Feed)
**Type:** Table (scrolling)  
**Time Range:** Most recent 100 alerts

### What It Shows
- **Columns:**
  - `timestamp`: When the fraud was detected
  - `caller_number`: Who committed fraud
  - `alert_type`: What kind of fraud
  - `duration_min`: Call duration (if applicable)

### Key Insights
✅ **Real-time Stream:** See fraud as it happens  
✅ **Alert Context:** Understand each fraud incident in detail  
✅ **Investigation Starting Point:** Click to explore individual cases  
✅ **Pattern Spotting:** Notice repeated numbers or timing patterns

### How to Use
**Watch for:**
- Same phone number appearing multiple times = active fraudster
- Clusters of alerts at specific times = coordinated attack
- Unusual fraud types appearing = new attack method

**Action:**
1. Note suspicious patterns
2. Cross-reference with Panel 4 (Top Flagged Users)
3. Escalate to security team if systematic attack detected

---

# Dashboard 2: Business Metrics

**URL:** http://localhost:3000/d/fraud-business  
**Purpose:** Financial impact and ROI tracking for management  
**Refresh Rate:** 30 seconds  
**Best For:** Executive reports, business justification, budget planning

## Panel 1: Estimated $ Blocked (24h)
**Type:** Stat (large number)  
**Time Range:** Last 24 hours

### What It Shows
- Total dollar value of fraud prevented in the past day
- **Calculation:** `duration_min × $0.05 per minute`
- Color thresholds:
  - 🟢 Green: <$1,000 (low fraud day)
  - 🟡 Yellow: $1,000-$5,000 (moderate)
  - 🔴 Red: >$5,000 (high fraud day)

### Key Insights
✅ **ROI Proof:** Show management the financial value of fraud detection  
✅ **Cost Justification:** "We saved $10,000 yesterday"  
✅ **Trend Tracking:** Is fraud costing more or less over time?  
✅ **Budget Impact:** Estimate monthly/yearly savings

### How to Use
**For Executive Reports:**
- Daily: "Today we blocked $8,453 in fraud"
- Monthly: Sum up 30 days to show monthly impact
- Yearly: Multiply average by 365 for annual savings

**Example Presentation:**  
> "Our fraud detection system blocked **$8,453** in fraudulent activity yesterday.  
> At this rate, we'll save **$3.1 million annually**."

**Action:** Use this number in budget presentations to justify fraud prevention investment.

---

## Panel 2: Detection Success Rate
**Type:** Gauge (percentage)  
**Time Range:** Last 1 hour vs last 24 hours

### What It Shows
- Percentage of recent alerts compared to 24h average
- **Formula:** `(alerts_last_hour / total_alerts_24h) × 100`
- Thresholds:
  - 🔴 Red: <70% (detection degraded)
  - 🟡 Yellow: 70-90% (acceptable)
  - 🟢 Green: >90% (optimal)

### Key Insights
✅ **System Effectiveness:** How well is fraud detection performing?  
✅ **Coverage Rate:** Are we catching most fraud attempts?  
✅ **Performance Degradation:** Early warning if detection quality drops  
✅ **Benchmark Metric:** Compare against industry standards (aim for >95%)

### How to Use
**Scenarios:**
- **90%+:** Excellent - system is catching most fraud
- **70-90%:** Good but room for improvement
- **<70%:** Problem - investigate why detection rate dropped

**Possible Causes of Low Rate:**
- New fraud technique not in detection rules
- System overload/performance issue
- Fraudsters evolved their methods

**Action:** If <80%, review fraud detection rules and update thresholds.

---

## Panel 3: Hourly Fraud Value Trend
**Type:** Time Series Chart  
**Time Range:** Last 24 hours

### What It Shows
- Line graph of fraud value ($) by hour
- Shows financial impact timeline
- Peaks = expensive fraud hours
- Valleys = low fraud periods

### Key Insights
✅ **Cost Timeline:** When is fraud most expensive?  
✅ **Attack Patterns:** Do fraudsters target specific hours?  
✅ **Resource Planning:** Staff more analysts during high-value hours  
✅ **Anomaly Detection:** Unusual peaks = major fraud incident

### How to Use
**Pattern Recognition:**
```
High fraud value at 2 AM - 4 AM
→ Fraudsters operate during off-hours
→ Action: Increase automated monitoring overnight
```

**Business Impact:**
- Calculate peak hours and allocate SOC resources accordingly
- Identify cost spikes to investigate major fraud events

**Action:** Schedule fraud analysts to work during high-value fraud hours.

---

## Panel 4: Top Fraud Types by Value
**Type:** Horizontal Bar Chart  
**Time Range:** Last 24 hours

### What It Shows
- Fraud types ranked by total dollar value blocked (not count)
- Each bar = one fraud type
- Longer bars = more expensive fraud

### Key Insights
✅ **Most Costly Fraud:** Which fraud hurts your business most financially?  
✅ **Priority Ranking:** Focus on the most expensive fraud first  
✅ **Investment Decisions:** Build defenses for high-value fraud types  
✅ **Risk Assessment:** Understand financial exposure by fraud category

### How to Use
**Example:**
```
IRSF (International): $5,000
SIM Box: $2,000
Wangiri: $1,200
```
**Analysis:** IRSF is the most expensive → prioritize international call monitoring.

**Strategic Decisions:**
- Top 3 fraud types = 80% of financial damage → Focus here first
- Low-value but high-volume fraud → Automate blocking
- High-value but low-volume → Manual investigation

**Action:** Allocate fraud prevention budget proportional to the bars in this chart.

---

## Panel 5: Top Fraudsters by Value (24h)
**Type:** Table  
**Time Range:** Last 24 hours

### What It Shows
- **Columns:**
  - `caller_number`: User phone number
  - `fraud_count`: Number of fraud attempts
  - `total_value`: Total $ damage caused
  - `last_seen`: Most recent fraud timestamp

### Key Insights
✅ **High-Value Targets:** Who are the most expensive fraudsters?  
✅ **Investigation Priority:** Start with highest total_value  
✅ **Criminal Profiling:** Understand fraudster behavior patterns  
✅ **Legal Action:** Evidence for law enforcement escalation

### How to Use
**Example Row:**
```
+1234567890 | 28 frauds | $1,450 | 2 hours ago
```
**Analysis:**  
- High frequency (28 attempts)
- High value ($1,450 damage)
- Still active (2 hours ago)
- **Priority:** IMMEDIATE INVESTIGATION

**Action Steps:**
1. Permanently block this number
2. Investigate associated accounts
3. Report to fraud investigation team
4. Check for fraud ring (other numbers with similar patterns)

**Legal Use:** This table can be evidence for legal prosecution.

---

# Dashboard 3: Operational Health

**URL:** http://localhost:3000/d/fraud-ops-health  
**Purpose:** System health and SLA monitoring for DevOps  
**Refresh Rate:** 10 seconds  
**Best For:** System administrators, infrastructure monitoring

## Panel 1: ⏱ Data Freshness
**Type:** Stat (seconds)  
**Time Range:** Real-time

### What It Shows
- Seconds since the last fraud alert was received
- **Color Coding:**
  - 🟢 Green (<10s): "HEALTHY" - System is live
  - 🟡 Yellow (10-60s): "WARNING" - Possible delay
  - 🔴 Red (>60s): "CRITICAL" - Pipeline may be down

### Key Insights
✅ **Pipeline Health:** Is data flowing?  
✅ **Real-time Status:** Single glance to know if system is alive  
✅ **Outage Detection:** Immediate alert if data stops  
✅ **SLA Monitoring:** Data must be <10s fresh for real-time fraud blocking

### How to Use
**Scenarios:**
- **Green (5s):** Normal - data flowing perfectly
- **Yellow (25s):** Warning - check Kafka lag, Spark health
- **Red (120s):** CRITICAL - pipeline is broken!

**Troubleshooting Red Status:**
1. Check if Spark streaming job is running (`bash scripts/run_spark.sh`)
2. Verify Kafka brokers are up (`docker ps`)
3. Test ClickHouse connectivity
4. Check producer is sending data

**Action:** Set up automated alert if this goes red for >2 minutes.

---

## Panel 2: Alert Processing Rate
**Type:** Bar Chart  
**Time Range:** Last 1 hour (grouped by minute)

### What It Shows
- Number of alerts processed per minute
- Each bar = one minute
- Bar height = alert volume in that minute

### Key Insights
✅ **Throughput Monitoring:** How many alerts/minute can the system handle?  
✅ **Capacity Planning:** Is the system at maximum load?  
✅ **Bottleneck Detection:** Consistent low bars = processing slowdown  
✅ **Burst Handling:** Can the system handle fraud spikes?

### How to Use
**Patterns:**
- **Steady bars (5-10/min):** Normal processing
- **Increasing trend:** Fraud attack in progress OR system catching up
- **Flat zero bars:** **CRITICAL** - processing stopped!

**Capacity Analysis:**
```
Average: 8 alerts/min
Peak: 45 alerts/min
→ System can handle 5x bursts without issues
```

**Action:** If bars are consistently near zero but Panel 1 shows data flowing, investigate processing lag.

---

## Panel 3: Detection Latency (SLA)
**Type:** Table  
**Time Range:** Last 1 hour

### What It Shows
- **Metrics:**
  - **P50 (Median):** 50% of alerts processed within this time
  - **P95:** 95% of alerts processed within this time
  - **P99:** 99% of alerts processed within this time
- **All values in seconds**

### Key Insights
✅ **Performance Guarantee:** Meet SLA commitments  
✅ **User Experience:** Fast detection = happy customers  
✅ **Competitive Advantage:** Sub-second fraud blocking  
✅ **System Optimization:** Identify if performance degrades

### How to Use
**SLA Targets:**
```
IDEAL:
P50 < 2s  (median under 2 seconds)
P95 < 5s  (95% under 5 seconds)
P99 < 10s (99% under 10 seconds)

WARNING:
P50 > 5s  → System slowing down
P95 > 20s → Serious performance issue
P99 > 30s → CRITICAL - investigate immediately
```

**Performance Benchmarks:**
- **Excellent:** P50=1s, P95=3s, P99=5s
- **Good:** P50=3s, P95=8s, P99=15s
- **Poor:** P50>10s, P95>30s, P99>60s

**Action:** If P95 > 10s, optimize Spark batch sizes or add more Kafka partitions.

---

## Panel 4: System Activity Trend
**Type:** Time Series (2 lines)  
**Time Range:** Last 6 hours

### What It Shows
- **Line 1 (Blue):** Unique users triggering fraud
- **Line 2 (Green):** Total fraud alerts
- Shows activity patterns over 6 hours

### Key Insights
✅ **User vs Alert Ratio:** Are many users doing little fraud, or few users doing lots?  
✅ **Attack Campaign Detection:** Sudden increases in both = coordinated attack  
✅ **System Load:** High alerts = high system load  
✅ **Normal Baseline:** Learn what "normal" looks like

### How to Use
**Pattern Analysis:**
```
Scenario 1: High alerts, low unique users
→ Few fraudsters, very active → Focus on blocking top offenders

Scenario 2: Low alerts, high unique users
→ Many users, small fraud → Broad fraud campaign
```

**Divergence:** If lines split widely:
- Alerts spike but users stay flat → One user doing mass fraud
- Users spike but alerts stay flat → Many users doing small fraud

**Action:** Use this to understand if you have a "volume problem" or a "concentration problem."

---

## Panel 5: 24h System Summary
**Type:** Stat Panel (4 metrics)  
**Time Range:** Last 24 hours

### What It Shows
Four key metrics side-by-side:
1. **Total Alerts:** Overall fraud volume
2. **Unique Fraudsters:** How many different users
3. **Fraud Types:** Variety of attack methods
4. **Total Minutes:** Cumulative fraud call duration

### Key Insights
✅ **Daily Summary:** One-line system status  
✅ **Executive Dashboard:** Quick daily briefing numbers  
✅ **Trend Comparison:** Compare today vs yesterday  
✅ **Comprehensive Health:** All key metrics in one place

### How to Use
**Daily Report Template:**
> "Yesterday, we detected **1,247 fraud alerts** from **89 unique fraudsters** using **7 different fraud types**, totaling **3,456 minutes** of fraudulent activity."

**Week-over-Week Comparison:**
- Are total alerts increasing? → Fraud growing
- Are unique fraudsters decreasing? → Blocking is working
- Are fraud types increasing? → New attack methods emerging

**Action:** Screenshot this panel daily for weekly status reports to management.

---

# Dashboard 4: Analytics & Insights

**URL:** http://localhost:3000/d/fraud-analytics  
**Purpose:** Pattern discovery and fraud intelligence for analysts  
**Refresh Rate:** 30 seconds  
**Best For:** Fraud analysts, data scientists, strategic planning

## Panel 1: Fraud Heatmap: Hour × Day of Week (7d)
**Type:** Heatmap  
**Time Range:** Last 7 days

### What It Shows
- Grid showing fraud volume by:
  - **Rows:** Days of week (Monday-Sunday)
  - **Columns:** Hours of day (0-23)
- **Color intensity:** Darker = more fraud

### Key Insights
✅ **Temporal Patterns:** When do fraudsters operate?  
✅ **Resource Planning:** Staff analysts during peak fraud times  
✅ **Prevention Strategy:** Block aggressively during high-risk hours  
✅ **Anomaly Detection:** Unusual patterns = new fraud campaign

### How to Use
**Pattern Recognition:**
```
Dark cells at:
- Weekends, 2AM-4AM → Fraudsters work off-hours
- Fridays, 5PM-8PM → Target evening commuters

Light cells at:
- Weekdays, 9AM-5PM → Less fraud during business hours
```

**Strategic Actions:**
1. **Dark cells:** Increase automated blocking, reduce false positive tolerance
2. **Light cells:** Allow more flexible thresholds, focus on other tasks
3. **Anomaly:** New dark cell appearing → Investigate new fraud pattern

**Staffing Decisions:**
- Schedule fraud analysts to work when heatmap is darkest
- Automate responses during low-activity periods

**Action:** Update fraud detection thresholds hour-by-hour based on this heatmap.

---

## Panel 2: Top Fraud Destination Numbers
**Type:** Table  
**Time Range:** Last 24 hours

### What It Shows
- **Columns:**
  - `receiver_number`: Called number (destination)
  - `call_count`: How many fraud calls to this number
  - `  unique_callers`: Different fraudsters calling it
  - `fraud_types`: Types of fraud targeting this number

### Key Insights
✅ **Honeypot Detection:** Which numbers attract fraud?  
✅ **Fraud Destination Patterns:** Where is fraud money flowing?  
✅ **International Fraud:** Identify high-cost international destinations  
✅ **Blocking Opportunities:** Block calls to known fraud destinations

### How to Use
**Example:**
```
+12125551234 | 45 calls | 12 callers | ['IRSF', 'Wangiri']
```
**Analysis:**
- This number received 45 fraudulent calls
- From 12 different fraudsters
- Used for IRSF and Wangiri attacks
- **Likely:** Premium rate number or international fraud destination

**Types of Fraud Destinations:**
1. **High call count, low unique callers:** Single fraud ring
2. **High call count, high unique callers:** Well-known fraud number
3. **International numbers:** IRSF (expensive international calls)

**Action:**
1. Add these numbers to call blocking list
2. Alert international carrier about abuse
3. Monitor for similar number patterns

**Note:** "No data" is normal if most fraud doesn't involve receiver numbers (like SIM Box, credit fraud).

---

## Panel 3: Fraud Pattern Breakdown
**Type:** Horizontal Bar Chart  
**Time Range:** Last 24 hours

### What It Shows
- Fraud categories ranked by volume (not value)
- Each bar = broad fraud category (e.g., "IRSF", "Wangiri")
- Extracted from `alert_type` field by splitting on "/"

### Key Insights
✅ **Attack Method Distribution:** What fraud techniques are popular?  
✅ **Defense Prioritization:** Which fraud patterns need better detection?  
✅ **Fraud Evolution:** Track how attack methods shift over time  
✅ **Education:** Understand fraud landscape for training

### How to Use
**Example:**
```
IRSF: 80,000 cases
Wangiri: 20,000 cases
SIM Box: 15,000 cases
```

**Analysis:**
- **IRSF dominates:** Focus on international call detection
- **Wangiri growing:** One-ring scam becoming popular
- **SIM Box decreasing:** Previous blocking measures working

**Strategic Response:**
- **Top bar:** Invest in advanced detection for this fraud type
- **Growing bars:** New threat → update detection rules
- **Shrinking bars:** Current defenses working → maintain

**Action:** Use this to prioritize which fraud detection rules to enhance next.

---

## Panel 4: Fraud Type Trends (24h)
**Type:** Time Series (multi-line)  
**Time Range:** Last 24 hours

### What It Shows
- Multiple colored lines, each = one fraud type
- Shows how each fraud type evolves hour-by-hour
- Line crossings = fraud type dominance shifts

### Key Insights
✅ **Fraud Campaigns:** See when specific fraud types spike  
✅ **Attack Waves:** Identify coordinated fraud attempts  
✅ **Fraud Lifecycle:** How long do fraud campaigns last?  
✅ **Countermeasure Effectiveness:** Did blocking reduce a specific fraud?

### How to Use
**Pattern Scenarios:**
```
Scenario 1: One line spikes suddenly at 2 AM
→ Coordinated attack using that method
→ Action: Emergency blocking update

Scenario 2: All lines rise together at 8 PM
→ General fraud increase (maybe Friday evening)
→ Action: Increase monitoring capacity

Scenario 3: Line rises then drops sharply
→ Fraud detected and blocked successfully
→ Action: Analysis - what worked?
```

**Fraud Campaign Lifecycle:**
1. **Spike:** New fraud campaign starts
2. **Plateau:** Campaign continues
3. **Drop:** Detection improves or fraudsters give up
4. **Absence:** Fraud method abandoned

**Action:** When a line spikes, investigate within 1 hour to catch fraudsters before they stop.

---

## Panel 5: Fraud Pattern Analytics (7d)
**Type:** Table  
**Time Range:** Last 7 days

### What It Shows
- **Columns:**
  - `alert_type`: Specific fraud pattern
  - `total_cases`: Total occurrences in 7 days
  - `avg_duration`: Average call duration
  - `first_seen`: When this fraud first appeared
  - `last_seen`: Most recent occurrence

### Key Insights
✅ **Fraud Lifecycle:** How long has each fraud type been active?  
✅ **Persistence:** Which fraud keeps coming back?  
✅ **Effectiveness Metrics:** Does your blocking work?  
✅ **Threat Intelligence:** Comprehensive fraud catalog

### How to Use
**Example Row:**
```
IRSF (High Duration) | 45,000 cases | 12.5 min | 6 days ago | 5 min ago
```
**Analysis:**
- Very active fraud (45,000 cases)
- Long calls (12.5 min average) = expensive
- Been running for 6 days = persistent campaign
- Still active (5 min ago) = ongoing threat

**Decision Matrix:**
| Cases | Duration | Action |
|-------|----------|--------|
| High (>10k) | Long (>10 min) | **CRITICAL** - Immediate blocking |
| High | Short | Automate detection |
| Low (<1k) | Long | Manual investigation |
| Low | Short | Monitor only |

**Fraud Evolution:**
- **first_seen = today:** New fraud type emerging → Create detection rule
- **last_seen = 5 days ago:** Fraud stopped → Blocking working OR fraudsters moved on

**Action:** Use `first_seen` and `last_seen` to create fraud timeline reports for security briefings.

---

# Quick Reference Guide

## Which Dashboard Should I Use When?

### Scenario 1: System is Alerting - Possible Attack
**Start:** Dashboard 3 (Operational Health)  
**Check:** Data freshness (is system alive?)  
**Then:** Dashboard 1 (Real-time Operations) → See what's happening  
**Finally:** Dashboard 2 (Business Metrics) → Assess financial impact

### Scenario 2: Weekly Executive Report
**Use:** Dashboard 2 (Business Metrics)  
**Take Screenshots of:**
- $ Blocked (24h) - multiply by 7 for week
- Top Fraudsters table
- Fraud value trend

**Generate Report:**
> "This week we blocked **$52,000** in fraudulent activity across **1,247 incidents**."

### Scenario 3: Investigating Specific Fraud Pattern
**Start:** Dashboard 4 (Analytics) → Fraud Pattern Breakdown  
**Then:** Dashboard 1 → Filter by that fraud type  
**Check:** Dashboard 2 → See if it's financially significant  
**Plan:** Update detection rules based on findings

### Scenario 4: System Performance Review
**Use:** Dashboard 3 (Operational Health)  
**Monitor:**
- Detection latency (aim for P95 < 5s)
- Processing rate (should be steady)
- Data freshness (must be < 10s)

**Optimization:** If metrics degrade, scale up Spark or Kafka

### Scenario 5: Training New Fraud Analyst
**Tour Order:**
1. Dashboard 1 → Show live fraud in action
2. Dashboard 4 → Explain fraud patterns
3. Dashboard 2 → Discuss business impact
4. Dashboard 3 → Teach system health monitoring

---

# Best Practices

## Daily Routine
**8:00 AM** - Check Dashboard 3 (Operational Health)
- Is data freshness green?
- Any processing issues overnight?

**8:15 AM** - Review Dashboard 2 (Business Metrics)
- How much $ blocked yesterday?
- Any expensive new fraudsters?

**9:00 AM** - Monitor Dashboard 1 (Real-time Operations) throughout day
- Watch for spikes
- Investigate unusual patterns

**5:00 PM** - Analyze Dashboard 4 (Analytics)
- Review fraud heatmap for patterns
- Plan next day's monitoring focus

## Weekly Planning
1. Export fraud heatmap from Dashboard 4
2. Adjust analyst schedules based on peak fraud hours
3. Update fraud detection thresholds
4. Create executive summary from Dashboard 2

## Alerting Setup
**Critical Alerts** (Page Immediately):
- Data freshness > 60s (Dashboard 3)
- $ Blocked (24h) > $10,000 (Dashboard 2)
- Total alerts > 100/hour (Dashboard 1)

**Warning Alerts** (Slack/Email):
- Detection latency P95 > 10s (Dashboard 3)
- New fraud type appears (Dashboard 4)
- Processing rate drops below 5/min (Dashboard 3)

---

# Glossary

**P50/P95/P99:** Percentile metrics. P95 = 5s means 95% of alerts are processed within 5 seconds.

**Data Freshness:** Time since last data point received. <10s = real-time.

**Alert Processing Rate:** Number of fraud alerts the system can handle per minute.

**SLA (Service Level Agreement):** Performance guarantees, e.g., "detect fraud within 5 seconds."

**Fraud Value:** Estimated financial cost of fraud, calculated from call duration.

**Unique Fraudsters:** Count of distinct phone numbers committing fraud.

**Heatmap:** Visual representation where color intensity shows magnitude.

---

*Last Updated: For production deployment with 4 dashboards and 20 panels*
