# إنشاء ملف ETL نظيف
cat > etl_batch_to_clickhouse.py << 'EOF'
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from clickhouse_driver import Client
import sys

print("🚀 Starting Batch ETL Pipeline...")

# ==========================================
# 1. Initialize Spark
# ==========================================
spark = SparkSession.builder \
    .appName("Telecom_Batch_ETL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark initialized")

# ==========================================
# 2. Load Data from HDFS
# ==========================================
def read_with_schema(layer, filename):
    path = f"hdfs://master:8020/telecom_project/{layer}/{filename}"
    return spark.read.csv(path, header=True, inferSchema=True)

print("📂 Loading data from HDFS...")

df_complaints = read_with_schema("batch_layer", "complaints_feedback.csv")
df_behavior   = read_with_schema("batch_layer", "customer_behavior.csv")
df_profiles   = read_with_schema("batch_layer", "customer_profiles.csv")
df_device     = read_with_schema("batch_layer", "device_information.csv")

df_cdr        = read_with_schema("stream_layer", "cdr_data.csv")
df_location   = read_with_schema("stream_layer", "location_updates.csv")
df_network    = read_with_schema("stream_layer", "network_metrics.csv")
df_payments   = read_with_schema("stream_layer", "payment_transactions.csv")
df_sdr        = read_with_schema("stream_layer", "sdr_data.csv")
df_security   = read_with_schema("stream_layer", "security_events.csv")
df_service    = read_with_schema("stream_layer", "service_usage.csv")

print("✅ Data loaded successfully")

# ==========================================
# 3. Create Dimensions
# ==========================================
print("🔨 Creating dimensions...")

# Dim_Customer
dim_customer = df_profiles.join(df_behavior, on="customer_id", how="left") \
    .select(
        col("customer_id"),
        col("age"),
        col("gender"),
        col("region"),
        col("marital_status"),
        col("education_level"),
        col("employment_status"),
        col("income"),
        col("preferred_language"),
        col("monthly_online_purchases"),
        col("avg_cart_value"),
        col("most_purchased_category"),
        col("preferred_contact_channel"),
        col("offer_response_rate")
    )

# Dim_Device
dim_device = df_device.select(
    col("customer_id"),
    col("device_type"),
    col("num_devices_registered"),
    col("device_age_months"),
    col("network_generation"),
    col("os_type"),
    col("uses_5g"),
    col("device_security_patch")
)

# Dim_Location
dim_location = df_location.select(
    col("tower_id"),
    col("city"),
    col("state"),
    col("zip_code"),
    col("latitude"),
    col("longitude"),
    col("urbanization_level")
).distinct()

# Dim_Time
spark.sql("""
    SELECT explode(sequence(to_date('2023-01-01'), to_date('2025-12-31'), interval 1 day)) as date_id
""").createOrReplaceTempView("dates_temp")

dim_time = spark.sql("""
    SELECT 
        date_id,
        year(date_id) as year,
        month(date_id) as month,
        day(date_id) as day,
        quarter(date_id) as quarter,
        date_format(date_id, 'EEEE') as day_name,
        CASE WHEN dayofweek(date_id) IN (1, 7) THEN true ELSE false END as is_weekend
    FROM dates_temp
""")

print("✅ Dimensions created")

# ==========================================
# 4. Create Facts
# ==========================================
print("🔨 Creating facts...")

fact_cdr = df_cdr.select(
    col("customer_id"),
    col("monthly_call_count"),
    col("monthly_call_duration"),
    col("international_call_duration"),
    col("call_drop_count"),
    col("sms_usage_per_month")
)

fact_payments = df_payments.select(
    col("customer_id"),
    col("monthly_spending"),
    col("credit_score"),
    col("payment_method"),
    col("avg_payment_delay"),
    col("payment_behavior_index"),
    col("credit_limit")
)

fact_complaints = df_complaints.select(
    col("customer_id"),
    col("customer_satisfaction_score"),
    col("customer_complaints"),
    col("technical_support_rating"),
    col("churn_risk_score"),
    col("has_contacted_support")
)

fact_internet = df_sdr.select(
    col("customer_id"),
    col("internet_usage_gb"),
    col("internet_speed_avg_mbps"),
    col("latency_ms"),
    col("peak_usage_hour"),
    col("weekend_data_ratio")
)

fact_network = df_network.select(
    col("tower_id"),
    col("signal_strength_avg"),
    col("network_downtime_minutes"),
    col("network_congestion_level"),
    col("avg_signal_strength")
)

fact_security = df_security.select(
    col("customer_id"),
    col("num_failed_logins"),
    col("num_fraud_attempts"),
    col("encryption_type"),
    col("biometric_auth"),
    col("two_factor_auth")
)

print("✅ Facts created")

# ==========================================
# 5. Connect to ClickHouse
# ==========================================
print("🔌 Connecting to ClickHouse...")

# Get ClickHouse IP
import subprocess
result = subprocess.run(['docker', 'inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', 'clickhouse'], 
                       capture_output=True, text=True)
CLICKHOUSE_IP = result.stdout.strip()

client = Client(host=CLICKHOUSE_IP, port=9000, user='admin', password='admin123', database='default')
print(f"✅ Connected to ClickHouse at {CLICKHOUSE_IP}")

# ==========================================
# 6. Create Tables in ClickHouse
# ==========================================
print("🔨 Creating ClickHouse tables...")

# Drop existing tables
tables = ['Dim_Customer', 'Dim_Device', 'Dim_Location', 'Dim_Time',
          'Fact_CDR', 'Fact_Payments', 'Fact_Complaints', 
          'Fact_Internet_Usage', 'Fact_Network_Metrics', 'Fact_Security']

for table in tables:
    client.execute(f'DROP TABLE IF EXISTS {table}')

# Create Dimensions
client.execute('''
    CREATE TABLE Dim_Customer (
        customer_id Int32,
        age Int32,
        gender String,
        region String,
        marital_status String,
        education_level String,
        employment_status String,
        income Float64,
        preferred_language String,
        monthly_online_purchases Int32,
        avg_cart_value Float64,
        most_purchased_category String,
        preferred_contact_channel String,
        offer_response_rate Float64
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

client.execute('''
    CREATE TABLE Dim_Device (
        customer_id Int32,
        device_type String,
        num_devices_registered Int32,
        device_age_months Int32,
        network_generation String,
        os_type String,
        uses_5g UInt8,
        device_security_patch String
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

client.execute('''
    CREATE TABLE Dim_Location (
        tower_id Int32,
        city String,
        state String,
        zip_code Int32,
        latitude Float64,
        longitude Float64,
        urbanization_level String
    ) ENGINE = MergeTree() ORDER BY tower_id
''')

client.execute('''
    CREATE TABLE Dim_Time (
        date_id Date,
        year Int32,
        month Int32,
        day Int32,
        quarter Int32,
        day_name String,
        is_weekend UInt8
    ) ENGINE = MergeTree() ORDER BY date_id
''')

# Create Facts
client.execute('''
    CREATE TABLE Fact_CDR (
        customer_id Int32,
        monthly_call_count Int32,
        monthly_call_duration Int32,
        international_call_duration Int32,
        call_drop_count Int32,
        sms_usage_per_month Int32
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

client.execute('''
    CREATE TABLE Fact_Payments (
        customer_id Int32,
        monthly_spending Float64,
        credit_score Int32,
        payment_method String,
        avg_payment_delay Float64,
        payment_behavior_index Float64,
        credit_limit Float64
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

client.execute('''
    CREATE TABLE Fact_Complaints (
        customer_id Int32,
        customer_satisfaction_score Float64,
        customer_complaints Int32,
        technical_support_rating Int32,
        churn_risk_score Float64,
        has_contacted_support UInt8
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

client.execute('''
    CREATE TABLE Fact_Internet_Usage (
        customer_id Int32,
        internet_usage_gb Float64,
        internet_speed_avg_mbps Float64,
        latency_ms Float64,
        peak_usage_hour Int32,
        weekend_data_ratio Float64
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

client.execute('''
    CREATE TABLE Fact_Network_Metrics (
        tower_id Int32,
        signal_strength_avg Float64,
        network_downtime_minutes Int32,
        network_congestion_level String,
        avg_signal_strength Float64
    ) ENGINE = MergeTree() ORDER BY tower_id
''')

client.execute('''
    CREATE TABLE Fact_Security (
        customer_id Int32,
        num_failed_logins Int32,
        num_fraud_attempts Int32,
        encryption_type String,
        biometric_auth String,
        two_factor_auth UInt8
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

print("✅ Tables created")

# ==========================================
# 7. Insert Data into ClickHouse
# ==========================================
def insert_batch(spark_df, table_name, batch_size=10000):
    print(f"📥 Inserting into {table_name}...")
    batch = []
    total = 0
    
    for row in spark_df.toLocalIterator():
        batch.append(row)
        if len(batch) >= batch_size:
            client.execute(f'INSERT INTO {table_name} VALUES', batch)
            total += len(batch)
            print(f"   → {total} rows inserted...")
            batch = []
    
    if batch:
        client.execute(f'INSERT INTO {table_name} VALUES', batch)
        total += len(batch)
    
    print(f"✅ {table_name}: {total} rows total\n")

# Insert Dimensions
insert_batch(dim_customer, 'Dim_Customer')
insert_batch(dim_device, 'Dim_Device')
insert_batch(dim_location, 'Dim_Location')
insert_batch(dim_time, 'Dim_Time')

# Insert Facts
insert_batch(fact_cdr, 'Fact_CDR')
insert_batch(fact_payments, 'Fact_Payments')
insert_batch(fact_complaints, 'Fact_Complaints')
insert_batch(fact_internet, 'Fact_Internet_Usage')
insert_batch(fact_network, 'Fact_Network_Metrics')
insert_batch(fact_security, 'Fact_Security')

print("🎉 Batch ETL completed successfully!")
spark.stop()
EOF
