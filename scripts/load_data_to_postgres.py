"""
Load CSV Data into PostgreSQL CDC Source Databases

This script reads CSV files and loads them into PostgreSQL tables
for CDC (Change Data Capture) processing via Debezium.

Data Flow:
CSV Files → PostgreSQL Tables → Debezium CDC → Kafka Topics → Spark

Usage:
    python load_data_to_postgres.py
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime

# PostgreSQL Connection
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'telecom',
    'user': 'postgres',
    'password': 'admin123'
}

# Data directory
if os.path.exists(r'D:\ITI-Data_Engineer\Projects\Final Project\Data\Source_System_Data'):
    DATA_DIR = r'D:\ITI-Data_Engineer\Projects\Final Project\Data\Source_System_Data'
else:
    DATA_DIR = '/mnt/d/ITI-Data_Engineer/Projects/Final Project/Data/Source_System_Data'

def get_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        print("✅ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def load_customer_profiles(conn):
    """Load customer_profiles.csv into crm_schema.customer_profiles"""
    print("\n" + "="*80)
    print("📊 Loading Customer Profiles")
    print("="*80)
    
    file_path = os.path.join(DATA_DIR, 'customer_profiles.csv')
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return
    
    df = pd.read_csv(file_path)
    print(f"  Loaded {len(df)} rows from CSV")
    
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute("TRUNCATE TABLE crm_schema.customer_profiles CASCADE")
    
    # Prepare data for insertion
    columns = ['customer_id', 'customer_name', 'age', 'gender', 'location', 
               'account_creation_date', 'plan_type', 'monthly_spending', 
               'credit_score', 'payment_method', 'avg_payment_delay', 
               'payment_behavior_index', 'credit_limit']
    
    # Handle missing columns
    for col in columns:
        if col not in df.columns:
            df[col] = None
    
    values = df[columns].values.tolist()
    
    # Insert data
    insert_query = f"""
        INSERT INTO crm_schema.customer_profiles 
        ({', '.join(columns)})
        VALUES %s
    """
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    
    print(f"✅ Inserted {len(df)} customer profiles")
    cursor.close()

def load_device_information(conn):
    """Load device_information.csv into crm_schema.device_information"""
    print("\n" + "="*80)
    print("📱 Loading Device Information")
    print("="*80)
    
    file_path = os.path.join(DATA_DIR, 'device_information.csv')
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return
    
    df = pd.read_csv(file_path)
    print(f"  Loaded {len(df)} rows from CSV")
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE crm_schema.device_information CASCADE")
    
    columns = ['customer_id', 'device_model', 'os_version', 'imei', 
               'device_status', 'device_age_days']
    
    for col in columns:
        if col not in df.columns:
            df[col] = None
    
    values = df[columns].values.tolist()
    
    insert_query = f"""
        INSERT INTO crm_schema.device_information 
        ({', '.join(columns)})
        VALUES %s
    """
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    
    print(f"✅ Inserted {len(df)} device records")
    cursor.close()

def load_payment_transactions(conn):
    """Load payment_transactions.csv into billing_schema.payment_transactions"""
    print("\n" + "="*80)
    print("💳 Loading Payment Transactions")
    print("="*80)
    
    file_path = os.path.join(DATA_DIR, 'payment_transactions.csv')
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return
    
    df = pd.read_csv(file_path)
    print(f"  Loaded {len(df)} rows from CSV")
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE billing_schema.payment_transactions CASCADE")
    
    columns = ['customer_id', 'wallet_balance', 'monthly_recharge_amount', 
               'failed_payment_attempts', 'vas_spending']
    
    for col in columns:
        if col not in df.columns:
            df[col] = None
    
    values = df[columns].values.tolist()
    
    insert_query = f"""
        INSERT INTO billing_schema.payment_transactions 
        ({', '.join(columns)})
        VALUES %s
    """
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    
    print(f"✅ Inserted {len(df)} payment transactions")
    cursor.close()

def load_customer_behavior(conn):
    """Load customer_behavior.csv into billing_schema.customer_behavior"""
    print("\n" + "="*80)
    print("📈 Loading Customer Behavior")
    print("="*80)
    
    file_path = os.path.join(DATA_DIR, 'customer_behavior.csv')
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return
    
    df = pd.read_csv(file_path)
    print(f"  Loaded {len(df)} rows from CSV")
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE billing_schema.customer_behavior CASCADE")
    
    columns = ['customer_id', 'avg_session_duration', 'app_usage_hours', 
               'favorite_service', 'churn_risk_score', 'customer_segment']
    
    for col in columns:
        if col not in df.columns:
            df[col] = None
    
    values = df[columns].values.tolist()
    
    insert_query = f"""
        INSERT INTO billing_schema.customer_behavior 
        ({', '.join(columns)})
        VALUES %s
    """
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    
    print(f"✅ Inserted {len(df)} behavior records")
    cursor.close()

def load_complaints_feedback(conn):
    """Load complaints_feedback.csv into care_schema.complaints_feedback"""
    print("\n" + "="*80)
    print("📞 Loading Complaints & Feedback")
    print("="*80)
    
    file_path = os.path.join(DATA_DIR, 'complaints_feedback.csv')
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return
    
    df = pd.read_csv(file_path)
    print(f"  Loaded {len(df)} rows from CSV")
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE care_schema.complaints_feedback CASCADE")
    
    columns = ['customer_id', 'complaint_type', 'severity', 
               'complaint_category', 'resolution_time_hours', 'complaint_status']
    
    for col in columns:
        if col not in df.columns:
            df[col] = None
    
    values = df[columns].values.tolist()
    
    insert_query = f"""
        INSERT INTO care_schema.complaints_feedback 
        ({', '.join(columns)})
        VALUES %s
    """
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    
    print(f"✅ Inserted {len(df)} complaint records")
    cursor.close()

def verify_data(conn):
    """Verify loaded data"""
    print("\n" + "="*80)
    print("🔍 Verifying Data")
    print("="*80)
    
    cursor = conn.cursor()
    
    tables = [
        ('crm_schema.customer_profiles', 'Customer Profiles'),
        ('crm_schema.device_information', 'Device Information'),
        ('billing_schema.payment_transactions', 'Payment Transactions'),
        ('billing_schema.customer_behavior', 'Customer Behavior'),
        ('care_schema.complaints_feedback', 'Complaints & Feedback')
    ]
    
    for table, name in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  ✓ {name}: {count} rows")
    
    cursor.close()

def main():
    """Main execution"""
    print("\n" + "="*80)
    print(" 🏢 PostgreSQL Data Loader for CDC")
    print("="*80)
    print(f"\n📂 Data Directory: {DATA_DIR}")
    print(f"🔌 PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}")
    print("="*80)
    
    conn = get_connection()
    if not conn:
        return
    
    try:
        load_customer_profiles(conn)
        load_device_information(conn)
        load_payment_transactions(conn)
        load_customer_behavior(conn)
        load_complaints_feedback(conn)
        
        verify_data(conn)
        
        print("\n" + "="*80)
        print("✅ All Data Loaded Successfully!")
        print("="*80)
        print("\n📊 Next Steps:")
        print("  1. Deploy Debezium connectors: bash scripts/deploy_debezium_connectors.sh")
        print("  2. Verify CDC topics in Kafka")
        print("  3. Start Spark processor to consume CDC data")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
