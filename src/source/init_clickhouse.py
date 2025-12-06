import requests
import time

CLICKHOUSE_URL = "http://localhost:8123/"
AUTH = ('admin', 'admin123')

def execute_query(query, description):
    print(f"Executing: {description}...")
    try:
        response = requests.post(CLICKHOUSE_URL, auth=AUTH, data=query)
        if response.status_code == 200:
            print(f"   ✅ Success")
        else:
            print(f"   ❌ Failed: {response.text}")
            # Don't exit, might be "already exists" error which is fine
    except Exception as e:
        print(f"   ❌ Connection Error: {e}")
        return False
    return True

def init_db():
    print("Initializing ClickHouse Database...")
    
    # Wait for ClickHouse to be ready
    max_retries = 10
    for i in range(max_retries):
        try:
            requests.get(CLICKHOUSE_URL)
            break
        except:
            print(f"   Waiting for ClickHouse... ({i+1}/{max_retries})")
            time.sleep(2)
            
    # 1. Create Database
    create_db_query = "CREATE DATABASE IF NOT EXISTS telecom_fraud"
    execute_query(create_db_query, "Create Database 'telecom_fraud'")
    
    # 2. Create Table
    # Using MergeTree engine for performance
    create_table_query = """
    CREATE TABLE IF NOT EXISTS telecom_fraud.fraud_alerts (
        call_id String,
        caller_number String,
        receiver_number String,
        duration_min Float32,
        timestamp DateTime,
        alert_type String
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    """
    execute_query(create_table_query, "Create Table 'fraud_alerts'")
    
    print("\nClickHouse Initialization Complete.")

if __name__ == "__main__":
    init_db()
