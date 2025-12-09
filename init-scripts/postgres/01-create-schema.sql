-- ========================================
-- Telecom Fraud Detection - PostgreSQL Schema
-- ========================================
-- This script creates all CDC source tables
--
-- Schema Organization:
-- - crm_schema: Customer profiles and device information
-- - billing_schema: Payment transactions and customer behavior
-- - care_schema: Customer complaints and feedback
-- ========================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS crm_schema;
CREATE SCHEMA IF NOT EXISTS billing_schema;
CREATE SCHEMA IF NOT EXISTS care_schema;

-- ========================================
-- CRM SCHEMA (Customer Relationship Management)
-- ========================================

-- Customer Profiles (Master Customer Data)
CREATE TABLE crm_schema.customer_profiles (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(255),
    age INT,
    gender VARCHAR(10),
    location VARCHAR(255),
    account_creation_date DATE,
    plan_type VARCHAR(50),
    monthly_spending DECIMAL(10,2),
    credit_score INT,
    payment_method VARCHAR(50),
    avg_payment_delay INT,
    payment_behavior_index DECIMAL(5,2),
    credit_limit DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_profiles_customer_id ON crm_schema.customer_profiles(customer_id);

-- Device Information (IMEI Registry)
CREATE TABLE crm_schema.device_information (
    device_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    device_model VARCHAR(255),
    os_version VARCHAR(50),
    imei VARCHAR(20) UNIQUE,
    device_status VARCHAR(50) DEFAULT 'active',
    device_age_days INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_device_customer_id ON crm_schema.device_information(customer_id);

-- ========================================
-- BILLING SCHEMA (Financial Transactions)
-- ========================================

CREATE TABLE billing_schema.payment_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    wallet_balance DECIMAL(10,2),
    monthly_recharge_amount DECIMAL(10,2),
    failed_payment_attempts INT DEFAULT 0,
    vas_spending DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_payment_customer_id ON billing_schema.payment_transactions(customer_id);

CREATE TABLE billing_schema.customer_behavior (
    behavior_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    avg_session_duration INT,
    app_usage_hours DECIMAL(5,2),
    favorite_service VARCHAR(100),
    churn_risk_score DECIMAL(3,2),
    customer_segment VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_behavior_customer_id ON billing_schema.customer_behavior(customer_id);

-- ========================================
-- CARE SCHEMA (Customer Support)
-- ========================================

CREATE TABLE care_schema.complaints_feedback (
    complaint_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    complaint_type VARCHAR(100),
    severity VARCHAR(20),
    complaint_category VARCHAR(100),
    resolution_time_hours INT,
    complaint_status VARCHAR(50) DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_complaint_customer_id ON care_schema.complaints_feedback(customer_id);

-- ========================================
-- TRIGGERS FOR updated_at
-- ========================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_customer_profiles_updated_at BEFORE UPDATE ON crm_schema.customer_profiles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_device_information_updated_at BEFORE UPDATE ON crm_schema.device_information
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_payment_transactions_updated_at BEFORE UPDATE ON billing_schema.payment_transactions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_customer_behavior_updated_at BEFORE UPDATE ON billing_schema.customer_behavior
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_complaints_feedback_updated_at BEFORE UPDATE ON care_schema.complaints_feedback
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ========================================
-- GRANT PERMISSIONS FOR DEBEZIUM
-- ========================================

-- Create replication user
CREATE USER debezium WITH REPLICATION PASSWORD 'dbz123';
GRANT CONNECT ON DATABASE telecom TO debezium;
GRANT USAGE ON SCHEMA crm_schema, billing_schema, care_schema TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA crm_schema, billing_schema, care_schema TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm_schema, billing_schema, care_schema GRANT SELECT ON TABLES TO debezium;

-- Create publication for CDC
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

SELECT 'PostgreSQL schema initialized successfully!' AS status;
