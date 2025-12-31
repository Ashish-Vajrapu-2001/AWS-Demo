-- Extension to existing Control Database for Silver Layer

CREATE SCHEMA IF NOT EXISTS silver_control;

-- 1. Table Metadata Configuration
CREATE TABLE IF NOT EXISTS control.table_metadata_silver (
    table_id SERIAL PRIMARY KEY,
    bronze_table_name VARCHAR(255) NOT NULL,
    silver_table_name VARCHAR(255) NOT NULL,
    silver_schema VARCHAR(100) DEFAULT 'silver',
    silver_s3_path VARCHAR(500) NOT NULL,
    primary_key_columns VARCHAR(500) NOT NULL, -- Comma separated
    scd_type INT DEFAULT 1, -- 1 = Overwrite/Merge, 2 = History
    scd_tracked_columns VARCHAR(1000), -- For SCD2
    partition_columns VARCHAR(500),
    z_order_columns VARCHAR(500),
    silver_initial_load_completed BIT DEFAULT '0',
    last_sync_version BIGINT DEFAULT 0,
    last_load_timestamp TIMESTAMP,
    last_load_status VARCHAR(50),
    is_active BIT DEFAULT '1',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Data Quality Rules
-- Stores SQL-compatible expressions for validation
CREATE TABLE IF NOT EXISTS control.data_quality_rules (
    rule_id VARCHAR(50) PRIMARY KEY,
    table_id INT REFERENCES control.table_metadata_silver(table_id),
    rule_type VARCHAR(50) NOT NULL, -- NULL_CHECK, RANGE_CHECK, FORMAT_CHECK, REF_INTEGRITY
    column_name VARCHAR(255),
    rule_expression TEXT NOT NULL, -- Spark SQL expression returning BOOLEAN
    severity VARCHAR(20) DEFAULT 'SKIP_ROW', -- ERROR, SKIP_ROW, WARNING
    is_active BIT DEFAULT '1'
);

-- 3. DQ Exception Log (For reporting, actual data goes to DLQ S3)
CREATE TABLE IF NOT EXISTS control.dq_exception_summary (
    log_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100),
    table_id INT,
    rule_id VARCHAR(50),
    exception_count INT,
    s3_dlq_path VARCHAR(500),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Execution Log
CREATE TABLE IF NOT EXISTS control.silver_execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100),
    table_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50), -- RUNNING, SUCCESS, FAILED
    records_read BIGINT,
    records_written BIGINT,
    records_quarantined BIGINT,
    error_message TEXT
);

CREATE INDEX idx_silver_meta_active ON control.table_metadata_silver(is_active);