-- Schema Creation
CREATE SCHEMA IF NOT EXISTS control;

-- 1. Source Systems Registry
CREATE TABLE control.source_systems (
    source_system_id SERIAL PRIMARY KEY,
    source_system_name VARCHAR(50) NOT NULL UNIQUE,
    source_system_type VARCHAR(20) NOT NULL,
    connection_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Table Metadata Configuration (CRITICAL TABLE)
CREATE TABLE control.table_metadata (
    table_id SERIAL PRIMARY KEY,
    source_system_id INTEGER REFERENCES control.source_systems(source_system_id),
    schema_name VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    primary_key_columns TEXT NOT NULL, -- Comma separated for composite keys
    load_type VARCHAR(20) CHECK (load_type IN ('FULL', 'CDC', 'WATERMARK')),
    is_active BOOLEAN DEFAULT TRUE,
    initial_load_completed BOOLEAN DEFAULT FALSE, -- CRITICAL FLAG
    last_sync_version BIGINT DEFAULT 0,
    last_load_status VARCHAR(20),
    last_load_timestamp TIMESTAMP,
    last_execution_id VARCHAR(255),
    records_loaded BIGINT DEFAULT 0,
    bronze_s3_path VARCHAR(255),
    load_priority INTEGER DEFAULT 100,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_schema_table UNIQUE (source_system_id, schema_name, table_name)
);

-- 3. Load Dependencies
CREATE TABLE control.load_dependencies (
    dependency_id SERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES control.table_metadata(table_id),
    depends_on_table_id INTEGER REFERENCES control.table_metadata(table_id),
    dependency_type VARCHAR(20) DEFAULT 'FOREIGN_KEY',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Pipeline Execution Log
CREATE TABLE control.pipeline_execution_log (
    log_id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    table_id INTEGER REFERENCES control.table_metadata(table_id),
    execution_status VARCHAR(20),
    records_processed BIGINT,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    error_message TEXT
);

-- 5. Data Quality Rules (Optional but recommended)
CREATE TABLE control.data_quality_rules (
    rule_id SERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES control.table_metadata(table_id),
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    rule_expression TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_metadata_active ON control.table_metadata(is_active);
CREATE INDEX idx_execution_log_id ON control.pipeline_execution_log(execution_id);
CREATE INDEX idx_execution_log_status ON control.pipeline_execution_log(execution_status);