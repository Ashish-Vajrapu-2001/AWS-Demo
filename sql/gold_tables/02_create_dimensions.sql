-- Use the Gold Database
USE myntra_clv_gold;

-- 1. Dimension: Customer (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customer (
    dim_customer_key BIGINT,
    customer_id BIGINT,
    email STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    date_of_birth DATE,
    registration_date TIMESTAMP,
    customer_type STRING,
    status STRING,
    rfm_segment STRING,
    -- SCD Type 2 Metadata
    _valid_from TIMESTAMP,
    _valid_to TIMESTAMP,
    _is_current BOOLEAN,
    _version INT,
    -- Audit
    _created_date TIMESTAMP,
    _pipeline_run_id STRING
)
USING DELTA
LOCATION 's3://myntra-clv-gold/dimensions/dim_customer/';

-- 2. Dimension: Product (SCD Type 1)
CREATE TABLE IF NOT EXISTS dim_product (
    dim_product_key BIGINT,
    product_id BIGINT,
    sku STRING,
    product_name STRING,
    list_price DECIMAL(10, 2),
    brand_name STRING,
    brand_tier STRING,
    category_name STRING,
    category_level_1 STRING,
    category_level_2 STRING,
    _created_date TIMESTAMP,
    _pipeline_run_id STRING
)
USING DELTA
LOCATION 's3://myntra-clv-gold/dimensions/dim_product/';

-- 3. Dimension: Location (SCD Type 1)
CREATE TABLE IF NOT EXISTS dim_location (
    dim_location_key BIGINT,
    address_id BIGINT,
    city STRING,
    state STRING,
    pincode STRING,
    city_tier INT,
    is_metro BOOLEAN,
    region STRING,
    _created_date TIMESTAMP,
    _pipeline_run_id STRING
)
USING DELTA
LOCATION 's3://myntra-clv-gold/dimensions/dim_location/';

-- 4. Dimension: Date (Static)
CREATE TABLE IF NOT EXISTS dim_date (
    dim_date_key INT, -- YYYYMMDD
    full_date DATE,
    year INT,
    quarter INT,
    month_number INT,
    month_name STRING,
    week_of_year INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
)
USING DELTA
LOCATION 's3://myntra-clv-gold/dimensions/dim_date/';