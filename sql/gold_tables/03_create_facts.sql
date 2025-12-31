-- Use the Gold Database
USE myntra_clv_gold;

-- 1. Fact: Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    -- Foreign Keys
    dim_date_key INT,
    dim_customer_key BIGINT,
    dim_location_key BIGINT,
    dim_campaign_key BIGINT,
    -- Degenerate Dimensions
    order_number STRING,
    order_status STRING,
    payment_method STRING,
    -- Measures
    total_amount DECIMAL(18, 2),
    subtotal_amount DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    tax_amount DECIMAL(18, 2),
    shipping_amount DECIMAL(18, 2),
    is_delivered INT,
    -- Audit
    _created_date TIMESTAMP,
    _pipeline_run_id STRING
)
USING DELTA
PARTITIONED BY (dim_date_key)
LOCATION 's3://myntra-clv-gold/facts/fact_orders/';

-- 2. Fact: Order Lines
CREATE TABLE IF NOT EXISTS fact_order_lines (
    -- Foreign Keys
    dim_date_key INT,
    dim_product_key BIGINT,
    dim_customer_key BIGINT,
    dim_order_key BIGINT,
    -- Degenerate Dimensions
    order_number STRING,
    line_number INT,
    line_status STRING,
    -- Measures
    quantity INT,
    line_amount DECIMAL(18, 2),
    unit_price DECIMAL(18, 2),
    -- Audit
    _created_date TIMESTAMP,
    _pipeline_run_id STRING
)
USING DELTA
PARTITIONED BY (dim_date_key)
LOCATION 's3://myntra-clv-gold/facts/fact_order_lines/';

-- 3. Aggregate: Customer Monthly Snapshot
CREATE TABLE IF NOT EXISTS agg_customer_monthly_snapshot (
    dim_customer_key BIGINT,
    month_key INT, -- YYYYMM
    monthly_revenue DECIMAL(18, 2),
    monthly_orders INT,
    monthly_discount DECIMAL(18, 2),
    lifetime_revenue DECIMAL(18, 2),
    lifetime_orders INT,
    recency_days INT,
    aov_lifetime DECIMAL(18, 2),
    _created_date TIMESTAMP,
    _pipeline_run_id STRING
)
USING DELTA
PARTITIONED BY (month_key)
LOCATION 's3://myntra-clv-gold/aggregates/agg_customer_monthly_snapshot/';