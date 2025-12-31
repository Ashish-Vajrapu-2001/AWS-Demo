-- Populate Metadata for In-Scope Entities

-- 1. Customers (SCD Type 2)
INSERT INTO control.table_metadata_silver 
(bronze_table_name, silver_table_name, silver_schema, silver_s3_path, primary_key_columns, scd_type, scd_tracked_columns, z_order_columns)
VALUES 
('customers', 'customers', 'silver_crm', 's3://myntra-clv-silver/crm/customers/', 'customer_id', 2, 'status,customer_type,email,phone', 'customer_id,email');

-- 2. Orders (SCD Type 1)
INSERT INTO control.table_metadata_silver 
(bronze_table_name, silver_table_name, silver_schema, silver_s3_path, primary_key_columns, scd_type, partition_columns, z_order_columns)
VALUES 
('oe_order_headers_all', 'orders', 'silver_erp', 's3://myntra-clv-silver/erp/orders/', 'order_id', 1, 'order_date', 'customer_id');

-- 3. Order Lines (SCD Type 1)
INSERT INTO control.table_metadata_silver 
(bronze_table_name, silver_table_name, silver_schema, silver_s3_path, primary_key_columns, scd_type, partition_columns)
VALUES 
('oe_order_lines_all', 'order_lines', 'silver_erp', 's3://myntra-clv-silver/erp/order_lines/', 'line_id', 1, 'order_date');

-- 4. Addresses (SCD Type 2)
INSERT INTO control.table_metadata_silver 
(bronze_table_name, silver_table_name, silver_schema, silver_s3_path, primary_key_columns, scd_type, scd_tracked_columns)
VALUES 
('addresses', 'addresses', 'silver_erp', 's3://myntra-clv-silver/erp/addresses/', 'address_id', 2, 'city,state,pincode');

-- 5. Products (SCD Type 1)
INSERT INTO control.table_metadata_silver 
(bronze_table_name, silver_table_name, silver_schema, silver_s3_path, primary_key_columns, scd_type)
VALUES 
('mtl_system_items_b', 'products', 'silver_erp', 's3://myntra-clv-silver/erp/products/', 'inventory_item_id', 1);

-- Populate Data Quality Rules (Sample based on LLD)

-- Customers Rules
INSERT INTO control.data_quality_rules (rule_id, table_id, rule_type, column_name, rule_expression, severity)
SELECT 'DQ-N-001', table_id, 'NULL_CHECK', 'email', 'email IS NOT NULL', 'ERROR'
FROM control.table_metadata_silver WHERE silver_table_name = 'customers';

INSERT INTO control.data_quality_rules (rule_id, table_id, rule_type, column_name, rule_expression, severity)
SELECT 'DQ-F-001', table_id, 'FORMAT_CHECK', 'email', "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'", 'SKIP_ROW'
FROM control.table_metadata_silver WHERE silver_table_name = 'customers';

-- Orders Rules
INSERT INTO control.data_quality_rules (rule_id, table_id, rule_type, column_name, rule_expression, severity)
SELECT 'DQ-V-001', table_id, 'RANGE_CHECK', 'total_amount', 'total_amount > 0', 'SKIP_ROW'
FROM control.table_metadata_silver WHERE silver_table_name = 'orders';

INSERT INTO control.data_quality_rules (rule_id, table_id, rule_type, column_name, rule_expression, severity)
SELECT 'DQ-R-001', table_id, 'REF_INTEGRITY', 'customer_id', 'EXISTS (SELECT 1 FROM silver_crm.customers c WHERE c.customer_id = customer_id)', 'SKIP_ROW'
FROM control.table_metadata_silver WHERE silver_table_name = 'orders';