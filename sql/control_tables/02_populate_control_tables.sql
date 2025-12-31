-- 1. Populate Source Systems
INSERT INTO control.source_systems (source_system_name, source_system_type, connection_name)
VALUES 
('SRC-001', 'MSSQL', 'clv/source/azure-sql-erp'),
('SRC-002', 'MSSQL', 'clv/source/azure-sql-crm'),
('SRC-003', 'MSSQL', 'clv/source/azure-sql-marketing');

-- 2. Populate Table Metadata
-- Note: initial_load_completed is explicitly set to FALSE to trigger first load

-- SRC-002 (CRM) Tables
INSERT INTO control.table_metadata 
(source_system_id, schema_name, table_name, primary_key_columns, load_type, initial_load_completed, bronze_s3_path, load_priority)
VALUES
(2, 'CRM', 'Customers', 'CUSTOMER_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-002/crm/customers/', 10),
(2, 'CRM', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-002/crm/customer_registration_source/', 20),
(2, 'CRM', 'INCIDENTS', 'INCIDENT_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-002/crm/incidents/', 20),
(2, 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-002/crm/interactions/', 30),
(2, 'CRM', 'SURVEYS', 'SURVEY_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-002/crm/surveys/', 30);

-- SRC-001 (ERP) Tables
INSERT INTO control.table_metadata 
(source_system_id, schema_name, table_name, primary_key_columns, load_type, initial_load_completed, bronze_s3_path, load_priority)
VALUES
(1, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-001/erp/oe_order_headers_all/', 20),
(1, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-001/erp/oe_order_lines_all/', 30),
(1, 'ERP', 'ADDRESSES', 'ADDRESS_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-001/erp/addresses/', 20),
(1, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 'CDC', FALSE, 's3://myntra-clv-bronze/src-001/erp/city_tier_master/', 10),
(1, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-001/erp/mtl_system_items_b/', 10),
(1, 'ERP', 'CATEGORIES', 'CATEGORY_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-001/erp/categories/', 5),
(1, 'ERP', 'BRANDS', 'BRAND_ID', 'CDC', FALSE, 's3://myntra-clv-bronze/src-001/erp/brands/', 5);

-- SRC-003 (Marketing) Tables
INSERT INTO control.table_metadata 
(source_system_id, schema_name, table_name, primary_key_columns, load_type, initial_load_completed, bronze_s3_path, load_priority)
VALUES
(3, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 'WATERMARK', FALSE, 's3://myntra-clv-bronze/src-003/marketing/marketing_campaigns/', 10);

-- 3. Populate Dependencies (Examples for topological sort)
-- Assuming table_ids are sequential based on insert order (1-13)

-- Customer Registration depends on Customers
INSERT INTO control.load_dependencies (table_id, depends_on_table_id)
SELECT child.table_id, parent.table_id
FROM control.table_metadata child, control.table_metadata parent
WHERE child.table_name = 'CustomerRegistrationSource' AND parent.table_name = 'Customers';

-- Orders depend on Customers
INSERT INTO control.load_dependencies (table_id, depends_on_table_id)
SELECT child.table_id, parent.table_id
FROM control.table_metadata child, control.table_metadata parent
WHERE child.table_name = 'OE_ORDER_HEADERS_ALL' AND parent.table_name = 'Customers';

-- Order Lines depend on Orders
INSERT INTO control.load_dependencies (table_id, depends_on_table_id)
SELECT child.table_id, parent.table_id
FROM control.table_metadata child, control.table_metadata parent
WHERE child.table_name = 'OE_ORDER_LINES_ALL' AND parent.table_name = 'OE_ORDER_HEADERS_ALL';

-- Addresses depend on Customers
INSERT INTO control.load_dependencies (table_id, depends_on_table_id)
SELECT child.table_id, parent.table_id
FROM control.table_metadata child, control.table_metadata parent
WHERE child.table_name = 'ADDRESSES' AND parent.table_name = 'Customers';

-- Items depend on Categories and Brands
INSERT INTO control.load_dependencies (table_id, depends_on_table_id)
SELECT child.table_id, parent.table_id
FROM control.table_metadata child, control.table_metadata parent
WHERE child.table_name = 'MTL_SYSTEM_ITEMS_B' AND parent.table_name = 'CATEGORIES';

INSERT INTO control.load_dependencies (table_id, depends_on_table_id)
SELECT child.table_id, parent.table_id
FROM control.table_metadata child, control.table_metadata parent
WHERE child.table_name = 'MTL_SYSTEM_ITEMS_B' AND parent.table_name = 'BRANDS';