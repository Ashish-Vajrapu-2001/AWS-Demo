USE myntra_clv_gold;

-- 1. View: Customer 360 (Current Snapshot)
CREATE OR REPLACE VIEW v_customer_360 AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.customer_type,
    c.rfm_segment,
    agg.month_key AS latest_activity_month,
    agg.lifetime_revenue,
    agg.lifetime_orders,
    agg.aov_lifetime,
    agg.recency_days
FROM dim_customer c
JOIN agg_customer_monthly_snapshot agg 
    ON c.dim_customer_key = agg.dim_customer_key
WHERE c._is_current = true
  AND agg.month_key = (SELECT MAX(month_key) FROM agg_customer_monthly_snapshot);

-- 2. View: Monthly Sales Trend
CREATE OR REPLACE VIEW v_monthly_sales_trend AS
SELECT 
    d.year,
    d.month_name,
    d.month_number,
    SUM(f.total_amount) AS total_revenue,
    COUNT(DISTINCT f.order_number) AS total_orders,
    AVG(f.total_amount) AS aov
FROM fact_orders f
JOIN dim_date d ON f.dim_date_key = d.dim_date_key
GROUP BY d.year, d.month_name, d.month_number
ORDER BY d.year DESC, d.month_number DESC;

-- 3. View: Product Performance
CREATE OR REPLACE VIEW v_product_performance AS
SELECT 
    p.category_name,
    p.brand_name,
    p.product_name,
    SUM(l.quantity) AS units_sold,
    SUM(l.line_amount) AS revenue_generated
FROM fact_order_lines l
JOIN dim_product p ON l.dim_product_key = p.dim_product_key
JOIN dim_date d ON l.dim_date_key = d.dim_date_key
WHERE d.full_date >= date_add('day', -30, current_date) -- Last 30 Days
GROUP BY p.category_name, p.brand_name, p.product_name
ORDER BY revenue_generated DESC;