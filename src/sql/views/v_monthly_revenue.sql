-- src/sql/views/v_monthly_revenue.sql

CREATE OR REPLACE VIEW nff_catalog.gold.v_monthly_revenue
COMMENT 'Monthly revenue aggregates with MoM growth for NZ regions'
AS
WITH monthly_stats AS (
    SELECT 
        region,
        date_trunc('month', tx_time) AS report_month,
        sum(amount) AS total_revenue,
        count(tx_id) AS transaction_count
    FROM nff_catalog.silver.transactions
    GROUP BY 1, 2
)
SELECT 
    region,
    report_month,
    total_revenue,
    transaction_count,
    -- Lead Architect Tip: Calculate MoM Growth using Window Functions
    LAG(total_revenue) OVER (PARTITION BY region ORDER BY report_month) AS prev_month_revenue,
    (total_revenue - LAG(total_revenue) OVER (PARTITION BY region ORDER BY report_month)) 
        / LAG(total_revenue) OVER (PARTITION BY region ORDER BY report_month) * 100 AS mom_growth_pct
FROM monthly_stats;