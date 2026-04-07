-- sql/views/v_monthly_revenue.sql

/* NEXUSFLOW EXECUTIVE VIEW: MONTHLY REVENUE
  Description: Aggregates total revenue and transaction volume by month and region.
  Note: Inherits Row-Level Security from nff_catalog.gold.fct_transactions.
*/

CREATE OR REPLACE VIEW nff_catalog.gold.v_monthly_revenue AS
SELECT
    -- 1. Time Dimensions
    date_format(tx_timestamp, 'yyyy-MM') AS report_month,
    
    -- 2. Categorical Dimensions
    region,
    customer_tier,
    
    -- 3. Key Performance Indicators (KPIs)
    round(sum(tx_amount), 2) AS total_revenue,
    count(tx_id) AS total_transactions,
    round(avg(tx_amount), 2) AS average_ticket_size,
    
    -- 4. Audit Metadata
    current_timestamp() AS view_refreshed_at
FROM 
    nff_catalog.gold.fct_transactions
GROUP BY 
    1, 2, 3
ORDER BY 
    report_month DESC, 
    total_revenue DESC;