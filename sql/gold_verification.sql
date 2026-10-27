-- NorthWind Retail — Gold Layer Verification Queries
-- Replace `nwretail_databricks_dev` if your Unity Catalog name differs.
-- Run these in Databricks SQL Editor or Synapse Serverless after the Gold pipeline completes.

-- ═══════════════════════════════════════════════════════
-- 1. ROW COUNTS — confirm all 5 Gold tables are populated
-- ═══════════════════════════════════════════════════════
SELECT 'daily_sales_summary'      AS gold_table, COUNT(*) AS row_count FROM nwretail_databricks_dev.gold.daily_sales_summary
UNION ALL
SELECT 'customer_lifetime_value'  AS gold_table, COUNT(*) AS row_count FROM nwretail_databricks_dev.gold.customer_lifetime_value
UNION ALL
SELECT 'product_performance'      AS gold_table, COUNT(*) AS row_count FROM nwretail_databricks_dev.gold.product_performance
UNION ALL
SELECT 'store_performance'        AS gold_table, COUNT(*) AS row_count FROM nwretail_databricks_dev.gold.store_performance
UNION ALL
SELECT 'returns_analysis'         AS gold_table, COUNT(*) AS row_count FROM nwretail_databricks_dev.gold.returns_analysis
UNION ALL
SELECT 'inventory_health'         AS gold_table, COUNT(*) AS row_count FROM nwretail_databricks_dev.gold.inventory_health
ORDER BY gold_table;

-- ═══════════════════════════════════════════
-- 2. DAILY SALES — top 10 revenue days
-- ═══════════════════════════════════════════
SELECT transaction_date, store_region,
       SUM(total_revenue) AS revenue,
       SUM(gross_profit)  AS profit,
       AVG(profit_margin_pct) AS avg_margin_pct
FROM nwretail_databricks_dev.gold.daily_sales_summary
GROUP BY transaction_date, store_region
ORDER BY revenue DESC
LIMIT 10;

-- ═══════════════════════════════════════════
-- 3. CUSTOMER CLV — tier distribution
-- ═══════════════════════════════════════════
SELECT clv_tier,
       COUNT(*)                        AS customer_count,
       ROUND(SUM(net_revenue), 2)      AS total_net_revenue,
       ROUND(AVG(avg_order_value), 2)  AS avg_order_value,
       ROUND(AVG(return_rate_pct), 2)  AS avg_return_rate_pct
FROM nwretail_databricks_dev.gold.customer_lifetime_value
GROUP BY clv_tier
ORDER BY total_net_revenue DESC;

-- ═══════════════════════════════════════════
-- 4. PRODUCT — top 10 by gross profit
-- ═══════════════════════════════════════════
SELECT product_name, category, brand,
       total_units_sold,
       ROUND(gross_revenue, 2) AS gross_revenue,
       ROUND(gross_profit, 2)  AS gross_profit,
       profit_margin_pct,
       performance_tier
FROM nwretail_databricks_dev.gold.product_performance
ORDER BY gross_profit DESC
LIMIT 10;

-- ═══════════════════════════════════════════
-- 5. STORE — performance tier distribution
-- ═══════════════════════════════════════════
SELECT performance_tier,
       COUNT(*)                           AS store_count,
       ROUND(SUM(net_revenue), 2)         AS total_net_revenue,
       ROUND(AVG(avg_transaction_value), 2) AS avg_txn_value
FROM nwretail_databricks_dev.gold.store_performance
GROUP BY performance_tier
ORDER BY total_net_revenue DESC;

-- ═══════════════════════════════════════════
-- 6. RETURNS — top reasons by refund amount
-- ═══════════════════════════════════════════
SELECT reason_category, reason,
       SUM(return_count)           AS total_returns,
       SUM(total_units_returned)   AS units_returned,
       ROUND(SUM(total_refund_amount), 2) AS total_refunded
FROM nwretail_databricks_dev.gold.returns_analysis
GROUP BY reason_category, reason
ORDER BY total_refunded DESC
LIMIT 15;

-- ═══════════════════════════════════════════
-- 7. INVENTORY — out of stock / reorder now
-- ═══════════════════════════════════════════
SELECT stock_status,
       COUNT(*)                          AS records,
       SUM(stock_on_hand)                AS total_units,
       ROUND(SUM(inventory_value), 2)    AS total_inventory_value
FROM nwretail_databricks_dev.gold.inventory_health
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM nwretail_databricks_dev.gold.inventory_health)
GROUP BY stock_status
ORDER BY stock_status;

-- ═══════════════════════════════════════════════════════
-- 8. GOLD AUDIT LOG — last run status per table
-- ═══════════════════════════════════════════════════════
SELECT table_name, status, rows_written,
       run_ts, error_message
FROM nwretail_databricks_dev.ops.gold_quality_summary
WHERE run_ts = (
    SELECT MAX(run_ts) FROM nwretail_databricks_dev.ops.gold_quality_summary g2
    WHERE g2.table_name = gold_quality_summary.table_name
)
ORDER BY table_name;
