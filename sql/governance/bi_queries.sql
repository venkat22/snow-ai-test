-- Sample BI Queries for GOLD Layer
-- These queries are aligned with tables created in 04_phase2_gold.sql and 05_phase3_data_products.sql.

USE DATABASE RAW_SALES;
USE SCHEMA GOLD;
USE WAREHOUSE ANALYTICS_WH;

-- ============================================================================
-- Query 1: Monthly Revenue by Territory (with Month-over-Month Growth)
-- ============================================================================
SELECT
    YEAR,
    MONTH,
    MONTH_NAME,
    TERRITORY_NAME,
    TOTAL_REVENUE,
    TOTAL_ORDERS,
    UNIQUE_CUSTOMERS,
    AVG_ORDER_VALUE,
    LAG(TOTAL_REVENUE) OVER (
        PARTITION BY TERRITORY_NAME ORDER BY YEAR, MONTH
    ) AS PREV_MONTH_REVENUE,
    ROUND(
        (TOTAL_REVENUE - LAG(TOTAL_REVENUE) OVER (PARTITION BY TERRITORY_NAME ORDER BY YEAR, MONTH))
        / NULLIF(LAG(TOTAL_REVENUE) OVER (PARTITION BY TERRITORY_NAME ORDER BY YEAR, MONTH), 0) * 100,
        2
    ) AS MONTH_OVER_MONTH_GROWTH_PCT
FROM RAW_SALES.GOLD.monthly_sales_summary
ORDER BY YEAR DESC, MONTH DESC, TERRITORY_NAME;


-- ============================================================================
-- Query 2: Top 20 Customers by Lifetime Value + Churn Action
-- ============================================================================
SELECT
    CUSTOMER_ID,
    NAME,
    REGION,
    SEGMENT,
    LIFETIME_VALUE,
    LIFETIME_ORDER_COUNT,
    ENGAGEMENT_STATUS,
    VALUE_SEGMENT,
    RECENCY_DAYS,
    CASE
        WHEN VALUE_SEGMENT = 'High-Value' AND ENGAGEMENT_STATUS = 'Dormant' THEN 'URGENT: Re-engage VIP'
        WHEN VALUE_SEGMENT = 'High-Value' AND ENGAGEMENT_STATUS = 'At-Risk' THEN 'HIGH: Nurture VIP'
        WHEN VALUE_SEGMENT = 'Medium-Value' AND ENGAGEMENT_STATUS = 'Dormant' THEN 'MEDIUM: Check-in'
        ELSE 'Monitor'
    END AS ACTION_RECOMMENDATION
FROM RAW_SALES.GOLD.customer_lifetime_value
ORDER BY LIFETIME_VALUE DESC
LIMIT 20;


-- ============================================================================
-- Query 3: Sales Rep Quota Attainment Scorecard
-- ============================================================================
SELECT
    REP_ID,
    NAME,
    TERRITORY_NAME,
    REGION,
    QUOTA,
    TOTAL_REVENUE,
    QUOTA_ATTAINMENT_PCT,
    UNIQUE_CUSTOMERS,
    TOTAL_ORDERS,
    AVG_REVENUE_PER_LINE,
    LARGEST_ORDER,
    PERFORMANCE_STATUS
FROM RAW_SALES.GOLD.sales_rep_scorecard
WHERE REP_ID IS NOT NULL
ORDER BY QUOTA_ATTAINMENT_PCT DESC, TOTAL_REVENUE DESC;


-- ============================================================================
-- Query 4: Product Performance Ranking (Top 20)
-- ============================================================================
SELECT
    REVENUE_RANK,
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    TOTAL_UNITS_SOLD,
    TOTAL_REVENUE,
    TOTAL_ORDERS,
    AVG_REVENUE_PER_LINE,
    AVG_UNITS_PER_ORDER,
    PRODUCT_FLAG
FROM RAW_SALES.GOLD.product_performance
ORDER BY REVENUE_RANK ASC
LIMIT 20;


-- ============================================================================
-- Query 5: Customer Segment Mix + Value Segment
-- ============================================================================
SELECT
    CS.CUSTOMER_SEGMENT,
    CLV.VALUE_SEGMENT,
    COUNT(*) AS CUSTOMER_COUNT,
    ROUND(AVG(CS.MONETARY), 2) AS AVG_CUSTOMER_REVENUE,
    ROUND(AVG(CS.RECENCY_DAYS), 1) AS AVG_DAYS_SINCE_ORDER,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS PCT_OF_TOTAL
FROM RAW_SALES.GOLD.customer_segmentation CS
JOIN RAW_SALES.GOLD.customer_lifetime_value CLV
    ON CS.CUSTOMER_ID = CLV.CUSTOMER_ID
GROUP BY CS.CUSTOMER_SEGMENT, CLV.VALUE_SEGMENT
ORDER BY CS.CUSTOMER_SEGMENT, CLV.VALUE_SEGMENT;


-- ============================================================================
-- Query 6: Quarterly Revenue YoY by Territory
-- ============================================================================
WITH quarterly_data AS (
    SELECT
        YEAR,
        QUARTER,
        TERRITORY_NAME,
        SUM(TOTAL_REVENUE) AS QUARTERLY_REVENUE,
        SUM(TOTAL_ORDERS) AS QUARTERLY_ORDERS
    FROM RAW_SALES.GOLD.monthly_sales_summary
    GROUP BY YEAR, QUARTER, TERRITORY_NAME
)
SELECT
    Q.YEAR AS CURRENT_YEAR,
    Q.QUARTER,
    Q.TERRITORY_NAME,
    Q.QUARTERLY_REVENUE AS CURRENT_YEAR_REVENUE,
    LAG(Q.QUARTERLY_REVENUE) OVER (
        PARTITION BY Q.TERRITORY_NAME, Q.QUARTER ORDER BY Q.YEAR
    ) AS PRIOR_YEAR_REVENUE,
    ROUND(
        (Q.QUARTERLY_REVENUE - LAG(Q.QUARTERLY_REVENUE) OVER (PARTITION BY Q.TERRITORY_NAME, Q.QUARTER ORDER BY Q.YEAR))
        / NULLIF(LAG(Q.QUARTERLY_REVENUE) OVER (PARTITION BY Q.TERRITORY_NAME, Q.QUARTER ORDER BY Q.YEAR), 0) * 100,
        2
    ) AS YOY_GROWTH_PCT
FROM quarterly_data Q
ORDER BY Q.YEAR DESC, Q.QUARTER DESC, Q.TERRITORY_NAME;


-- ============================================================================
-- Query 7: Cohort Trend (Data Product)
-- ============================================================================
SELECT
    COHORT_MONTH,
    MONTHS_SINCE_ACQUISITION,
    COHORT_SIZE,
    ACTIVE_CUSTOMERS,
    RETENTION_PCT,
    COHORT_REVENUE,
    LTV_PER_ACQUIREE
FROM RAW_SALES.GOLD.customer_acquisition_cohort
ORDER BY COHORT_MONTH DESC, MONTHS_SINCE_ACQUISITION;


-- ============================================================================
-- Query 8: Monthly Cash Flow Trend + Moving Average
-- ============================================================================
SELECT
    YEAR,
    MONTH,
    MONTH_NAME,
    SUM(TOTAL_REVENUE) AS TOTAL_MONTHLY_REVENUE,
    SUM(TOTAL_ORDERS) AS TOTAL_MONTHLY_ORDERS,
    ROUND(AVG(AVG_ORDER_VALUE), 2) AS AVG_ORDER_VALUE,
    LAG(SUM(TOTAL_REVENUE)) OVER (ORDER BY YEAR, MONTH) AS PREV_MONTH_REVENUE,
    ROUND(
        (SUM(TOTAL_REVENUE) - LAG(SUM(TOTAL_REVENUE)) OVER (ORDER BY YEAR, MONTH))
        / NULLIF(LAG(SUM(TOTAL_REVENUE)) OVER (ORDER BY YEAR, MONTH), 0) * 100,
        2
    ) AS MOM_GROWTH_PCT,
    ROUND(
        AVG(SUM(TOTAL_REVENUE)) OVER (ORDER BY YEAR, MONTH ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),
        2
    ) AS REVENUE_3MONTH_MA
FROM RAW_SALES.GOLD.monthly_sales_summary
GROUP BY YEAR, MONTH, MONTH_NAME
ORDER BY YEAR DESC, MONTH DESC;


-- ============================================================================
-- Query 9: Product Category Revenue Mix
-- ============================================================================
SELECT
    CATEGORY,
    COUNT(DISTINCT PRODUCT_ID) AS NUM_PRODUCTS,
    SUM(TOTAL_REVENUE) AS CATEGORY_REVENUE,
    ROUND(100.0 * SUM(TOTAL_REVENUE) / SUM(SUM(TOTAL_REVENUE)) OVER (), 1) AS REVENUE_CONTRIBUTION_PCT,
    SUM(TOTAL_UNITS_SOLD) AS UNITS_SOLD,
    ROUND(AVG(AVG_REVENUE_PER_LINE), 2) AS AVG_REVENUE_PER_LINE,
    RANK() OVER (ORDER BY SUM(TOTAL_REVENUE) DESC) AS CATEGORY_RANK
FROM RAW_SALES.GOLD.product_performance
GROUP BY CATEGORY
ORDER BY CATEGORY_RANK;


-- ============================================================================
-- Query 10: Geographic Sales Performance + Rep Coverage
-- ============================================================================
WITH reps_per_territory AS (
    SELECT
        TERRITORY_NAME,
        COUNT(DISTINCT REP_ID) AS NUM_REPS
    FROM RAW_SALES.GOLD.sales_rep_scorecard
    GROUP BY TERRITORY_NAME
)
SELECT
    M.TERRITORY_NAME,
    SUM(M.TOTAL_REVENUE) AS TOTAL_REVENUE,
    SUM(M.TOTAL_ORDERS) AS TOTAL_ORDERS,
    COALESCE(R.NUM_REPS, 0) AS NUM_REPS,
    ROUND(AVG(M.AVG_ORDER_VALUE), 2) AS AVG_ORDER_VALUE,
    ROUND(SUM(M.TOTAL_REVENUE) / SUM(SUM(M.TOTAL_REVENUE)) OVER () * 100, 1) AS REVENUE_SHARE_PCT,
    RANK() OVER (ORDER BY SUM(M.TOTAL_REVENUE) DESC) AS TERRITORY_RANK
FROM RAW_SALES.GOLD.monthly_sales_summary M
LEFT JOIN reps_per_territory R
    ON M.TERRITORY_NAME = R.TERRITORY_NAME
GROUP BY M.TERRITORY_NAME, R.NUM_REPS
ORDER BY TERRITORY_RANK;


-- ============================================================================
-- Performance Test Query (expected to be fast with clustering)
-- ============================================================================
ALTER SESSION SET QUERY_TAG = 'performance_test';

SELECT
    DATE_TRUNC('MONTH', D.DATE_KEY) AS SALES_MONTH,
    T.TERRITORY_NAME,
    P.CATEGORY,
    COUNT(DISTINCT F.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    ROUND(SUM(F.REVENUE), 2) AS MONTHLY_REVENUE
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_dates D
    ON F.DATE_KEY = D.DATE_KEY
JOIN RAW_SALES.GOLD.dim_territories T
    ON F.TERRITORY_ID = T.TERRITORY_ID
JOIN RAW_SALES.GOLD.dim_products P
    ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;


-- ============================================================================
-- Query 11: Platform Health Snapshot (Streams + Dynamic Tables + Tasks)
-- ============================================================================

SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE;
SHOW DYNAMIC TABLES IN SCHEMA RAW_SALES.SILVER;
SHOW TASKS IN SCHEMA RAW_SALES.MONITORING;


-- ============================================================================
-- Query 12: Snowpark and Task Audit Operational Trend
-- ============================================================================

SELECT
    DATE_TRUNC('DAY', RUN_AT) AS RUN_DAY,
    JOB_NAME,
    STATUS,
    COUNT(*) AS RUNS
FROM RAW_SALES.MONITORING.snowpark_job_runs
GROUP BY 1, 2, 3
ORDER BY RUN_DAY DESC, JOB_NAME;

SELECT
    DATE_TRUNC('DAY', EXECUTED_AT) AS EXEC_DAY,
    PRODUCT_NAME,
    RESULT_STATUS,
    COUNT(*) AS RUNS
FROM RAW_SALES.MONITORING.task_run_audit
GROUP BY 1, 2, 3
ORDER BY EXEC_DAY DESC, PRODUCT_NAME;
