-- ============================================================================
-- DEMO STEP 9: BI ANALYTICS QUERIES
-- ============================================================================
-- What this does:
--   10 ready-to-run BI queries demonstrating the Gold layer's analytical power.
--   Copy-paste into Snowflake worksheets or connect from Tableau/Power BI.
--
-- Talk track:
--   "These are production-ready queries analysts can run today. Monthly revenue
--    with MoM growth, top customers by LTV, sales rep scorecards, product
--    rankings, RFM customer segmentation, cohort analysis, and geographic
--    performance — all sub-second on our clustered star schema."
--
-- Runtime: Each query runs in ~1-3 seconds
-- Prerequisites: Steps 05-06 completed (Gold + Data Products)
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA GOLD;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- QUERY 1: Monthly Revenue by Territory + Month-over-Month Growth
-- ============================================================================

SELECT YEAR, MONTH, MONTH_NAME, TERRITORY_NAME, TOTAL_REVENUE, TOTAL_ORDERS,
    LAG(TOTAL_REVENUE) OVER (PARTITION BY TERRITORY_NAME ORDER BY YEAR, MONTH) AS PREV_MONTH,
    ROUND(
        (TOTAL_REVENUE - LAG(TOTAL_REVENUE) OVER (PARTITION BY TERRITORY_NAME ORDER BY YEAR, MONTH))
        / NULLIF(LAG(TOTAL_REVENUE) OVER (PARTITION BY TERRITORY_NAME ORDER BY YEAR, MONTH), 0) * 100, 2
    ) AS MOM_GROWTH_PCT
FROM RAW_SALES.GOLD.monthly_sales_summary
ORDER BY YEAR DESC, MONTH DESC, TERRITORY_NAME;


-- ============================================================================
-- QUERY 2: Top 20 Customers by Lifetime Value + Churn Risk Action
-- ============================================================================

SELECT CUSTOMER_ID, NAME, REGION, SEGMENT, LIFETIME_VALUE, ENGAGEMENT_STATUS, VALUE_SEGMENT,
    CASE
        WHEN VALUE_SEGMENT = 'High-Value' AND ENGAGEMENT_STATUS = 'Dormant' THEN 'URGENT: Re-engage VIP'
        WHEN VALUE_SEGMENT = 'High-Value' AND ENGAGEMENT_STATUS = 'At-Risk' THEN 'HIGH: Nurture VIP'
        WHEN VALUE_SEGMENT = 'Medium-Value' AND ENGAGEMENT_STATUS = 'Dormant' THEN 'MEDIUM: Check-in'
        ELSE 'Monitor'
    END AS ACTION
FROM RAW_SALES.GOLD.customer_lifetime_value
ORDER BY LIFETIME_VALUE DESC LIMIT 20;


-- ============================================================================
-- QUERY 3: Sales Rep Quota Attainment Scorecard
-- ============================================================================

SELECT REP_ID, NAME, TERRITORY_NAME, REGION, QUOTA, TOTAL_REVENUE,
    QUOTA_ATTAINMENT_PCT, UNIQUE_CUSTOMERS, TOTAL_ORDERS, PERFORMANCE_STATUS
FROM RAW_SALES.GOLD.sales_rep_scorecard
WHERE REP_ID IS NOT NULL
ORDER BY QUOTA_ATTAINMENT_PCT DESC;


-- ============================================================================
-- QUERY 4: Product Performance Ranking (Top 20 by Revenue)
-- ============================================================================

SELECT REVENUE_RANK, PRODUCT_NAME, CATEGORY, TOTAL_UNITS_SOLD, TOTAL_REVENUE,
    RETURN_RATE_PCT, PRODUCT_FLAG
FROM RAW_SALES.GOLD.product_performance
ORDER BY REVENUE_RANK LIMIT 20;


-- ============================================================================
-- QUERY 5: Customer Segment Mix (RFM x Value)
-- ============================================================================

SELECT CS.CUSTOMER_SEGMENT, CLV.VALUE_SEGMENT,
    COUNT(*) AS CUSTOMERS,
    ROUND(AVG(CS.MONETARY), 2) AS AVG_REVENUE,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS PCT_OF_TOTAL
FROM RAW_SALES.GOLD.customer_segmentation CS
JOIN RAW_SALES.GOLD.customer_lifetime_value CLV ON CS.CUSTOMER_ID = CLV.CUSTOMER_ID
GROUP BY CS.CUSTOMER_SEGMENT, CLV.VALUE_SEGMENT
ORDER BY CS.CUSTOMER_SEGMENT, CLV.VALUE_SEGMENT;


-- ============================================================================
-- QUERY 6: Quarterly Revenue YoY Growth
-- ============================================================================

WITH quarterly AS (
    SELECT YEAR, QUARTER, TERRITORY_NAME,
        SUM(TOTAL_REVENUE) AS REVENUE
    FROM RAW_SALES.GOLD.monthly_sales_summary
    GROUP BY YEAR, QUARTER, TERRITORY_NAME
)
SELECT YEAR, QUARTER, TERRITORY_NAME, REVENUE,
    LAG(REVENUE) OVER (PARTITION BY TERRITORY_NAME, QUARTER ORDER BY YEAR) AS PRIOR_YEAR,
    ROUND((REVENUE - LAG(REVENUE) OVER (PARTITION BY TERRITORY_NAME, QUARTER ORDER BY YEAR))
        / NULLIF(LAG(REVENUE) OVER (PARTITION BY TERRITORY_NAME, QUARTER ORDER BY YEAR), 0) * 100, 2
    ) AS YOY_GROWTH_PCT
FROM quarterly ORDER BY YEAR DESC, QUARTER DESC, TERRITORY_NAME;


-- ============================================================================
-- QUERY 7: Cohort Retention (Data Product 3)
-- ============================================================================

SELECT COHORT_MONTH, MONTHS_SINCE_ACQUISITION, COHORT_SIZE,
    ACTIVE_CUSTOMERS, RETENTION_PCT, LTV_PER_ACQUIREE
FROM RAW_SALES.GOLD.customer_acquisition_cohort
ORDER BY COHORT_MONTH DESC, MONTHS_SINCE_ACQUISITION;


-- ============================================================================
-- QUERY 8: Monthly Cash Flow with 3-Month Moving Average
-- ============================================================================

SELECT YEAR, MONTH, MONTH_NAME,
    SUM(TOTAL_REVENUE) AS MONTHLY_REVENUE,
    SUM(TOTAL_ORDERS) AS MONTHLY_ORDERS,
    ROUND(AVG(SUM(TOTAL_REVENUE)) OVER (ORDER BY YEAR, MONTH ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS REVENUE_3M_MA
FROM RAW_SALES.GOLD.monthly_sales_summary
GROUP BY YEAR, MONTH, MONTH_NAME
ORDER BY YEAR DESC, MONTH DESC;


-- ============================================================================
-- QUERY 9: Product Category Revenue Mix
-- ============================================================================

SELECT CATEGORY, COUNT(DISTINCT PRODUCT_ID) AS NUM_PRODUCTS,
    SUM(TOTAL_REVENUE) AS CATEGORY_REVENUE,
    ROUND(100.0 * SUM(TOTAL_REVENUE) / SUM(SUM(TOTAL_REVENUE)) OVER (), 1) AS REVENUE_PCT,
    RANK() OVER (ORDER BY SUM(TOTAL_REVENUE) DESC) AS RANK
FROM RAW_SALES.GOLD.product_performance
GROUP BY CATEGORY ORDER BY RANK;


-- ============================================================================
-- QUERY 10: Geographic Sales Performance + Rep Coverage
-- ============================================================================

WITH reps AS (
    SELECT TERRITORY_NAME, COUNT(DISTINCT REP_ID) AS NUM_REPS
    FROM RAW_SALES.GOLD.sales_rep_scorecard GROUP BY TERRITORY_NAME
)
SELECT M.TERRITORY_NAME, SUM(M.TOTAL_REVENUE) AS REVENUE, SUM(M.TOTAL_ORDERS) AS ORDERS,
    COALESCE(R.NUM_REPS, 0) AS REPS,
    ROUND(SUM(M.TOTAL_REVENUE) / SUM(SUM(M.TOTAL_REVENUE)) OVER () * 100, 1) AS REVENUE_SHARE_PCT,
    RANK() OVER (ORDER BY SUM(M.TOTAL_REVENUE) DESC) AS TERRITORY_RANK
FROM RAW_SALES.GOLD.monthly_sales_summary M
LEFT JOIN reps R ON M.TERRITORY_NAME = R.TERRITORY_NAME
GROUP BY M.TERRITORY_NAME, R.NUM_REPS
ORDER BY TERRITORY_RANK;


-- ============================================================================
-- CHECKPOINT: All 10 queries run sub-second on Gold star schema.
-- NEXT: Run 10_acceptance_gates.sql
-- ============================================================================
