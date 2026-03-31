-- ============================================================================
-- DATA PRODUCT 4: Product Performance Analytics
-- ============================================================================
-- What this does:
--   Creates a monthly product performance data product with revenue metrics,
--   return rates, growth trends, and performance tiers.
--
-- Owner:            Product Team
-- Refresh Cadence:  Weekly (Tuesdays 7 AM ET)
-- SLA:              <2s query latency, 99% completeness, weekly refresh
-- Source Tables:    fact_orders, dim_products, dim_dates
--
-- Talk track:
--   "This data product gives the Product team a single pane of glass for
--    product performance — monthly revenue, return rates, month-over-month
--    growth, and an automated performance tier. It refreshes weekly and
--    feeds the marketplace alongside our other three data products."
--
-- Prerequisites: Steps 05 (Gold star schema) completed
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA GOLD;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- PART A: CREATE THE DATA PRODUCT TABLE
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.product_performance_analytics AS
WITH monthly_product AS (
    SELECT
        D.YEAR,
        D.MONTH,
        D.MONTH_NAME,
        D.YEAR_MONTH,
        D.QUARTER,
        P.PRODUCT_ID,
        P.PRODUCT_NAME,
        P.CATEGORY,
        P.MANUFACTURER,
        P.BRAND,
        COUNT(DISTINCT F.ORDER_ID)                          AS TOTAL_ORDERS,
        ROUND(SUM(F.QUANTITY), 0)                           AS TOTAL_UNITS_SOLD,
        ROUND(SUM(F.REVENUE), 2)                            AS TOTAL_REVENUE,
        ROUND(AVG(F.UNIT_PRICE), 2)                         AS AVG_UNIT_PRICE,
        COUNT(CASE WHEN F.RETURN_FLAG = 'R' THEN 1 END)    AS RETURNED_ITEMS,
        ROUND(
            100.0 * COUNT(CASE WHEN F.RETURN_FLAG = 'R' THEN 1 END)
            / NULLIF(COUNT(*), 0), 2
        )                                                   AS RETURN_RATE_PCT
    FROM RAW_SALES.GOLD.fact_orders F
    JOIN RAW_SALES.GOLD.dim_products P ON F.PRODUCT_ID = P.PRODUCT_ID
    JOIN RAW_SALES.GOLD.dim_dates   D ON F.DATE_KEY   = D.DATE_KEY
    GROUP BY D.YEAR, D.MONTH, D.MONTH_NAME, D.YEAR_MONTH, D.QUARTER,
             P.PRODUCT_ID, P.PRODUCT_NAME, P.CATEGORY, P.MANUFACTURER, P.BRAND
)
SELECT
    mp.*,
    DENSE_RANK() OVER (
        PARTITION BY mp.YEAR, mp.MONTH
        ORDER BY mp.TOTAL_REVENUE DESC
    )                                                       AS REVENUE_RANK,
    LAG(mp.TOTAL_REVENUE) OVER (
        PARTITION BY mp.PRODUCT_ID
        ORDER BY mp.YEAR, mp.MONTH
    )                                                       AS PREV_MONTH_REVENUE,
    ROUND(
        (mp.TOTAL_REVENUE - LAG(mp.TOTAL_REVENUE) OVER (
            PARTITION BY mp.PRODUCT_ID ORDER BY mp.YEAR, mp.MONTH
        )) / NULLIF(LAG(mp.TOTAL_REVENUE) OVER (
            PARTITION BY mp.PRODUCT_ID ORDER BY mp.YEAR, mp.MONTH
        ), 0) * 100, 2
    )                                                       AS MOM_GROWTH_PCT,
    CASE
        WHEN DENSE_RANK() OVER (
            PARTITION BY mp.YEAR, mp.MONTH ORDER BY mp.TOTAL_REVENUE DESC
        ) <= 50 AND mp.RETURN_RATE_PCT < 5  THEN 'Star'
        WHEN (mp.TOTAL_REVENUE - LAG(mp.TOTAL_REVENUE) OVER (
            PARTITION BY mp.PRODUCT_ID ORDER BY mp.YEAR, mp.MONTH
        )) / NULLIF(LAG(mp.TOTAL_REVENUE) OVER (
            PARTITION BY mp.PRODUCT_ID ORDER BY mp.YEAR, mp.MONTH
        ), 0) * 100 > 10                    THEN 'Growth'
        WHEN mp.RETURN_RATE_PCT >= 10        THEN 'Underperforming'
        ELSE                                      'Stable'
    END                                                     AS PERFORMANCE_TIER,
    CURRENT_TIMESTAMP()                                     AS _REFRESHED_AT
FROM monthly_product mp;


-- ============================================================================
-- PART B: TABLE COMMENT
-- ============================================================================

COMMENT ON TABLE RAW_SALES.GOLD.product_performance_analytics IS
    'Monthly product performance metrics with revenue ranking, return rates, and growth trends. Owner: Product. Refresh: Weekly (Tuesdays 7 AM).';


-- ============================================================================
-- PART C: UPDATE SLA MONITORING VIEW (recreate with 4 products)
-- ============================================================================

USE SCHEMA MONITORING;

CREATE OR REPLACE VIEW RAW_SALES.MONITORING.product_sla_status AS

-- Product 1: Sales Rep Monthly Performance (Daily)
SELECT
    'sales_rep_monthly_performance'     AS PRODUCT_NAME,
    'Sales Operations'                  AS OWNER,
    'Daily'                             AS REFRESH_FREQUENCY,
    MAX(_REFRESHED_AT)                  AS LAST_REFRESHED_AT,
    DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) AS HOURS_SINCE_REFRESH,
    CASE
        WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 24 THEN 'PASS'
        WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 48 THEN 'WARN'
        ELSE 'FAIL'
    END AS SLA_STATUS,
    (SELECT COUNT(*) FROM RAW_SALES.GOLD.sales_rep_monthly_performance) AS CURRENT_ROW_COUNT
FROM RAW_SALES.GOLD.sales_rep_monthly_performance

UNION ALL

-- Product 2: Customer Revenue Forecast (Monthly)
SELECT
    'customer_revenue_forecast',
    'Finance',
    'Monthly',
    MAX(_REFRESHED_AT),
    DATEDIFF(DAY, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()),
    CASE
        WHEN DATEDIFF(DAY, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 35 THEN 'PASS'
        WHEN DATEDIFF(DAY, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 50 THEN 'WARN'
        ELSE 'FAIL'
    END,
    (SELECT COUNT(*) FROM RAW_SALES.GOLD.customer_revenue_forecast)
FROM RAW_SALES.GOLD.customer_revenue_forecast

UNION ALL

-- Product 3: Customer Acquisition Cohort (Weekly)
SELECT
    'customer_acquisition_cohort',
    'Marketing',
    'Weekly',
    MAX(_REFRESHED_AT),
    DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()),
    CASE
        WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 168  THEN 'PASS'
        WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 240  THEN 'WARN'
        ELSE 'FAIL'
    END,
    (SELECT COUNT(*) FROM RAW_SALES.GOLD.customer_acquisition_cohort)
FROM RAW_SALES.GOLD.customer_acquisition_cohort

UNION ALL

-- Product 4: Product Performance Analytics (Weekly)  << NEW
SELECT
    'product_performance_analytics',
    'Product',
    'Weekly',
    MAX(_REFRESHED_AT),
    DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()),
    CASE
        WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 168  THEN 'PASS'
        WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 240  THEN 'WARN'
        ELSE 'FAIL'
    END,
    (SELECT COUNT(*) FROM RAW_SALES.GOLD.product_performance_analytics)
FROM RAW_SALES.GOLD.product_performance_analytics;


-- ============================================================================
-- PART D: RBAC — PRODUCT_ANALYSTS ROLE
-- ============================================================================

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS PRODUCT_ANALYSTS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH     TO ROLE PRODUCT_ANALYSTS;
GRANT USAGE ON DATABASE RAW_SALES          TO ROLE PRODUCT_ANALYSTS;
GRANT USAGE ON SCHEMA RAW_SALES.GOLD       TO ROLE PRODUCT_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.product_performance_analytics TO ROLE PRODUCT_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.product_performance           TO ROLE PRODUCT_ANALYSTS;

-- Also grant to the generic DATA_CONSUMERS role
GRANT SELECT ON TABLE RAW_SALES.GOLD.product_performance_analytics TO ROLE DATA_CONSUMERS;

USE ROLE SYSADMIN;


-- ============================================================================
-- PART E: VERIFICATION QUERIES
-- ============================================================================

-- Row count
SELECT COUNT(*) AS ROW_COUNT FROM RAW_SALES.GOLD.product_performance_analytics;

-- Top 10 products by revenue this month
SELECT TOP 10
    YEAR, MONTH, MONTH_NAME,
    PRODUCT_ID, PRODUCT_NAME, CATEGORY,
    TOTAL_REVENUE, TOTAL_UNITS_SOLD, RETURN_RATE_PCT,
    REVENUE_RANK, PERFORMANCE_TIER
FROM RAW_SALES.GOLD.product_performance_analytics
ORDER BY YEAR DESC, MONTH DESC, REVENUE_RANK ASC;

-- Performance tier distribution
SELECT
    PERFORMANCE_TIER,
    COUNT(*) AS PRODUCT_MONTHS,
    ROUND(AVG(TOTAL_REVENUE), 2) AS AVG_REVENUE,
    ROUND(AVG(RETURN_RATE_PCT), 2) AS AVG_RETURN_RATE
FROM RAW_SALES.GOLD.product_performance_analytics
GROUP BY PERFORMANCE_TIER
ORDER BY AVG_REVENUE DESC;

-- Month-over-month growth leaders
SELECT TOP 10
    YEAR_MONTH, PRODUCT_NAME, CATEGORY,
    TOTAL_REVENUE, PREV_MONTH_REVENUE, MOM_GROWTH_PCT
FROM RAW_SALES.GOLD.product_performance_analytics
WHERE MOM_GROWTH_PCT IS NOT NULL
ORDER BY MOM_GROWTH_PCT DESC;

-- SLA status check (should now show 4 products)
SELECT * FROM RAW_SALES.MONITORING.product_sla_status;
