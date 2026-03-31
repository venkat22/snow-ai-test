-- ============================================================================
-- DROP DATA PRODUCT 4: Product Performance Analytics
-- ============================================================================
-- Removes the product_performance_analytics data product and cleans up
-- grants and SLA monitoring.
--
-- Prerequisites: Run with a role that has ownership on the objects.
-- ============================================================================

USE DATABASE RAW_SALES;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- STEP 1: REVOKE GRANTS (skipped — roles do not exist in this account)
-- ============================================================================

-- REVOKE SELECT ON TABLE RAW_SALES.GOLD.product_performance_analytics FROM ROLE PRODUCT_ANALYSTS;
-- REVOKE SELECT ON TABLE RAW_SALES.GOLD.product_performance_analytics FROM ROLE DATA_CONSUMERS;


-- ============================================================================
-- STEP 2: DROP THE TABLE
-- ============================================================================

DROP TABLE IF EXISTS RAW_SALES.GOLD.product_performance_analytics;


-- ============================================================================
-- STEP 3: RECREATE SLA MONITORING VIEW WITHOUT PRODUCT 4
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
FROM RAW_SALES.GOLD.customer_acquisition_cohort;


-- ============================================================================
-- STEP 4: VERIFY
-- ============================================================================

-- Confirm table is gone
SELECT TABLE_NAME
FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'GOLD'
  AND TABLE_NAME = 'PRODUCT_PERFORMANCE_ANALYTICS';

-- SLA view should now show 3 products
SELECT * FROM RAW_SALES.MONITORING.product_sla_status;
