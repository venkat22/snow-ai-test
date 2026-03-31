-- ============================================================================
-- DEMO STEP 6: DATA PRODUCTS & GOVERNANCE
-- ============================================================================
-- What this does:
--   Creates 3 curated data products with SLA metadata:
--     1. sales_rep_monthly_performance (Sales Ops, daily refresh)
--     2. customer_revenue_forecast (Finance, monthly refresh)
--     3. customer_acquisition_cohort (Marketing, weekly refresh)
--   Sets up RBAC roles, SLA monitoring, pre-refresh validation, and Tasks.
--
-- Talk track:
--   "Data products are purpose-built, SLA-governed tables. Each has an owner,
--    a refresh cadence, and automated pre-refresh validation via Snowflake
--    Tasks. We also set up role-based access — Sales Analysts only see sales
--    data, Finance only sees forecasts, etc."
--
-- Runtime: ~5 minutes
-- Prerequisites: Step 05 completed
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA GOLD;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- DATA PRODUCT 1: Sales Rep Monthly Performance
-- Owner: Sales Operations | SLA: Daily refresh by 9 AM
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.sales_rep_monthly_performance AS
SELECT
    D.YEAR, D.MONTH, D.MONTH_NAME, D.YEAR_MONTH, D.QUARTER,
    SR.REP_ID, SR.NAME AS REP_NAME, SR.TERRITORY_NAME, SR.REGION, SR.QUOTA,
    COUNT(DISTINCT F.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    COUNT(DISTINCT F.ORDER_ID) AS ORDERS_CLOSED,
    ROUND(SUM(F.REVENUE), 2) AS REVENUE_GENERATED,
    ROUND(SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) * 100, 1) AS QUOTA_ATTAINMENT_PCT,
    LAG(ROUND(SUM(F.REVENUE), 2)) OVER (PARTITION BY SR.REP_ID ORDER BY D.YEAR, D.MONTH) AS PREV_MONTH_REVENUE,
    ROUND(
        (SUM(F.REVENUE) - LAG(SUM(F.REVENUE)) OVER (PARTITION BY SR.REP_ID ORDER BY D.YEAR, D.MONTH))
        / NULLIF(LAG(SUM(F.REVENUE)) OVER (PARTITION BY SR.REP_ID ORDER BY D.YEAR, D.MONTH), 0) * 100, 2
    ) AS MOM_GROWTH_PCT,
    CASE
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 1.0 THEN 'Exceeds Quota'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.8 THEN 'On Track'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.5 THEN 'Below Target'
        ELSE 'At Risk'
    END AS PERFORMANCE_STATUS,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_dates D ON F.DATE_KEY = D.DATE_KEY
JOIN RAW_SALES.GOLD.dim_sales_reps SR ON F.REP_ID = SR.REP_ID
GROUP BY D.YEAR, D.MONTH, D.MONTH_NAME, D.YEAR_MONTH, D.QUARTER,
         SR.REP_ID, SR.NAME, SR.TERRITORY_NAME, SR.REGION, SR.QUOTA;

COMMENT ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance IS
    'DATA PRODUCT 1: Monthly KPIs per sales rep. Owner: Sales Ops. SLA: Daily by 9 AM.';


-- ============================================================================
-- DATA PRODUCT 2: Customer Revenue Forecast
-- Owner: Finance | SLA: Monthly refresh, 95% accuracy
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_revenue_forecast AS
WITH monthly_spend AS (
    SELECT F.CUSTOMER_ID, D.YEAR_MONTH, SUM(F.REVENUE) AS MONTHLY_REVENUE
    FROM RAW_SALES.GOLD.fact_orders F
    JOIN RAW_SALES.GOLD.dim_dates D ON F.DATE_KEY = D.DATE_KEY
    GROUP BY F.CUSTOMER_ID, D.YEAR_MONTH
),
customer_stats AS (
    SELECT CUSTOMER_ID,
        COUNT(DISTINCT YEAR_MONTH) AS MONTHS_ACTIVE,
        ROUND(AVG(MONTHLY_REVENUE), 2) AS AVG_MONTHLY_REVENUE,
        ROUND(STDDEV_POP(MONTHLY_REVENUE), 2) AS STDDEV_MONTHLY_REVENUE
    FROM monthly_spend GROUP BY CUSTOMER_ID
)
SELECT
    CS.CUSTOMER_ID, C.NAME, C.SEGMENT, C.TERRITORY_NAME, C.REGION, C.LIFETIME_VALUE,
    CS.MONTHS_ACTIVE, CS.AVG_MONTHLY_REVENUE,
    ROUND(CS.AVG_MONTHLY_REVENUE * 12, 2) AS PROJECTED_12MONTH_REVENUE,
    ROUND(GREATEST((CS.AVG_MONTHLY_REVENUE - CS.STDDEV_MONTHLY_REVENUE) * 12, 0), 2) AS CONSERVATIVE_FORECAST,
    ROUND((CS.AVG_MONTHLY_REVENUE + CS.STDDEV_MONTHLY_REVENUE) * 12, 2) AS OPTIMISTIC_FORECAST,
    CASE
        WHEN CS.AVG_MONTHLY_REVENUE < 5000 THEN 'Low'
        WHEN CS.AVG_MONTHLY_REVENUE < 20000 THEN 'Medium'
        ELSE 'High'
    END AS FORECAST_TIER,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM customer_stats CS
JOIN RAW_SALES.GOLD.dim_customers C ON CS.CUSTOMER_ID = C.CUSTOMER_ID;

COMMENT ON TABLE RAW_SALES.GOLD.customer_revenue_forecast IS
    'DATA PRODUCT 2: 12-month revenue projections per customer. Owner: Finance. SLA: Monthly.';


-- ============================================================================
-- DATA PRODUCT 3: Customer Acquisition Cohort
-- Owner: Marketing | SLA: Weekly refresh Mondays
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_acquisition_cohort AS
WITH cohort_base AS (
    SELECT C.CUSTOMER_ID,
        DATE_TRUNC('MONTH', C.FIRST_ORDER_DATE) AS COHORT_MONTH,
        DATE_TRUNC('MONTH', O.ORDER_DATE) AS ORDER_MONTH,
        DATEDIFF(MONTH, DATE_TRUNC('MONTH', C.FIRST_ORDER_DATE), DATE_TRUNC('MONTH', O.ORDER_DATE)) AS MONTHS_SINCE_ACQUISITION,
        O.ORDER_AMOUNT
    FROM RAW_SALES.GOLD.dim_customers C
    JOIN RAW_SALES.SILVER.orders O ON C.CUSTOMER_ID = O.CUSTOMER_ID
    WHERE C.FIRST_ORDER_DATE IS NOT NULL
),
cohort_sizes AS (
    SELECT COHORT_MONTH, COUNT(DISTINCT CUSTOMER_ID) AS COHORT_SIZE
    FROM cohort_base WHERE MONTHS_SINCE_ACQUISITION = 0
    GROUP BY COHORT_MONTH
)
SELECT
    CB.COHORT_MONTH, CS.COHORT_SIZE, CB.MONTHS_SINCE_ACQUISITION,
    COUNT(DISTINCT CB.CUSTOMER_ID) AS ACTIVE_CUSTOMERS,
    ROUND(100.0 * COUNT(DISTINCT CB.CUSTOMER_ID) / CS.COHORT_SIZE, 2) AS RETENTION_PCT,
    ROUND(SUM(CB.ORDER_AMOUNT), 2) AS COHORT_REVENUE,
    ROUND(SUM(CB.ORDER_AMOUNT) / CS.COHORT_SIZE, 2) AS LTV_PER_ACQUIREE,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM cohort_base CB
JOIN cohort_sizes CS ON CB.COHORT_MONTH = CS.COHORT_MONTH
GROUP BY CB.COHORT_MONTH, CS.COHORT_SIZE, CB.MONTHS_SINCE_ACQUISITION;

COMMENT ON TABLE RAW_SALES.GOLD.customer_acquisition_cohort IS
    'DATA PRODUCT 3: Cohort retention and LTV. Owner: Marketing. SLA: Weekly Mondays.';


-- ============================================================================
-- RBAC: Role-based access control
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS SALES_ANALYSTS;
CREATE ROLE IF NOT EXISTS FINANCE_ANALYSTS;
CREATE ROLE IF NOT EXISTS MARKETING_ANALYSTS;
CREATE ROLE IF NOT EXISTS DATA_CONSUMERS;

-- Grant warehouse + database access to all roles
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE SALES_ANALYSTS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE FINANCE_ANALYSTS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE MARKETING_ANALYSTS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE DATA_CONSUMERS;

GRANT USAGE ON DATABASE RAW_SALES TO ROLE SALES_ANALYSTS;
GRANT USAGE ON DATABASE RAW_SALES TO ROLE FINANCE_ANALYSTS;
GRANT USAGE ON DATABASE RAW_SALES TO ROLE MARKETING_ANALYSTS;
GRANT USAGE ON DATABASE RAW_SALES TO ROLE DATA_CONSUMERS;

GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE SALES_ANALYSTS;
GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE FINANCE_ANALYSTS;
GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE MARKETING_ANALYSTS;
GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE DATA_CONSUMERS;

-- Role-specific table grants
GRANT SELECT ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance TO ROLE SALES_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.sales_rep_scorecard TO ROLE SALES_ANALYSTS;

GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_revenue_forecast TO ROLE FINANCE_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.monthly_sales_summary TO ROLE FINANCE_ANALYSTS;

GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_acquisition_cohort TO ROLE MARKETING_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_segmentation TO ROLE MARKETING_ANALYSTS;

-- Generic consumer gets all 3 data products
GRANT SELECT ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance TO ROLE DATA_CONSUMERS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_revenue_forecast TO ROLE DATA_CONSUMERS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_acquisition_cohort TO ROLE DATA_CONSUMERS;


-- ============================================================================
-- SLA MONITORING VIEW
-- ============================================================================

USE SCHEMA MONITORING;

CREATE OR REPLACE VIEW RAW_SALES.MONITORING.product_sla_status AS
SELECT 'sales_rep_monthly_performance' AS PRODUCT_NAME, 'Sales Ops' AS OWNER, 'Daily' AS REFRESH_FREQ,
    MAX(_REFRESHED_AT) AS LAST_REFRESHED,
    DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) AS HOURS_SINCE_REFRESH,
    CASE WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 24 THEN 'PASS' ELSE 'FAIL' END AS SLA_STATUS
FROM RAW_SALES.GOLD.sales_rep_monthly_performance
UNION ALL
SELECT 'customer_revenue_forecast', 'Finance', 'Monthly', MAX(_REFRESHED_AT),
    DATEDIFF(DAY, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF(DAY, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 35 THEN 'PASS' ELSE 'FAIL' END
FROM RAW_SALES.GOLD.customer_revenue_forecast
UNION ALL
SELECT 'customer_acquisition_cohort', 'Marketing', 'Weekly', MAX(_REFRESHED_AT),
    DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 168 THEN 'PASS' ELSE 'FAIL' END
FROM RAW_SALES.GOLD.customer_acquisition_cohort;


-- ============================================================================
-- VALIDATION PROCEDURE & TASKS
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.MONITORING.task_run_audit (
    AUDIT_ID INT AUTOINCREMENT PRIMARY KEY,
    TASK_NAME VARCHAR(255), PRODUCT_NAME VARCHAR(255),
    EXECUTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESULT_STATUS VARCHAR(20), RESULT_MESSAGE VARCHAR(2000)
);

CREATE OR REPLACE PROCEDURE RAW_SALES.MONITORING.validate_before_refresh(product_name STRING)
RETURNS STRING LANGUAGE JAVASCRIPT AS
$$
  var checks = [];
  var isValid = true;
  if (PRODUCT_NAME === 'sales_rep_monthly_performance') {
    var rs = snowflake.execute({ sqlText: "SELECT COUNT(*) FROM RAW_SALES.SILVER.orders" });
    rs.next(); var cnt = rs.getColumnValue(1);
    if (cnt < 100) { checks.push("FAIL: orders=" + cnt); isValid = false; }
    else { checks.push("PASS: orders=" + cnt); }
  }
  return (isValid ? "VALIDATION PASSED\n" : "VALIDATION FAILED\n") + checks.join("\n");
$$;

CREATE OR REPLACE TASK RAW_SALES.MONITORING.task_validate_sales_rep_monthly
    WAREHOUSE = ANALYTICS_WH SCHEDULE = 'USING CRON 0 13 * * * UTC'
AS CALL RAW_SALES.MONITORING.validate_before_refresh('sales_rep_monthly_performance');

ALTER TASK RAW_SALES.MONITORING.task_validate_sales_rep_monthly SUSPEND;
SHOW TASKS IN SCHEMA RAW_SALES.MONITORING;


-- ============================================================================
-- VALIDATION: Preview data products
-- ============================================================================

-- Data Product 1 sample
SELECT YEAR, MONTH, REP_NAME, REVENUE_GENERATED, QUOTA_ATTAINMENT_PCT, PERFORMANCE_STATUS
FROM RAW_SALES.GOLD.sales_rep_monthly_performance
WHERE YEAR = (SELECT MAX(YEAR) FROM RAW_SALES.GOLD.sales_rep_monthly_performance)
ORDER BY REVENUE_GENERATED DESC LIMIT 10;

-- Data Product 2 sample
SELECT NAME, SEGMENT, PROJECTED_12MONTH_REVENUE, CONSERVATIVE_FORECAST, OPTIMISTIC_FORECAST, FORECAST_TIER
FROM RAW_SALES.GOLD.customer_revenue_forecast ORDER BY PROJECTED_12MONTH_REVENUE DESC LIMIT 10;

-- Data Product 3 sample
SELECT COHORT_MONTH, MONTHS_SINCE_ACQUISITION, COHORT_SIZE, ACTIVE_CUSTOMERS, RETENTION_PCT
FROM RAW_SALES.GOLD.customer_acquisition_cohort
WHERE COHORT_MONTH = (SELECT MIN(COHORT_MONTH) FROM RAW_SALES.GOLD.customer_acquisition_cohort)
ORDER BY MONTHS_SINCE_ACQUISITION LIMIT 12;

-- SLA dashboard
SELECT * FROM RAW_SALES.MONITORING.product_sla_status;


-- ============================================================================
-- CHECKPOINT: 3 data products live, RBAC configured, SLA monitoring active.
-- NEXT: Run 07_feature_store_ml.sql
-- ============================================================================
