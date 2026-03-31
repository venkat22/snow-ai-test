-- ============================================================================
-- PHASE 3: DATA PRODUCTS, GOVERNANCE & MARKETPLACE
-- Purpose: Curate 3 data products, set up access control, and prepare
--          for Snowflake Marketplace publishing
-- Prerequisite: 04_phase2_gold.sql completed successfully
-- Estimated runtime: ~5 minutes
-- ============================================================================

-- If unsure of your warehouse name, run: SHOW WAREHOUSES;
USE DATABASE RAW_SALES;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- PART A: CREATE DATA PRODUCTS (Curated tables in GOLD schema)
-- ============================================================================

USE SCHEMA GOLD;


-- --------------------------------------------------------------------------
-- DATA PRODUCT 1: sales_rep_monthly_performance
-- Audience: Sales managers, sales ops
-- SLA: Daily refresh, <1s latency, 100% completeness
-- --------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_SALES.GOLD.sales_rep_monthly_performance AS
SELECT
    D.YEAR,
    D.MONTH,
    D.MONTH_NAME,
    D.YEAR_MONTH,
    D.QUARTER,
    SR.REP_ID,
    SR.NAME                         AS REP_NAME,
    SR.TERRITORY_NAME,
    SR.REGION,
    SR.QUOTA,
    COUNT(DISTINCT F.CUSTOMER_ID)   AS UNIQUE_CUSTOMERS,
    COUNT(DISTINCT F.ORDER_ID)      AS ORDERS_CLOSED,
    ROUND(SUM(F.REVENUE), 2)        AS REVENUE_GENERATED,
    ROUND(AVG(F.REVENUE), 2)        AS AVG_REVENUE_PER_LINE,
    ROUND(SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0), 4) AS QUOTA_ATTAINMENT_RATIO,
    ROUND(SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) * 100, 1) AS QUOTA_ATTAINMENT_PCT,
    -- Month-over-month revenue growth
    LAG(ROUND(SUM(F.REVENUE), 2)) OVER (
        PARTITION BY SR.REP_ID ORDER BY D.YEAR, D.MONTH
    ) AS PREV_MONTH_REVENUE,
    ROUND(
        (SUM(F.REVENUE) - LAG(SUM(F.REVENUE)) OVER (PARTITION BY SR.REP_ID ORDER BY D.YEAR, D.MONTH))
        / NULLIF(LAG(SUM(F.REVENUE)) OVER (PARTITION BY SR.REP_ID ORDER BY D.YEAR, D.MONTH), 0) * 100,
        2
    ) AS MOM_GROWTH_PCT,
    CASE
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 1.0   THEN 'Exceeds Quota'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.8   THEN 'On Track'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.5   THEN 'Below Target'
        ELSE 'At Risk'
    END AS PERFORMANCE_STATUS,
    CURRENT_TIMESTAMP()             AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_dates D
    ON F.DATE_KEY = D.DATE_KEY
JOIN RAW_SALES.GOLD.dim_sales_reps SR
    ON F.REP_ID = SR.REP_ID
GROUP BY D.YEAR, D.MONTH, D.MONTH_NAME, D.YEAR_MONTH, D.QUARTER,
         SR.REP_ID, SR.NAME, SR.TERRITORY_NAME, SR.REGION, SR.QUOTA;

COMMENT ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance IS
    'DATA PRODUCT 1: Monthly KPIs for each sales rep. Owner: Sales Operations. SLA: Daily refresh by 9 AM, <1s latency, 100% completeness. Contact: sales-ops@company.com';


-- --------------------------------------------------------------------------
-- DATA PRODUCT 2: customer_revenue_forecast
-- Audience: Finance team, CFO
-- SLA: Monthly refresh, 95% forecast accuracy
-- --------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_revenue_forecast AS
WITH monthly_spend AS (
    SELECT
        F.CUSTOMER_ID,
        D.YEAR_MONTH,
        SUM(F.REVENUE) AS MONTHLY_REVENUE
    FROM RAW_SALES.GOLD.fact_orders F
    JOIN RAW_SALES.GOLD.dim_dates D
        ON F.DATE_KEY = D.DATE_KEY
    GROUP BY F.CUSTOMER_ID, D.YEAR_MONTH
),
customer_stats AS (
    SELECT
        CUSTOMER_ID,
        COUNT(DISTINCT YEAR_MONTH)                  AS MONTHS_ACTIVE,
        ROUND(AVG(MONTHLY_REVENUE), 2)              AS AVG_MONTHLY_REVENUE,
        ROUND(STDDEV_POP(MONTHLY_REVENUE), 2)       AS STDDEV_MONTHLY_REVENUE,
        ROUND(MAX(MONTHLY_REVENUE), 2)              AS MAX_MONTHLY_REVENUE,
        ROUND(MIN(MONTHLY_REVENUE), 2)              AS MIN_MONTHLY_REVENUE
    FROM monthly_spend
    GROUP BY CUSTOMER_ID
)
SELECT
    CS.CUSTOMER_ID,
    C.NAME,
    C.SEGMENT,
    C.TERRITORY_NAME,
    C.REGION,
    C.LIFETIME_VALUE,
    CS.MONTHS_ACTIVE,
    CS.AVG_MONTHLY_REVENUE,
    -- 12-month projection (simple: avg × 12)
    ROUND(CS.AVG_MONTHLY_REVENUE * 12, 2)                           AS PROJECTED_12MONTH_REVENUE,
    -- Conservative: -1 standard deviation
    ROUND(GREATEST((CS.AVG_MONTHLY_REVENUE - CS.STDDEV_MONTHLY_REVENUE) * 12, 0), 2) AS CONSERVATIVE_FORECAST,
    -- Optimistic: +1 standard deviation
    ROUND((CS.AVG_MONTHLY_REVENUE + CS.STDDEV_MONTHLY_REVENUE) * 12, 2) AS OPTIMISTIC_FORECAST,
    ROUND(CS.STDDEV_MONTHLY_REVENUE, 2)                             AS REVENUE_VOLATILITY,
    CASE
        WHEN CS.AVG_MONTHLY_REVENUE IS NULL OR CS.AVG_MONTHLY_REVENUE = 0 THEN 'Inactive'
        WHEN CS.AVG_MONTHLY_REVENUE < 5000    THEN 'Low'
        WHEN CS.AVG_MONTHLY_REVENUE < 20000   THEN 'Medium'
        ELSE 'High'
    END AS FORECAST_TIER,
    CURRENT_TIMESTAMP()                                             AS _REFRESHED_AT
FROM customer_stats CS
JOIN RAW_SALES.GOLD.dim_customers C
    ON CS.CUSTOMER_ID = C.CUSTOMER_ID;

COMMENT ON TABLE RAW_SALES.GOLD.customer_revenue_forecast IS
    'DATA PRODUCT 2: 12-month revenue projections per customer with confidence range. Owner: Finance. SLA: Monthly refresh, 95% accuracy vs actuals. Contact: finance@company.com';


-- --------------------------------------------------------------------------
-- DATA PRODUCT 3: customer_acquisition_cohort
-- Audience: Marketing, growth team
-- SLA: Weekly refresh, <100ms query latency
-- --------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_acquisition_cohort AS
WITH cohort_base AS (
    SELECT
        C.CUSTOMER_ID,
        DATE_TRUNC('MONTH', C.FIRST_ORDER_DATE)         AS COHORT_MONTH,
        DATE_TRUNC('MONTH', O.ORDER_DATE)               AS ORDER_MONTH,
        DATEDIFF(MONTH,
            DATE_TRUNC('MONTH', C.FIRST_ORDER_DATE),
            DATE_TRUNC('MONTH', O.ORDER_DATE))           AS MONTHS_SINCE_ACQUISITION,
        O.ORDER_AMOUNT
    FROM RAW_SALES.GOLD.dim_customers C
    JOIN RAW_SALES.SILVER.orders O
        ON C.CUSTOMER_ID = O.CUSTOMER_ID
    WHERE C.FIRST_ORDER_DATE IS NOT NULL
),
cohort_sizes AS (
    SELECT COHORT_MONTH, COUNT(DISTINCT CUSTOMER_ID) AS COHORT_SIZE
    FROM cohort_base
    WHERE MONTHS_SINCE_ACQUISITION = 0  -- Only founding month
    GROUP BY COHORT_MONTH
)
SELECT
    CB.COHORT_MONTH,
    CS.COHORT_SIZE,
    CB.MONTHS_SINCE_ACQUISITION,
    COUNT(DISTINCT CB.CUSTOMER_ID)                      AS ACTIVE_CUSTOMERS,
    ROUND(100.0 * COUNT(DISTINCT CB.CUSTOMER_ID) / CS.COHORT_SIZE, 2) AS RETENTION_PCT,
    ROUND(SUM(CB.ORDER_AMOUNT), 2)                      AS COHORT_REVENUE,
    ROUND(SUM(CB.ORDER_AMOUNT) / CS.COHORT_SIZE, 2)    AS LTV_PER_ACQUIREE,
    CURRENT_TIMESTAMP()                                 AS _REFRESHED_AT
FROM cohort_base CB
JOIN cohort_sizes CS
    ON CB.COHORT_MONTH = CS.COHORT_MONTH
GROUP BY CB.COHORT_MONTH, CS.COHORT_SIZE, CB.MONTHS_SINCE_ACQUISITION
ORDER BY CB.COHORT_MONTH, CB.MONTHS_SINCE_ACQUISITION;

COMMENT ON TABLE RAW_SALES.GOLD.customer_acquisition_cohort IS
    'DATA PRODUCT 3: Customer retention and LTV by acquisition cohort month. Owner: Marketing. SLA: Weekly refresh Mondays 8 AM, <100ms query latency. Contact: marketing@company.com';


-- ============================================================================
-- PART B: ACCESS CONTROL — Roles, Grants, and PII Protection
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Must be ACCOUNTADMIN to create roles and grant privileges

-- Create consumer roles (least-privilege by domain)
CREATE ROLE IF NOT EXISTS SALES_ANALYSTS;
CREATE ROLE IF NOT EXISTS FINANCE_ANALYSTS;
CREATE ROLE IF NOT EXISTS MARKETING_ANALYSTS;
CREATE ROLE IF NOT EXISTS DATA_CONSUMERS;           -- Generic read-only role

-- Grant warehouse usage (all roles need compute)
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE SALES_ANALYSTS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE FINANCE_ANALYSTS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE MARKETING_ANALYSTS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE DATA_CONSUMERS;

-- Grant database/schema access
GRANT USAGE ON DATABASE RAW_SALES TO ROLE SALES_ANALYSTS;
GRANT USAGE ON DATABASE RAW_SALES TO ROLE FINANCE_ANALYSTS;
GRANT USAGE ON DATABASE RAW_SALES TO ROLE MARKETING_ANALYSTS;
GRANT USAGE ON DATABASE RAW_SALES TO ROLE DATA_CONSUMERS;

GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE SALES_ANALYSTS;
GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE FINANCE_ANALYSTS;
GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE MARKETING_ANALYSTS;
GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE DATA_CONSUMERS;

-- SALES_ANALYSTS: Access to Sales Rep Performance and all Gold tables
GRANT SELECT ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance TO ROLE SALES_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.sales_rep_scorecard TO ROLE SALES_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.monthly_sales_summary TO ROLE SALES_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.product_performance TO ROLE SALES_ANALYSTS;

-- FINANCE_ANALYSTS: Access to revenue forecast and financial aggregates
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_revenue_forecast TO ROLE FINANCE_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.monthly_sales_summary TO ROLE FINANCE_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_lifetime_value TO ROLE FINANCE_ANALYSTS;

-- MARKETING_ANALYSTS: Access to cohort + segmentation tables
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_acquisition_cohort TO ROLE MARKETING_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_segmentation TO ROLE MARKETING_ANALYSTS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_lifetime_value TO ROLE MARKETING_ANALYSTS;

-- DATA_CONSUMERS: Generic read on all 3 published data products
GRANT SELECT ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance TO ROLE DATA_CONSUMERS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_revenue_forecast TO ROLE DATA_CONSUMERS;
GRANT SELECT ON TABLE RAW_SALES.GOLD.customer_acquisition_cohort TO ROLE DATA_CONSUMERS;

-- Verify grants
SHOW GRANTS ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance;


-- ============================================================================
-- PART C: SLA MONITORING VIEW
-- ============================================================================

USE SCHEMA MONITORING;

CREATE OR REPLACE VIEW RAW_SALES.MONITORING.product_sla_status AS
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
-- PART D: PRE-REFRESH VALIDATION PROCEDURE
-- Run this before each data product refresh to confirm source data is ready
-- ============================================================================

CREATE OR REPLACE PROCEDURE RAW_SALES.MONITORING.validate_before_refresh(product_name STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var checks = [];
  var isValid = true;

  if (PRODUCT_NAME === 'sales_rep_monthly_performance') {
    // Check 1: Silver orders must have > 100 rows
    var rs = snowflake.execute({ sqlText: "SELECT COUNT(*) AS CNT FROM RAW_SALES.SILVER.orders" });
    rs.next();
    var cnt = rs.getColumnValue(1);
    if (cnt < 100) {
      checks.push("FAIL: Silver orders has " + cnt + " rows (min 100 required)");
      isValid = false;
    } else {
      checks.push("PASS: Silver orders has " + cnt + " rows");
    }

    // Check 2: Sales reps must have > 0 active rows
    rs = snowflake.execute({ sqlText: "SELECT COUNT(*) AS CNT FROM RAW_SALES.SILVER.sales_reps WHERE STATUS = 'Active'" });
    rs.next();
    cnt = rs.getColumnValue(1);
    if (cnt === 0) {
      checks.push("FAIL: No active sales reps found");
      isValid = false;
    } else {
      checks.push("PASS: " + cnt + " active sales reps");
    }
  }

  if (PRODUCT_NAME === 'customer_revenue_forecast') {
    // Check: dim_customers must be populated
    var rs = snowflake.execute({ sqlText: "SELECT COUNT(*) AS CNT FROM RAW_SALES.GOLD.dim_customers" });
    rs.next();
    var cnt = rs.getColumnValue(1);
    if (cnt < 1000) {
      checks.push("FAIL: dim_customers has only " + cnt + " rows (min 1000 required)");
      isValid = false;
    } else {
      checks.push("PASS: dim_customers has " + cnt + " rows");
    }
  }

  return (isValid ? "VALIDATION PASSED\n" : "VALIDATION FAILED\n") + checks.join("\n");
$$;

-- Test the procedure
CALL RAW_SALES.MONITORING.validate_before_refresh('sales_rep_monthly_performance');
CALL RAW_SALES.MONITORING.validate_before_refresh('customer_revenue_forecast');


-- ============================================================================
-- PART D2: TASK ORCHESTRATION (Mandatory platform capability)
-- Tasks run SLA pre-refresh validation on product schedules and log outcomes.
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.MONITORING.task_run_audit (
        AUDIT_ID             INT AUTOINCREMENT PRIMARY KEY,
        TASK_NAME            VARCHAR(255),
        PRODUCT_NAME         VARCHAR(255),
        EXECUTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        RESULT_STATUS        VARCHAR(20),
        RESULT_MESSAGE       VARCHAR(2000)
);

CREATE OR REPLACE PROCEDURE RAW_SALES.MONITORING.run_product_validation(product_name STRING, task_name STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var rs = snowflake.execute({
        sqlText: "CALL RAW_SALES.MONITORING.validate_before_refresh(?)",
        binds: [PRODUCT_NAME]
    });
    rs.next();
    var resultMsg = rs.getColumnValue(1);
    var status = resultMsg.startsWith("VALIDATION PASSED") ? "PASS" : "FAIL";

    snowflake.execute({
        sqlText: `INSERT INTO RAW_SALES.MONITORING.task_run_audit
                            (TASK_NAME, PRODUCT_NAME, RESULT_STATUS, RESULT_MESSAGE)
                            VALUES (?, ?, ?, ?)`,
        binds: [TASK_NAME, PRODUCT_NAME, status, resultMsg]
    });

    return status + '\n' + resultMsg;
$$;

CREATE OR REPLACE TASK RAW_SALES.MONITORING.task_validate_sales_rep_monthly
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 13 * * * UTC'  -- 9 AM ET approx (DST-neutral guidance)
AS
    CALL RAW_SALES.MONITORING.run_product_validation('sales_rep_monthly_performance', 'task_validate_sales_rep_monthly');

CREATE OR REPLACE TASK RAW_SALES.MONITORING.task_validate_customer_revenue_forecast
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 14 2 * * UTC'  -- Monthly refresh check on day 2
AS
    CALL RAW_SALES.MONITORING.run_product_validation('customer_revenue_forecast', 'task_validate_customer_revenue_forecast');

CREATE OR REPLACE TASK RAW_SALES.MONITORING.task_validate_customer_acquisition_cohort
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 13 * * MON UTC'  -- Weekly check Monday
AS
    CALL RAW_SALES.MONITORING.run_product_validation('customer_acquisition_cohort', 'task_validate_customer_acquisition_cohort');

-- Keep tasks suspended by default for controlled rollout.
ALTER TASK RAW_SALES.MONITORING.task_validate_sales_rep_monthly SUSPEND;
ALTER TASK RAW_SALES.MONITORING.task_validate_customer_revenue_forecast SUSPEND;
ALTER TASK RAW_SALES.MONITORING.task_validate_customer_acquisition_cohort SUSPEND;

-- Manual run (optional): EXECUTE TASK RAW_SALES.MONITORING.task_validate_sales_rep_monthly;
SHOW TASKS IN SCHEMA RAW_SALES.MONITORING;


-- ============================================================================
-- PART E: VERIFICATION — All 3 Data Products Working
-- ============================================================================

-- Data Product 1: Sales Rep Monthly Performance (sample)
SELECT YEAR, MONTH, REP_NAME, TERRITORY_NAME, REVENUE_GENERATED, QUOTA_ATTAINMENT_PCT, PERFORMANCE_STATUS
FROM RAW_SALES.GOLD.sales_rep_monthly_performance
WHERE YEAR = (SELECT MAX(YEAR) FROM RAW_SALES.GOLD.sales_rep_monthly_performance)
ORDER BY REVENUE_GENERATED DESC
LIMIT 10;


-- Data Product 2: Customer Revenue Forecast (top forecasted customers)
SELECT CUSTOMER_ID, NAME, SEGMENT, PROJECTED_12MONTH_REVENUE, CONSERVATIVE_FORECAST, OPTIMISTIC_FORECAST, FORECAST_TIER
FROM RAW_SALES.GOLD.customer_revenue_forecast
ORDER BY PROJECTED_12MONTH_REVENUE DESC
LIMIT 10;


-- Data Product 3: Cohort Analysis (first cohort, first 12 months)
SELECT COHORT_MONTH, MONTHS_SINCE_ACQUISITION, COHORT_SIZE, ACTIVE_CUSTOMERS, RETENTION_PCT, LTV_PER_ACQUIREE
FROM RAW_SALES.GOLD.customer_acquisition_cohort
WHERE COHORT_MONTH = (SELECT MIN(COHORT_MONTH) FROM RAW_SALES.GOLD.customer_acquisition_cohort)
ORDER BY MONTHS_SINCE_ACQUISITION
LIMIT 12;


-- SLA Dashboard
SELECT * FROM RAW_SALES.MONITORING.product_sla_status;

-- Task audit sample
SELECT *
FROM RAW_SALES.MONITORING.task_run_audit
ORDER BY EXECUTED_AT DESC
LIMIT 20;


-- ============================================================================
-- PART F: SNOWFLAKE MARKETPLACE — Steps (Manual in UI, guided by notes below)
-- ============================================================================

-- After verifying data products above, follow these steps in Snowflake UI:
--
-- 1. Go to: Data > Provider Studio > + New Listing
-- 2. Title: "Sales Analytics Data Bundle"
-- 3. Description: (copy from data_products.md)
-- 4. Add data objects:
--    • RAW_SALES.GOLD.sales_rep_monthly_performance
--    • RAW_SALES.GOLD.customer_revenue_forecast
--    • RAW_SALES.GOLD.customer_acquisition_cohort
-- 5. Set visibility: Public (auto-approved) or Private (request required)
-- 6. Contact email: your email
-- 7. Publish!
--
-- To test as a consumer:
--    Option A — use a second Snowflake account and search for your listing
--    Option B — share via Private Listing with your own second account email
--
-- Verify consumer can run:
--    SELECT * FROM <shared_db>.GOLD.sales_rep_monthly_performance LIMIT 10;

-- ============================================================================
-- PHASE 3 COMPLETE ✓
-- All 3 data products created, access control configured, SLA monitoring live
-- ============================================================================
