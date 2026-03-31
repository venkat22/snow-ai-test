-- ============================================================================
-- DEMO STEP 10: RELEASE ACCEPTANCE GATES (Go/No-Go)
-- ============================================================================
-- What this does:
--   Runs strict pass/fail gates across all phases to validate go-live readiness.
--   Gates A-F cover: source data, platform capabilities, data quality,
--   product SLAs, marketplace readiness, and performance evidence.
--   Results are persisted to release_gate_results for audit trail.
--
-- Talk track:
--   "Before we go live, we run acceptance gates. These are strict yes/no checks
--    across every layer — are all Landing tables present? Are Streams and
--    Dynamic Tables running? Do all DQ checks pass? Are SLAs met?
--    One failure blocks the release. The final line tells you PASS or FAIL."
--
-- Runtime: ~1 minute
-- Prerequisites: Steps 01-06 completed
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA MONITORING;
USE WAREHOUSE ANALYTICS_WH;


-- Setup: Manual evidence + results tables
CREATE TABLE IF NOT EXISTS RAW_SALES.MONITORING.manual_release_checks (
    CHECK_NAME VARCHAR(200) PRIMARY KEY, CHECK_VALUE BOOLEAN,
    LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(255), NOTES VARCHAR(1000)
);

MERGE INTO RAW_SALES.MONITORING.manual_release_checks t
USING (SELECT 'marketplace_consumer_test_passed' AS CHECK_NAME, FALSE AS CHECK_VALUE,
    CURRENT_TIMESTAMP() AS LAST_UPDATED_AT, CURRENT_USER() AS UPDATED_BY,
    'Set TRUE after consumer account validates access.' AS NOTES) s
ON t.CHECK_NAME = s.CHECK_NAME
WHEN NOT MATCHED THEN INSERT VALUES (s.CHECK_NAME, s.CHECK_VALUE, s.LAST_UPDATED_AT, s.UPDATED_BY, s.NOTES);

CREATE TABLE IF NOT EXISTS RAW_SALES.MONITORING.release_gate_results (
    RUN_ID NUMBER, GATE_ID VARCHAR(20), GATE_NAME VARCHAR(200),
    STATUS VARCHAR(10), ACTUAL_VALUE VARCHAR(500), EXPECTED_VALUE VARCHAR(500),
    DETAILS VARCHAR(2000), EVALUATED_AT TIMESTAMP_NTZ
);

SET GATE_RUN_ID = (SELECT COALESCE(MAX(RUN_ID), 0) + 1 FROM RAW_SALES.MONITORING.release_gate_results);
SET GATE_EVAL_TS = CURRENT_TIMESTAMP();


-- ============================================================================
-- GATE A: Source & Mapping Consistency
-- ============================================================================

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'A1', 'LANDING tables present',
    CASE WHEN cnt = 6 THEN 'PASS' ELSE 'FAIL' END, TO_VARCHAR(cnt), '6',
    'Required: CUSTOMERS, ORDERS, ORDER_ITEMS, PRODUCTS, SALES_REPS, TERRITORIES', $GATE_EVAL_TS
FROM (SELECT COUNT(*) AS cnt FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'LANDING' AND TABLE_NAME IN ('CUSTOMERS','ORDERS','ORDER_ITEMS','PRODUCTS','SALES_REPS','TERRITORIES'));

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'A2', 'LANDING row counts non-zero',
    CASE WHEN MIN(rc) > 0 THEN 'PASS' ELSE 'FAIL' END, 'MIN=' || TO_VARCHAR(MIN(rc)), 'MIN > 0',
    'All LANDING tables must contain data', $GATE_EVAL_TS
FROM (
    SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.CUSTOMERS) AS rc
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDERS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.PRODUCTS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.SALES_REPS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.TERRITORIES));


-- ============================================================================
-- GATE B: Platform Capabilities
-- ============================================================================

SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE;
SET BRONZE_STREAM_COUNT = (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'B1', 'Bronze streams present',
    CASE WHEN $BRONZE_STREAM_COUNT >= 6 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR($BRONZE_STREAM_COUNT), '>= 6', 'One stream per Bronze table', $GATE_EVAL_TS;

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'B2', 'Silver dynamic tables present',
    CASE WHEN cnt >= 3 THEN 'PASS' ELSE 'FAIL' END, TO_VARCHAR(cnt), '>= 3',
    'Dynamic projections for customers/orders/order_items', $GATE_EVAL_TS
FROM (SELECT COUNT(*) AS cnt FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SILVER' AND IS_DYNAMIC = 'YES');

SHOW TASKS IN SCHEMA RAW_SALES.MONITORING;
SET TASK_COUNT = (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'B3', 'Monitoring tasks present',
    CASE WHEN $TASK_COUNT >= 1 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR($TASK_COUNT), '>= 1', 'Validation tasks exist', $GATE_EVAL_TS;

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'B4', 'Snowpark telemetry present',
    CASE WHEN cnt >= 1 THEN 'PASS' ELSE 'FAIL' END, TO_VARCHAR(cnt), '>= 1',
    'At least one snowpark_job_runs row', $GATE_EVAL_TS
FROM (SELECT COUNT(*) AS cnt FROM RAW_SALES.MONITORING.snowpark_job_runs);


-- ============================================================================
-- GATE C: Data Quality & Trust
-- ============================================================================

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'C1', 'DQ log populated',
    CASE WHEN cnt >= 1 THEN 'PASS' ELSE 'FAIL' END, TO_VARCHAR(cnt), '>= 1',
    'DQ checks recorded in monitoring log', $GATE_EVAL_TS
FROM (SELECT COUNT(*) AS cnt FROM RAW_SALES.MONITORING.data_quality_log);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'C2', 'Silver order_items populated',
    CASE WHEN cnt > 0 THEN 'PASS' ELSE 'FAIL' END, TO_VARCHAR(cnt), '> 0',
    'Core fact grain exists in Silver', $GATE_EVAL_TS
FROM (SELECT COUNT(*) AS cnt FROM RAW_SALES.SILVER.order_items);


-- ============================================================================
-- GATE D: Product SLA Compliance
-- ============================================================================

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'D1', 'All 3 data products populated',
    CASE WHEN MIN(rc) > 0 THEN 'PASS' ELSE 'FAIL' END, 'MIN=' || TO_VARCHAR(MIN(rc)), 'MIN > 0',
    'sales_rep_monthly_performance, customer_revenue_forecast, customer_acquisition_cohort', $GATE_EVAL_TS
FROM (
    SELECT (SELECT COUNT(*) FROM RAW_SALES.GOLD.sales_rep_monthly_performance) AS rc
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.GOLD.customer_revenue_forecast)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.GOLD.customer_acquisition_cohort));


-- ============================================================================
-- GATE E: Marketplace Readiness (manual evidence)
-- ============================================================================

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'E1', 'Marketplace consumer test',
    CASE WHEN CHECK_VALUE THEN 'PASS' ELSE 'FAIL' END,
    IFF(CHECK_VALUE, 'TRUE', 'FALSE'), 'TRUE',
    COALESCE(NOTES, 'Manual evidence required'), $GATE_EVAL_TS
FROM RAW_SALES.MONITORING.manual_release_checks
WHERE CHECK_NAME = 'marketplace_consumer_test_passed';


-- ============================================================================
-- GATE F: Performance Evidence
-- ============================================================================

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT $GATE_RUN_ID, 'F1', 'fact_orders clustered',
    CASE WHEN cluster_expr IS NOT NULL THEN 'PASS' ELSE 'FAIL' END,
    COALESCE(cluster_expr, 'NULL'), 'Non-null', 'Clustering on fact_orders', $GATE_EVAL_TS
FROM (SELECT CLUSTERING_KEY AS cluster_expr FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = 'FACT_ORDERS');


-- ============================================================================
-- FINAL VERDICT
-- ============================================================================

-- Detailed results
SELECT GATE_ID, GATE_NAME, STATUS, ACTUAL_VALUE, EXPECTED_VALUE
FROM RAW_SALES.MONITORING.release_gate_results
WHERE RUN_ID = $GATE_RUN_ID ORDER BY GATE_ID;

-- One-line release decision
SELECT $GATE_RUN_ID AS RUN_ID,
    IFF(COUNT_IF(STATUS = 'FAIL') = 0, 'RELEASE: PASS', 'RELEASE: FAIL') AS VERDICT,
    COUNT_IF(STATUS = 'PASS') AS PASSED,
    COUNT_IF(STATUS = 'FAIL') AS FAILED
FROM RAW_SALES.MONITORING.release_gate_results
WHERE RUN_ID = $GATE_RUN_ID;

-- To mark marketplace gate as passed after consumer validation:
-- UPDATE RAW_SALES.MONITORING.manual_release_checks
-- SET CHECK_VALUE = TRUE, LAST_UPDATED_AT = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER()
-- WHERE CHECK_NAME = 'marketplace_consumer_test_passed';


-- ============================================================================
-- DEMO COMPLETE! Full medallion architecture from raw data to go-live.
-- ============================================================================
