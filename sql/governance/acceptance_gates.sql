-- ============================================================================
-- RELEASE ACCEPTANCE GATES (STRICT PASS/FAIL)
-- Purpose: Evaluate go-live readiness in one place and persist gate outcomes.
-- Run after phase scripts: 01 -> 05.
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA MONITORING;
USE WAREHOUSE ANALYTICS_WH;

-- Manual evidence table for checks that cannot be inferred automatically.
CREATE TABLE IF NOT EXISTS RAW_SALES.MONITORING.manual_release_checks (
    CHECK_NAME        VARCHAR(200) PRIMARY KEY,
    CHECK_VALUE       BOOLEAN,
    LAST_UPDATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY        VARCHAR(255),
    NOTES             VARCHAR(1000)
);

-- Seed manual marketplace check if not present.
MERGE INTO RAW_SALES.MONITORING.manual_release_checks t
USING (
    SELECT
      'marketplace_consumer_test_passed' AS CHECK_NAME,
      FALSE AS CHECK_VALUE,
      CURRENT_TIMESTAMP() AS LAST_UPDATED_AT,
      CURRENT_USER() AS UPDATED_BY,
      'Set CHECK_VALUE=TRUE only after a real consumer account can discover, subscribe, and query.' AS NOTES
) s
ON t.CHECK_NAME = s.CHECK_NAME
WHEN NOT MATCHED THEN INSERT (CHECK_NAME, CHECK_VALUE, LAST_UPDATED_AT, UPDATED_BY, NOTES)
VALUES (s.CHECK_NAME, s.CHECK_VALUE, s.LAST_UPDATED_AT, s.UPDATED_BY, s.NOTES);

-- Result history table.
CREATE TABLE IF NOT EXISTS RAW_SALES.MONITORING.release_gate_results (
    RUN_ID            NUMBER,
    GATE_ID           VARCHAR(20),
    GATE_NAME         VARCHAR(200),
    STATUS            VARCHAR(10),      -- PASS | FAIL
    ACTUAL_VALUE      VARCHAR(500),
    EXPECTED_VALUE    VARCHAR(500),
    DETAILS           VARCHAR(2000),
    EVALUATED_AT      TIMESTAMP_NTZ
);

-- Run marker.
SET GATE_RUN_ID = (SELECT COALESCE(MAX(RUN_ID), 0) + 1 FROM RAW_SALES.MONITORING.release_gate_results);
SET GATE_EVAL_TS = CURRENT_TIMESTAMP();

-- ============================================================================
-- Gate A: Source and Mapping Consistency
-- ============================================================================
INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'A1',
    'LANDING required tables present',
    CASE WHEN cnt = 6 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(cnt),
    '6',
    'Required LANDING tables: CUSTOMERS, ORDERS, ORDER_ITEMS, PRODUCTS, SALES_REPS, TERRITORIES',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS cnt
    FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'LANDING'
      AND TABLE_NAME IN ('CUSTOMERS', 'ORDERS', 'ORDER_ITEMS', 'PRODUCTS', 'SALES_REPS', 'TERRITORIES')
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'A2',
    'LANDING row counts non-zero',
    CASE WHEN MIN(rc) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'MIN=' || TO_VARCHAR(MIN(rc)),
    'MIN > 0',
    'All core LANDING tables must contain data',
    $GATE_EVAL_TS
FROM (
    SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.CUSTOMERS) AS rc
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDERS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.PRODUCTS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.SALES_REPS)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.LANDING.TERRITORIES)
);

-- ============================================================================
-- Gate B: Platform Capability Health
-- ============================================================================
SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE;
SET BRONZE_STREAM_COUNT = (
    SELECT COUNT(*)
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'B1',
    'Bronze streams present',
    CASE WHEN $BRONZE_STREAM_COUNT >= 6 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR($BRONZE_STREAM_COUNT),
    '>= 6',
    'Expected one stream per Bronze raw table',
    $GATE_EVAL_TS;

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'B2',
    'Silver dynamic tables present',
    CASE WHEN cnt >= 3 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(cnt),
    '>= 3',
    'Expected dynamic projections for customers/orders/order_items',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS cnt
    FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SILVER'
      AND IS_DYNAMIC = 'YES'
);

SHOW TASKS IN SCHEMA RAW_SALES.MONITORING;
SET MONITORING_TASK_COUNT = (
    SELECT COUNT(*)
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'B3',
    'Monitoring tasks present',
    CASE WHEN $MONITORING_TASK_COUNT >= 3 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR($MONITORING_TASK_COUNT),
    '>= 3',
    'Expected daily/weekly/monthly validation tasks',
    $GATE_EVAL_TS;

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'B4',
    'Snowpark telemetry present',
    CASE WHEN cnt >= 1 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(cnt),
    '>= 1',
    'Expected at least one row in snowpark_job_runs',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS cnt
    FROM RAW_SALES.MONITORING.snowpark_job_runs
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'B5',
    'Monitoring task success rate (7d)',
    CASE
        WHEN total_runs = 0 THEN 'FAIL'
        WHEN success_rate_pct >= 95 THEN 'PASS'
        ELSE 'FAIL'
    END,
    TO_VARCHAR(success_rate_pct),
    '>= 95',
    'Success rate over last 7 days from MONITORING.task_run_audit',
    $GATE_EVAL_TS
FROM (
    SELECT
        COUNT(*) AS total_runs,
        ROUND(100.0 * COUNT_IF(RESULT_STATUS = 'PASS') / NULLIF(COUNT(*), 0), 2) AS success_rate_pct
    FROM RAW_SALES.MONITORING.task_run_audit
    WHERE EXECUTED_AT >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
);

-- ============================================================================
-- Gate C: Data Quality and Trust
-- ============================================================================
INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'C1',
    'DQ log populated',
    CASE WHEN cnt >= 1 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(cnt),
    '>= 1',
    'Expected DQ check records in monitoring log',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS cnt
    FROM RAW_SALES.MONITORING.data_quality_log
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'C2',
    'Silver order_items populated',
    CASE WHEN cnt > 0 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(cnt),
    '> 0',
    'Core trusted fact grain must exist in Silver',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS cnt
    FROM RAW_SALES.SILVER.order_items
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'C3',
    'AI-ready artifacts present',
    CASE WHEN cnt = 3 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(cnt),
    '3',
    'Expected: GOLD.ai_semantic_metadata, GOLD.ai_retrieval_index_stub, GOLD.ai_rag_query_path_stub',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT TABLE_NAME AS OBJECT_NAME
        FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'GOLD'
          AND TABLE_NAME IN ('AI_SEMANTIC_METADATA', 'AI_RETRIEVAL_INDEX_STUB')
        UNION ALL
        SELECT TABLE_NAME AS OBJECT_NAME
        FROM RAW_SALES.INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = 'GOLD'
          AND TABLE_NAME IN ('AI_RAG_QUERY_PATH_STUB')
    )
);

-- ============================================================================
-- Gate D: Product SLA Compliance
-- ============================================================================
INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'D1',
    'SLA status all PASS',
    CASE WHEN fail_cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(fail_cnt),
    '0',
    'Rows in product_sla_status where SLA_STATUS <> PASS',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS fail_cnt
    FROM RAW_SALES.MONITORING.product_sla_status
    WHERE SLA_STATUS <> 'PASS'
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'D2',
    'All three data products populated',
    CASE WHEN MIN(rc) > 0 THEN 'PASS' ELSE 'FAIL' END,
    'MIN=' || TO_VARCHAR(MIN(rc)),
    'MIN > 0',
    'sales_rep_monthly_performance, customer_revenue_forecast, customer_acquisition_cohort',
    $GATE_EVAL_TS
FROM (
    SELECT (SELECT COUNT(*) FROM RAW_SALES.GOLD.sales_rep_monthly_performance) AS rc
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.GOLD.customer_revenue_forecast)
    UNION ALL SELECT (SELECT COUNT(*) FROM RAW_SALES.GOLD.customer_acquisition_cohort)
);

-- ============================================================================
-- Gate E: Marketplace Readiness (manual evidence)
-- ============================================================================
INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'E1',
    'Marketplace consumer test evidence',
    CASE WHEN CHECK_VALUE THEN 'PASS' ELSE 'FAIL' END,
    IFF(CHECK_VALUE, 'TRUE', 'FALSE'),
    'TRUE',
    COALESCE(NOTES, 'Manual evidence required'),
    $GATE_EVAL_TS
FROM RAW_SALES.MONITORING.manual_release_checks
WHERE CHECK_NAME = 'marketplace_consumer_test_passed';

-- ============================================================================
-- Gate F: Performance and Cost Evidence
-- ============================================================================
INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'F1',
    'fact_orders clustered',
    CASE WHEN cluster_expr IS NOT NULL THEN 'PASS' ELSE 'FAIL' END,
    COALESCE(cluster_expr, 'NULL'),
    'Non-null clustering key',
    'Checks clustering metadata for GOLD.fact_orders',
    $GATE_EVAL_TS
FROM (
    SELECT CLUSTERING_KEY AS cluster_expr
    FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'GOLD'
      AND TABLE_NAME = 'FACT_ORDERS'
);

INSERT INTO RAW_SALES.MONITORING.release_gate_results
SELECT
    $GATE_RUN_ID,
    'F2',
    'Task audit telemetry present',
    CASE WHEN cnt >= 1 THEN 'PASS' ELSE 'FAIL' END,
    TO_VARCHAR(cnt),
    '>= 1',
    'Expected at least one row in task_run_audit after validation runs',
    $GATE_EVAL_TS
FROM (
    SELECT COUNT(*) AS cnt
    FROM RAW_SALES.MONITORING.task_run_audit
);

-- ============================================================================
-- Final output
-- ============================================================================

-- Detailed gate results for this run
SELECT *
FROM RAW_SALES.MONITORING.release_gate_results
WHERE RUN_ID = $GATE_RUN_ID
ORDER BY GATE_ID;

-- One-line release decision
SELECT
    $GATE_RUN_ID AS RUN_ID,
    IFF(COUNT_IF(STATUS = 'FAIL') = 0, 'PASS', 'FAIL') AS RELEASE_STATUS,
    COUNT_IF(STATUS = 'PASS') AS PASS_COUNT,
    COUNT_IF(STATUS = 'FAIL') AS FAIL_COUNT,
    MIN(EVALUATED_AT) AS EVALUATED_AT
FROM RAW_SALES.MONITORING.release_gate_results
WHERE RUN_ID = $GATE_RUN_ID;

-- Update manual marketplace gate when evidence is complete:
-- UPDATE RAW_SALES.MONITORING.manual_release_checks
-- SET CHECK_VALUE = TRUE, LAST_UPDATED_AT = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER(),
--     NOTES = 'Validated from separate consumer account on 2026-03-30'
-- WHERE CHECK_NAME = 'marketplace_consumer_test_passed';
