-- DAMA 6 Data Quality Checks
-- Standalone checks aligned to RAW_SALES.SILVER and RAW_SALES.MONITORING objects.

USE DATABASE RAW_SALES;
USE SCHEMA MONITORING;
USE WAREHOUSE ANALYTICS_WH;

-- ============================================================================
-- Completeness
-- ============================================================================
SELECT 'customers.customer_id nulls' AS check_name,
       COUNT(*) AS failed_rows
FROM RAW_SALES.SILVER.customers
WHERE CUSTOMER_ID IS NULL
UNION ALL
SELECT 'orders.customer_id/order_id/date nulls',
       COUNT(*)
FROM RAW_SALES.SILVER.orders
WHERE ORDER_ID IS NULL OR CUSTOMER_ID IS NULL OR ORDER_DATE IS NULL
UNION ALL
SELECT 'order_items critical nulls',
       COUNT(*)
FROM RAW_SALES.SILVER.order_items
WHERE ORDER_ITEM_ID IS NULL OR ORDER_ID IS NULL OR PRODUCT_ID IS NULL;


-- ============================================================================
-- Uniqueness
-- ============================================================================
SELECT 'customers duplicate customer_id' AS check_name,
       COUNT(*) AS duplicate_keys
FROM (
    SELECT CUSTOMER_ID
    FROM RAW_SALES.SILVER.customers
    GROUP BY CUSTOMER_ID
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'orders duplicate order_id',
       COUNT(*)
FROM (
    SELECT ORDER_ID
    FROM RAW_SALES.SILVER.orders
    GROUP BY ORDER_ID
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'order_items duplicate order_item_id',
       COUNT(*)
FROM (
    SELECT ORDER_ITEM_ID
    FROM RAW_SALES.SILVER.order_items
    GROUP BY ORDER_ITEM_ID
    HAVING COUNT(*) > 1
);


-- ============================================================================
-- Timeliness
-- ============================================================================
SELECT 'orders future-dated rows' AS check_name,
       COUNT(*) AS failed_rows
FROM RAW_SALES.SILVER.orders
WHERE ORDER_DATE > CURRENT_DATE();


-- ============================================================================
-- Validity
-- ============================================================================
SELECT 'orders non-positive amount' AS check_name,
       COUNT(*) AS failed_rows
FROM RAW_SALES.SILVER.orders
WHERE ORDER_AMOUNT <= 0
UNION ALL
SELECT 'order_items non-positive qty/price/total',
       COUNT(*)
FROM RAW_SALES.SILVER.order_items
WHERE QUANTITY <= 0 OR UNIT_PRICE <= 0 OR LINE_TOTAL <= 0
UNION ALL
SELECT 'order_items line total math mismatch',
       COUNT(*)
FROM RAW_SALES.SILVER.order_items
WHERE ABS(QUANTITY * UNIT_PRICE * (1 - COALESCE(DISCOUNT_RATE, 0)) - LINE_TOTAL) >= 1.0;


-- ============================================================================
-- Accuracy (referential integrity)
-- ============================================================================
SELECT 'orders orphan customer_id' AS check_name,
       COUNT(*) AS failed_rows
FROM RAW_SALES.SILVER.orders O
LEFT JOIN RAW_SALES.SILVER.customers C
    ON O.CUSTOMER_ID = C.CUSTOMER_ID
WHERE C.CUSTOMER_ID IS NULL
UNION ALL
SELECT 'order_items orphan order_id',
       COUNT(*)
FROM RAW_SALES.SILVER.order_items OI
LEFT JOIN RAW_SALES.SILVER.orders O
    ON OI.ORDER_ID = O.ORDER_ID
WHERE O.ORDER_ID IS NULL
UNION ALL
SELECT 'order_items orphan product_id',
       COUNT(*)
FROM RAW_SALES.SILVER.order_items OI
LEFT JOIN RAW_SALES.SILVER.products P
    ON OI.PRODUCT_ID = P.PRODUCT_ID
WHERE P.PRODUCT_ID IS NULL;


-- ============================================================================
-- Consistency
-- ============================================================================
SELECT 'orders invalid normalized status' AS check_name,
       COUNT(*) AS failed_rows
FROM RAW_SALES.SILVER.orders
WHERE STATUS NOT IN ('Open', 'Fulfilled', 'Processing', 'Unknown')
UNION ALL
SELECT 'customers segment not upper-trimmed',
       COUNT(*)
FROM RAW_SALES.SILVER.customers
WHERE SEGMENT <> UPPER(TRIM(SEGMENT))
UNION ALL
SELECT 'territories region not upper-trimmed',
       COUNT(*)
FROM RAW_SALES.SILVER.territories
WHERE REGION <> UPPER(TRIM(REGION));


-- ============================================================================
-- Monitoring table summary
-- ============================================================================
SELECT
    TARGET_TABLE,
    DQ_DIMENSION,
    RECORDS_PASSED,
    RECORDS_FAILED,
    PASS_RATE_PCT,
    STATUS,
    LOG_TIMESTAMP
FROM RAW_SALES.MONITORING.data_quality_log
ORDER BY LOG_TIMESTAMP DESC;

SELECT
    REJECTION_REASON,
    COUNT(*) AS REJECTED_ROWS
FROM RAW_SALES.MONITORING.order_items_rejected
GROUP BY REJECTION_REASON
ORDER BY REJECTED_ROWS DESC;


-- ============================================================================
-- Platform Capability Checks (Mandatory Scope)
-- ============================================================================

-- Streams presence in Bronze
SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE;

-- Dynamic table presence and state in Silver
SHOW DYNAMIC TABLES IN SCHEMA RAW_SALES.SILVER;

-- Task presence and current state in Monitoring
SHOW TASKS IN SCHEMA RAW_SALES.MONITORING;

-- Snowpark telemetry
SELECT
    JOB_NAME,
    STATUS,
    MESSAGE,
    RUN_AT
FROM RAW_SALES.MONITORING.snowpark_job_runs
ORDER BY RUN_AT DESC;

-- Task audit telemetry
SELECT
    TASK_NAME,
    PRODUCT_NAME,
    RESULT_STATUS,
    EXECUTED_AT
FROM RAW_SALES.MONITORING.task_run_audit
ORDER BY EXECUTED_AT DESC;
