-- ============================================================================
-- DEMO STEP 8: DAMA 6 DATA QUALITY CHECKS (Standalone)
-- ============================================================================
-- What this does:
--   Runs all 6 DAMA data quality dimensions as standalone checks against
--   Silver tables. Use this anytime to audit data trust.
--
-- Talk track:
--   "This is our quality audit script. It checks all 6 DAMA dimensions in
--    one pass — completeness, uniqueness, timeliness, validity, accuracy,
--    and consistency. Every check should return 0 failed rows. If any fail,
--    we trace back to the exact Bronze row that caused it."
--
-- Runtime: ~1-2 minutes
-- Prerequisites: Step 04 completed (Silver layer)
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA MONITORING;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- 1. COMPLETENESS: No NULLs in critical columns
-- ============================================================================

SELECT 'customers.CUSTOMER_ID nulls' AS CHECK_NAME, COUNT(*) AS FAILED_ROWS
FROM RAW_SALES.SILVER.customers WHERE CUSTOMER_ID IS NULL
UNION ALL
SELECT 'orders.ORDER_ID/CUSTOMER_ID/DATE nulls', COUNT(*)
FROM RAW_SALES.SILVER.orders WHERE ORDER_ID IS NULL OR CUSTOMER_ID IS NULL OR ORDER_DATE IS NULL
UNION ALL
SELECT 'order_items.critical nulls', COUNT(*)
FROM RAW_SALES.SILVER.order_items WHERE ORDER_ITEM_ID IS NULL OR ORDER_ID IS NULL OR PRODUCT_ID IS NULL;

-- Expected: All 0


-- ============================================================================
-- 2. UNIQUENESS: No duplicate primary keys
-- ============================================================================

SELECT 'customers duplicate CUSTOMER_ID' AS CHECK_NAME, COUNT(*) AS DUPLICATES
FROM (SELECT CUSTOMER_ID FROM RAW_SALES.SILVER.customers GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1)
UNION ALL
SELECT 'orders duplicate ORDER_ID', COUNT(*)
FROM (SELECT ORDER_ID FROM RAW_SALES.SILVER.orders GROUP BY ORDER_ID HAVING COUNT(*) > 1)
UNION ALL
SELECT 'order_items duplicate ORDER_ITEM_ID', COUNT(*)
FROM (SELECT ORDER_ITEM_ID FROM RAW_SALES.SILVER.order_items GROUP BY ORDER_ITEM_ID HAVING COUNT(*) > 1);

-- Expected: All 0


-- ============================================================================
-- 3. TIMELINESS: No future-dated records
-- ============================================================================

SELECT 'orders future-dated rows' AS CHECK_NAME, COUNT(*) AS FAILED_ROWS
FROM RAW_SALES.SILVER.orders WHERE ORDER_DATE > CURRENT_DATE();

-- Expected: 0


-- ============================================================================
-- 4. VALIDITY: Business rule enforcement
-- ============================================================================

SELECT 'orders non-positive amount' AS CHECK_NAME, COUNT(*) AS FAILED_ROWS
FROM RAW_SALES.SILVER.orders WHERE ORDER_AMOUNT <= 0
UNION ALL
SELECT 'order_items non-positive qty/price/total', COUNT(*)
FROM RAW_SALES.SILVER.order_items WHERE QUANTITY <= 0 OR UNIT_PRICE <= 0 OR LINE_TOTAL <= 0
UNION ALL
SELECT 'order_items line total math mismatch', COUNT(*)
FROM RAW_SALES.SILVER.order_items
WHERE ABS(QUANTITY * UNIT_PRICE * (1 - COALESCE(DISCOUNT_RATE, 0)) - LINE_TOTAL) >= 1.0;

-- Expected: All 0


-- ============================================================================
-- 5. ACCURACY: Referential integrity (no orphans)
-- ============================================================================

SELECT 'orders orphan CUSTOMER_ID' AS CHECK_NAME, COUNT(*) AS FAILED_ROWS
FROM RAW_SALES.SILVER.orders O
LEFT JOIN RAW_SALES.SILVER.customers C ON O.CUSTOMER_ID = C.CUSTOMER_ID
WHERE C.CUSTOMER_ID IS NULL
UNION ALL
SELECT 'order_items orphan ORDER_ID', COUNT(*)
FROM RAW_SALES.SILVER.order_items OI
LEFT JOIN RAW_SALES.SILVER.orders O ON OI.ORDER_ID = O.ORDER_ID
WHERE O.ORDER_ID IS NULL
UNION ALL
SELECT 'order_items orphan PRODUCT_ID', COUNT(*)
FROM RAW_SALES.SILVER.order_items OI
LEFT JOIN RAW_SALES.SILVER.products P ON OI.PRODUCT_ID = P.PRODUCT_ID
WHERE P.PRODUCT_ID IS NULL;

-- Expected: All 0


-- ============================================================================
-- 6. CONSISTENCY: Standardized enums and formatting
-- ============================================================================

SELECT 'orders invalid STATUS enum' AS CHECK_NAME, COUNT(*) AS FAILED_ROWS
FROM RAW_SALES.SILVER.orders WHERE STATUS NOT IN ('Open', 'Fulfilled', 'Processing', 'Unknown')
UNION ALL
SELECT 'customers SEGMENT not upper-trimmed', COUNT(*)
FROM RAW_SALES.SILVER.customers WHERE SEGMENT <> UPPER(TRIM(SEGMENT))
UNION ALL
SELECT 'territories REGION not upper-trimmed', COUNT(*)
FROM RAW_SALES.SILVER.territories WHERE REGION <> UPPER(TRIM(REGION));

-- Expected: All 0


-- ============================================================================
-- BONUS: DQ log and rejection audit
-- ============================================================================

SELECT TARGET_TABLE, DQ_DIMENSION, PASS_RATE_PCT, STATUS
FROM RAW_SALES.MONITORING.data_quality_log ORDER BY LOG_TIMESTAMP DESC;

SELECT REJECTION_REASON, COUNT(*) AS REJECTED_ROWS
FROM RAW_SALES.MONITORING.order_items_rejected GROUP BY REJECTION_REASON ORDER BY REJECTED_ROWS DESC;


-- ============================================================================
-- BONUS: Platform capability health
-- ============================================================================

SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE;
SHOW DYNAMIC TABLES IN SCHEMA RAW_SALES.SILVER;
SHOW TASKS IN SCHEMA RAW_SALES.MONITORING;


-- ============================================================================
-- CHECKPOINT: All 6 DAMA dimensions pass with 0 failures.
-- NEXT: Run 09_bi_analytics_queries.sql
-- ============================================================================
