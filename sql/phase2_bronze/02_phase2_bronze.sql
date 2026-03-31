-- ============================================================================
-- PHASE 2A: BRONZE LAYER
-- Purpose: Copy LANDING data → BRONZE with metadata columns added
-- Prerequisite: 01_phase1_foundation.sql completed successfully
-- Estimated runtime: ~3 minutes on XS warehouse (6M+ rows)
-- ============================================================================

-- If unsure of your warehouse name, run: SHOW WAREHOUSES;
USE DATABASE RAW_SALES;
USE SCHEMA BRONZE;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- BRONZE LAYER TABLES
-- Changes from LANDING:
--   + _LOADED_AT   : timestamp this row was ingested into Bronze
--   + _SOURCE_TABLE: which LANDING table the row came from
--   + _ROW_NUMBER  : original row order for debugging/tracing
-- No data is filtered or modified at this layer — full fidelity copy
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.BRONZE.customers_raw AS
SELECT
    CUSTOMER_ID,
    NAME,
    ADDRESS,
    NATION_KEY,
    PHONE,
    ACCOUNT_BALANCE,
    SEGMENT,
    NOTES,
    CURRENT_TIMESTAMP()         AS _LOADED_AT,
    'LANDING.CUSTOMERS'         AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY CUSTOMER_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.CUSTOMERS;


CREATE OR REPLACE TABLE RAW_SALES.BRONZE.orders_raw AS
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    STATUS,
    ORDER_AMOUNT,
    ORDER_DATE,
    PRIORITY,
    SALES_CLERK,
    SHIP_PRIORITY,
    NOTES,
    CURRENT_TIMESTAMP()         AS _LOADED_AT,
    'LANDING.ORDERS'            AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY ORDER_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.ORDERS;


CREATE OR REPLACE TABLE RAW_SALES.BRONZE.order_items_raw AS
SELECT
    ORDER_ITEM_ID,
    ORDER_ID,
    PRODUCT_ID,
    SUPPLIER_ID,
    LINE_NUMBER,
    QUANTITY,
    UNIT_PRICE,
    LINE_TOTAL,
    DISCOUNT_RATE,
    RETURN_FLAG,
    LINE_STATUS,
    SHIP_DATE,
    COMMIT_DATE,
    RECEIPT_DATE,
    SHIP_MODE,
    CURRENT_TIMESTAMP()         AS _LOADED_AT,
    'LANDING.ORDER_ITEMS'       AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY ORDER_ID, LINE_NUMBER) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.ORDER_ITEMS;


CREATE OR REPLACE TABLE RAW_SALES.BRONZE.products_raw AS
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    MANUFACTURER,
    BRAND,
    CATEGORY,
    FULL_TYPE,
    PRODUCT_SIZE,
    CONTAINER_TYPE,
    UNIT_PRICE,
    NOTES,
    CURRENT_TIMESTAMP()         AS _LOADED_AT,
    'LANDING.PRODUCTS'          AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY PRODUCT_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.PRODUCTS;


CREATE OR REPLACE TABLE RAW_SALES.BRONZE.sales_reps_raw AS
SELECT
    REP_ID,
    NAME,
    ADDRESS,
    NATION_KEY,
    PHONE,
    ACCOUNT_BALANCE,
    QUOTA,
    STATUS,
    NOTES,
    CURRENT_TIMESTAMP()         AS _LOADED_AT,
    'LANDING.SALES_REPS'        AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY REP_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.SALES_REPS;


CREATE OR REPLACE TABLE RAW_SALES.BRONZE.territories_raw AS
SELECT
    TERRITORY_ID,
    TERRITORY_NAME,
    REGION_ID,
    REGION,
    MANAGER_ID,
    CURRENT_TIMESTAMP()         AS _LOADED_AT,
    'LANDING.TERRITORIES'       AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY TERRITORY_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.TERRITORIES;


-- ============================================================================
-- PLATFORM CAPABILITY: STREAMS (CDC)
-- Append-only streams track new rows for downstream incremental processing.
-- ============================================================================

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.customers_raw_stream
    ON TABLE RAW_SALES.BRONZE.customers_raw
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.orders_raw_stream
    ON TABLE RAW_SALES.BRONZE.orders_raw
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.order_items_raw_stream
    ON TABLE RAW_SALES.BRONZE.order_items_raw
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.products_raw_stream
    ON TABLE RAW_SALES.BRONZE.products_raw
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.sales_reps_raw_stream
    ON TABLE RAW_SALES.BRONZE.sales_reps_raw
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.territories_raw_stream
    ON TABLE RAW_SALES.BRONZE.territories_raw
    APPEND_ONLY = TRUE;


-- ============================================================================
-- BRONZE VERIFICATION: Row counts must match LANDING exactly
-- ============================================================================

SELECT
    'LANDING'   AS LAYER,
    'CUSTOMERS' AS TABLE_NAME,
    COUNT(*)    AS ROW_COUNT
FROM RAW_SALES.LANDING.CUSTOMERS
UNION ALL SELECT 'BRONZE', 'CUSTOMERS',  COUNT(*) FROM RAW_SALES.BRONZE.customers_raw
UNION ALL SELECT 'LANDING', 'ORDERS',    COUNT(*) FROM RAW_SALES.LANDING.ORDERS
UNION ALL SELECT 'BRONZE',  'ORDERS',    COUNT(*) FROM RAW_SALES.BRONZE.orders_raw
UNION ALL SELECT 'LANDING', 'ORDER_ITEMS', COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS
UNION ALL SELECT 'BRONZE',  'ORDER_ITEMS', COUNT(*) FROM RAW_SALES.BRONZE.order_items_raw
UNION ALL SELECT 'LANDING', 'PRODUCTS',  COUNT(*) FROM RAW_SALES.LANDING.PRODUCTS
UNION ALL SELECT 'BRONZE',  'PRODUCTS',  COUNT(*) FROM RAW_SALES.BRONZE.products_raw
UNION ALL SELECT 'LANDING', 'SALES_REPS', COUNT(*) FROM RAW_SALES.LANDING.SALES_REPS
UNION ALL SELECT 'BRONZE',  'SALES_REPS', COUNT(*) FROM RAW_SALES.BRONZE.sales_reps_raw
UNION ALL SELECT 'LANDING', 'TERRITORIES', COUNT(*) FROM RAW_SALES.LANDING.TERRITORIES
UNION ALL SELECT 'BRONZE',  'TERRITORIES', COUNT(*) FROM RAW_SALES.BRONZE.territories_raw
ORDER BY TABLE_NAME, LAYER;

-- ✓ Each LANDING row count must equal its BRONZE counterpart exactly (0% loss)


-- Verify metadata columns populated on a sample
SELECT _LOADED_AT, _SOURCE_TABLE, COUNT(*) AS ROWS
FROM RAW_SALES.BRONZE.orders_raw
GROUP BY 1, 2;

-- Verify stream objects exist (required platform capability)
SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE;

-- ============================================================================
-- BRONZE LAYER COMPLETE ✓
-- NEXT STEP: Run 03_phase2_silver.sql
-- ============================================================================
