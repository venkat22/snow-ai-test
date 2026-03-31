-- ============================================================================
-- DEMO STEP 3: BRONZE LAYER — Full-Fidelity Copy + Audit Metadata
-- ============================================================================
-- What this does:
--   Copies all 6 Landing tables to Bronze, adding 3 audit columns:
--   _LOADED_AT, _SOURCE_TABLE, _ROW_NUMBER.
--   Creates append-only STREAMS for change data capture (CDC).
--
-- Talk track:
--   "Bronze is our immutable audit layer. Every row gets a load timestamp,
--    source tracking, and row number. We also create Snowflake Streams on
--    each table — these track inserts for incremental downstream processing,
--    which is how the Silver layer knows what's new."
--
-- Runtime: ~3 minutes on XS warehouse
-- Prerequisites: Step 02 completed
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA BRONZE;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- BRONZE TABLES (full copy + metadata columns)
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.BRONZE.customers_raw AS
SELECT *, CURRENT_TIMESTAMP() AS _LOADED_AT, 'LANDING.CUSTOMERS' AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY CUSTOMER_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.CUSTOMERS;

CREATE OR REPLACE TABLE RAW_SALES.BRONZE.orders_raw AS
SELECT *, CURRENT_TIMESTAMP() AS _LOADED_AT, 'LANDING.ORDERS' AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY ORDER_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.ORDERS;

CREATE OR REPLACE TABLE RAW_SALES.BRONZE.order_items_raw AS
SELECT *, CURRENT_TIMESTAMP() AS _LOADED_AT, 'LANDING.ORDER_ITEMS' AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY ORDER_ID, LINE_NUMBER) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.ORDER_ITEMS;

CREATE OR REPLACE TABLE RAW_SALES.BRONZE.products_raw AS
SELECT *, CURRENT_TIMESTAMP() AS _LOADED_AT, 'LANDING.PRODUCTS' AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY PRODUCT_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.PRODUCTS;

CREATE OR REPLACE TABLE RAW_SALES.BRONZE.sales_reps_raw AS
SELECT *, CURRENT_TIMESTAMP() AS _LOADED_AT, 'LANDING.SALES_REPS' AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY REP_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.SALES_REPS;

CREATE OR REPLACE TABLE RAW_SALES.BRONZE.territories_raw AS
SELECT *, CURRENT_TIMESTAMP() AS _LOADED_AT, 'LANDING.TERRITORIES' AS _SOURCE_TABLE,
    ROW_NUMBER() OVER (ORDER BY TERRITORY_ID) AS _ROW_NUMBER
FROM RAW_SALES.LANDING.TERRITORIES;


-- ============================================================================
-- STREAMS (Change Data Capture for incremental processing)
-- ============================================================================

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.customers_raw_stream
    ON TABLE RAW_SALES.BRONZE.customers_raw APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.orders_raw_stream
    ON TABLE RAW_SALES.BRONZE.orders_raw APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.order_items_raw_stream
    ON TABLE RAW_SALES.BRONZE.order_items_raw APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.products_raw_stream
    ON TABLE RAW_SALES.BRONZE.products_raw APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.sales_reps_raw_stream
    ON TABLE RAW_SALES.BRONZE.sales_reps_raw APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW_SALES.BRONZE.territories_raw_stream
    ON TABLE RAW_SALES.BRONZE.territories_raw APPEND_ONLY = TRUE;


-- ============================================================================
-- VALIDATION: Row counts must match Landing exactly (0% data loss)
-- ============================================================================

SELECT
    'LANDING' AS LAYER, 'CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT
FROM RAW_SALES.LANDING.CUSTOMERS
UNION ALL SELECT 'BRONZE', 'CUSTOMERS',  COUNT(*) FROM RAW_SALES.BRONZE.customers_raw
UNION ALL SELECT 'LANDING', 'ORDERS',    COUNT(*) FROM RAW_SALES.LANDING.ORDERS
UNION ALL SELECT 'BRONZE',  'ORDERS',    COUNT(*) FROM RAW_SALES.BRONZE.orders_raw
UNION ALL SELECT 'LANDING', 'ORDER_ITEMS', COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS
UNION ALL SELECT 'BRONZE',  'ORDER_ITEMS', COUNT(*) FROM RAW_SALES.BRONZE.order_items_raw
ORDER BY TABLE_NAME, LAYER;

-- Verify streams exist
SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE;

-- Verify metadata columns
SELECT _LOADED_AT, _SOURCE_TABLE, COUNT(*) AS ROWS
FROM RAW_SALES.BRONZE.orders_raw
GROUP BY 1, 2;


-- ============================================================================
-- CHECKPOINT: All 6 Bronze tables match Landing row counts. 6 streams created.
-- NEXT: Run 04_silver_layer_dama6_quality.sql
-- ============================================================================
