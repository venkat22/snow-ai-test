-- ============================================================================
-- DEMO STEP 2: INGEST LANDING DATA FROM TPC-H
-- ============================================================================
-- What this does:
--   Loads 6 tables into LANDING from Snowflake's built-in TPC-H sample data.
--   Renames columns to business-friendly names (no CSV upload needed).
--
-- Talk track:
--   "We ingest ~7.8M rows from TPC-H into semantically named landing tables.
--    This is our raw source of truth — no transformations yet, just clean
--    column aliases so downstream layers have consistent naming."
--
-- Runtime: ~1-2 minutes on XS warehouse
-- Prerequisites: Step 01 completed
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA LANDING;
USE WAREHOUSE ANALYTICS_WH;


-- Verify TPC-H sample data is accessible
SELECT COUNT(*) AS CUSTOMER_COUNT FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;   -- ~150,000
SELECT COUNT(*) AS ORDER_COUNT    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;     -- ~1,500,000


-- CUSTOMERS (~150K rows)
CREATE OR REPLACE TABLE RAW_SALES.LANDING.CUSTOMERS AS
SELECT
    C_CUSTKEY       AS CUSTOMER_ID,
    C_NAME          AS NAME,
    C_ADDRESS       AS ADDRESS,
    C_NATIONKEY     AS NATION_KEY,
    C_PHONE         AS PHONE,
    C_ACCTBAL       AS ACCOUNT_BALANCE,
    C_MKTSEGMENT    AS SEGMENT,
    C_COMMENT       AS NOTES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;


-- ORDERS (~1.5M rows)
CREATE OR REPLACE TABLE RAW_SALES.LANDING.ORDERS AS
SELECT
    O_ORDERKEY      AS ORDER_ID,
    O_CUSTKEY       AS CUSTOMER_ID,
    O_ORDERSTATUS   AS STATUS,
    O_TOTALPRICE    AS ORDER_AMOUNT,
    O_ORDERDATE     AS ORDER_DATE,
    O_ORDERPRIORITY AS PRIORITY,
    O_CLERK         AS SALES_CLERK,
    O_SHIPPRIORITY  AS SHIP_PRIORITY,
    O_COMMENT       AS NOTES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;


-- ORDER_ITEMS (~6M rows)
CREATE OR REPLACE TABLE RAW_SALES.LANDING.ORDER_ITEMS AS
SELECT
    L_ORDERKEY                                              AS ORDER_ID,
    L_PARTKEY                                               AS PRODUCT_ID,
    L_SUPPKEY                                               AS SUPPLIER_ID,
    L_LINENUMBER                                            AS LINE_NUMBER,
    CONCAT(L_ORDERKEY, '-', L_LINENUMBER)                   AS ORDER_ITEM_ID,
    L_QUANTITY                                              AS QUANTITY,
    L_EXTENDEDPRICE / NULLIF(L_QUANTITY, 0)                 AS UNIT_PRICE,
    ROUND(L_EXTENDEDPRICE * (1 - L_DISCOUNT), 2)            AS LINE_TOTAL,
    L_DISCOUNT                                              AS DISCOUNT_RATE,
    L_RETURNFLAG                                            AS RETURN_FLAG,
    L_LINESTATUS                                            AS LINE_STATUS,
    L_SHIPDATE                                              AS SHIP_DATE,
    L_COMMITDATE                                            AS COMMIT_DATE,
    L_RECEIPTDATE                                           AS RECEIPT_DATE,
    L_SHIPMODE                                              AS SHIP_MODE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;


-- PRODUCTS (~200K rows)
CREATE OR REPLACE TABLE RAW_SALES.LANDING.PRODUCTS AS
SELECT
    P_PARTKEY       AS PRODUCT_ID,
    P_NAME          AS PRODUCT_NAME,
    P_MFGR          AS MANUFACTURER,
    P_BRAND         AS BRAND,
    SPLIT_PART(P_TYPE, ' ', 3)  AS CATEGORY,
    P_TYPE          AS FULL_TYPE,
    P_SIZE          AS PRODUCT_SIZE,
    P_CONTAINER     AS CONTAINER_TYPE,
    P_RETAILPRICE   AS UNIT_PRICE,
    P_COMMENT       AS NOTES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;


-- SALES_REPS (~10K rows, mapped from Suppliers)
CREATE OR REPLACE TABLE RAW_SALES.LANDING.SALES_REPS AS
SELECT
    S_SUPPKEY       AS REP_ID,
    S_NAME          AS NAME,
    S_ADDRESS       AS ADDRESS,
    S_NATIONKEY     AS NATION_KEY,
    S_PHONE         AS PHONE,
    S_ACCTBAL       AS ACCOUNT_BALANCE,
    ROUND(ABS(S_ACCTBAL) * 100, 2)  AS QUOTA,
    'Active'        AS STATUS,
    S_COMMENT       AS NOTES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;


-- TERRITORIES (25 rows, Nations + Regions)
CREATE OR REPLACE TABLE RAW_SALES.LANDING.TERRITORIES AS
SELECT
    N.N_NATIONKEY       AS TERRITORY_ID,
    N.N_NAME            AS TERRITORY_NAME,
    R.R_REGIONKEY       AS REGION_ID,
    R.R_NAME            AS REGION,
    NULL::STRING        AS MANAGER_ID
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION R
    ON N.N_REGIONKEY = R.R_REGIONKEY;


-- ============================================================================
-- VALIDATION: Confirm all 6 tables loaded
-- ============================================================================

SELECT 'CUSTOMERS'   AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW_SALES.LANDING.CUSTOMERS   UNION ALL
SELECT 'ORDERS',               COUNT(*) FROM RAW_SALES.LANDING.ORDERS                        UNION ALL
SELECT 'ORDER_ITEMS',          COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS                   UNION ALL
SELECT 'PRODUCTS',             COUNT(*) FROM RAW_SALES.LANDING.PRODUCTS                      UNION ALL
SELECT 'SALES_REPS',           COUNT(*) FROM RAW_SALES.LANDING.SALES_REPS                    UNION ALL
SELECT 'TERRITORIES',          COUNT(*) FROM RAW_SALES.LANDING.TERRITORIES
ORDER BY TABLE_NAME;

-- Quick data profile
SELECT
    MIN(ORDER_DATE) AS EARLIEST_ORDER,
    MAX(ORDER_DATE) AS LATEST_ORDER,
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    ROUND(SUM(ORDER_AMOUNT), 2) AS TOTAL_REVENUE
FROM RAW_SALES.LANDING.ORDERS;

-- Preview sample rows
SELECT * FROM RAW_SALES.LANDING.CUSTOMERS LIMIT 5;
SELECT * FROM RAW_SALES.LANDING.ORDERS LIMIT 5;


-- ============================================================================
-- CHECKPOINT: ~7.86M total rows across 6 tables. Date range: 1992-1998.
-- NEXT: Run 03_bronze_layer_with_metadata.sql
-- ============================================================================
