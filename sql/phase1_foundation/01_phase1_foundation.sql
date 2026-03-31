-- ============================================================================
-- PHASE 1: SNOWFLAKE FOUNDATION
-- Data Source: Snowflake built-in TPC-H sample data (no CSV upload needed)
-- Estimated runtime: < 2 minutes on XS warehouse
-- Run this entire script top-to-bottom in a single Snowflake Worksheet
-- ============================================================================

-- ============================================================================
-- STEP 0: CREATE WAREHOUSE (New account — run this block first)
-- ============================================================================

-- Create a dedicated XS warehouse for this project
-- XS = 1 credit/hour; auto-suspends after 60s idle to save credits
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE    = 'X-SMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT           = 'Warehouse for RAW_SALES medallion architecture project';

-- Verify warehouse created
SHOW WAREHOUSES LIKE 'ANALYTICS_WH';


-- ============================================================================
-- STEP 1: CREATE DATABASE & SCHEMAS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS RAW_SALES;

CREATE SCHEMA IF NOT EXISTS RAW_SALES.LANDING;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.BRONZE;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.SILVER;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.GOLD;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.MONITORING;

-- Verify all schemas were created
SHOW SCHEMAS IN DATABASE RAW_SALES;


-- ============================================================================
-- STEP 2: SET SESSION CONTEXT
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA LANDING;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- STEP 3: VERIFY TPC-H SAMPLE DATA ACCESS
-- ============================================================================

-- Confirm TPC-H sample data exists (should return rows immediately)
SELECT COUNT(*) AS CUSTOMER_COUNT   FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;   -- ~150,000 rows
SELECT COUNT(*) AS ORDER_COUNT      FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;     -- ~1,500,000 rows
SELECT COUNT(*) AS LINEITEM_COUNT   FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;   -- ~6,000,000 rows
SELECT COUNT(*) AS PRODUCT_COUNT    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;       -- ~200,000 rows
SELECT COUNT(*) AS SUPPLIER_COUNT   FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;   -- ~10,000 rows
SELECT COUNT(*) AS TERRITORY_COUNT  FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;     -- 25 rows
SELECT COUNT(*) AS REGION_COUNT     FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;     -- 5 rows

-- If any of these fail, run:
--   CREATE DATABASE IF NOT EXISTS SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;


-- ============================================================================
-- STEP 4: CREATE LANDING TABLES (Column-renamed from TPC-H)
-- Normalizing column names to our standard schema here so all downstream
-- Bronze/Silver/Gold layers use consistent, readable column names.
-- Using TPCH_SF1 = 1GB scale factor (good balance of size & speed)
-- ============================================================================

-- Table: CUSTOMERS
-- Source: TPCH_SF1.CUSTOMER (~150,000 rows)
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


-- Table: ORDERS
-- Source: TPCH_SF1.ORDERS (~1,500,000 rows)
-- O_CLERK maps to a sales rep identifier (TPC-H uses clerk names like 'Clerk#000000001')
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


-- Table: ORDER_ITEMS (Lineitems in TPC-H)
-- Source: TPCH_SF1.LINEITEM (~6,000,000 rows)
-- LINE_TOTAL = extended price after discount (most representative revenue figure)
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


-- Table: PRODUCTS (Parts in TPC-H)
-- Source: TPCH_SF1.PART (~200,000 rows)
-- P_TYPE is used as CATEGORY (e.g. 'ECONOMY ANODIZED STEEL')
CREATE OR REPLACE TABLE RAW_SALES.LANDING.PRODUCTS AS
SELECT
    P_PARTKEY       AS PRODUCT_ID,
    P_NAME          AS PRODUCT_NAME,
    P_MFGR          AS MANUFACTURER,
    P_BRAND         AS BRAND,
    SPLIT_PART(P_TYPE, ' ', 3)  AS CATEGORY,      -- Last word of TYPE (e.g. STEEL, COPPER)
    P_TYPE          AS FULL_TYPE,
    P_SIZE          AS PRODUCT_SIZE,
    P_CONTAINER     AS CONTAINER_TYPE,
    P_RETAILPRICE   AS UNIT_PRICE,
    P_COMMENT       AS NOTES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;


-- Table: SUPPLIERS mapped to SALES_REPS
-- Source: TPCH_SF1.SUPPLIER (~10,000 rows)
-- QUOTA: use ACCTBAL as a proxy for quota (scaled to realistic values)
CREATE OR REPLACE TABLE RAW_SALES.LANDING.SALES_REPS AS
SELECT
    S_SUPPKEY       AS REP_ID,
    S_NAME          AS NAME,
    S_ADDRESS       AS ADDRESS,
    S_NATIONKEY     AS NATION_KEY,
    S_PHONE         AS PHONE,
    S_ACCTBAL       AS ACCOUNT_BALANCE,
    ROUND(ABS(S_ACCTBAL) * 100, 2)  AS QUOTA,          -- Scale acctbal to quota proxy
    'Active'        AS STATUS,
    S_COMMENT       AS NOTES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;


-- Table: TERRITORIES (TPC-H NATION + REGION joined)
-- Source: TPCH_SF1.NATION joined with TPCH_SF1.REGION (25 rows)
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
-- STEP 5: VALIDATE INGESTION
-- ============================================================================

-- Row count summary (all tables in one query)
SELECT 'CUSTOMERS'   AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW_SALES.LANDING.CUSTOMERS   UNION ALL
SELECT 'ORDERS',               COUNT(*) FROM RAW_SALES.LANDING.ORDERS                        UNION ALL
SELECT 'ORDER_ITEMS',          COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS                   UNION ALL
SELECT 'PRODUCTS',             COUNT(*) FROM RAW_SALES.LANDING.PRODUCTS                      UNION ALL
SELECT 'SALES_REPS',           COUNT(*) FROM RAW_SALES.LANDING.SALES_REPS                    UNION ALL
SELECT 'TERRITORIES',          COUNT(*) FROM RAW_SALES.LANDING.TERRITORIES
ORDER BY TABLE_NAME;

-- Expected results:
-- CUSTOMERS    ~150,000
-- ORDER_ITEMS  ~6,000,000
-- ORDERS       ~1,500,000
-- PRODUCTS     ~200,000
-- SALES_REPS   ~10,000
-- TERRITORIES  25


-- ============================================================================
-- STEP 6: SPOT-CHECK SAMPLE DATA (Verify column mappings are correct)
-- ============================================================================

-- Preview each table
SELECT * FROM RAW_SALES.LANDING.CUSTOMERS   LIMIT 5;
SELECT * FROM RAW_SALES.LANDING.ORDERS      LIMIT 5;
SELECT * FROM RAW_SALES.LANDING.ORDER_ITEMS LIMIT 5;
SELECT * FROM RAW_SALES.LANDING.PRODUCTS    LIMIT 5;
SELECT * FROM RAW_SALES.LANDING.SALES_REPS  LIMIT 5;
SELECT * FROM RAW_SALES.LANDING.TERRITORIES LIMIT 25;  -- Only 25 rows total


-- ============================================================================
-- STEP 7: NULL CHECKS (Data quality baseline)
-- ============================================================================

-- Check critical columns for NULLs
SELECT
    'CUSTOMERS'     AS TABLE_NAME,
    'CUSTOMER_ID'   AS COLUMN_NAME,
    SUM(CASE WHEN CUSTOMER_ID IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT,
    COUNT(*)        AS TOTAL_ROWS
FROM RAW_SALES.LANDING.CUSTOMERS

UNION ALL SELECT 'ORDERS', 'ORDER_ID',
    SUM(CASE WHEN ORDER_ID IS NULL THEN 1 ELSE 0 END), COUNT(*)
FROM RAW_SALES.LANDING.ORDERS

UNION ALL SELECT 'ORDERS', 'CUSTOMER_ID',
    SUM(CASE WHEN CUSTOMER_ID IS NULL THEN 1 ELSE 0 END), COUNT(*)
FROM RAW_SALES.LANDING.ORDERS

UNION ALL SELECT 'ORDERS', 'ORDER_DATE',
    SUM(CASE WHEN ORDER_DATE IS NULL THEN 1 ELSE 0 END), COUNT(*)
FROM RAW_SALES.LANDING.ORDERS

UNION ALL SELECT 'ORDER_ITEMS', 'ORDER_ITEM_ID',
    SUM(CASE WHEN ORDER_ITEM_ID IS NULL THEN 1 ELSE 0 END), COUNT(*)
FROM RAW_SALES.LANDING.ORDER_ITEMS

UNION ALL SELECT 'ORDER_ITEMS', 'QUANTITY',
    SUM(CASE WHEN QUANTITY IS NULL OR QUANTITY <= 0 THEN 1 ELSE 0 END), COUNT(*)
FROM RAW_SALES.LANDING.ORDER_ITEMS

UNION ALL SELECT 'PRODUCTS', 'PRODUCT_ID',
    SUM(CASE WHEN PRODUCT_ID IS NULL THEN 1 ELSE 0 END), COUNT(*)
FROM RAW_SALES.LANDING.PRODUCTS;

-- All NULL_COUNTs should be 0 for a clean TPC-H dataset


-- ============================================================================
-- STEP 8: BUSINESS PROFILE (Quick stats on the Sales domain)
-- ============================================================================

-- Order date range (what time period does data cover?)
SELECT
    MIN(ORDER_DATE)   AS EARLIEST_ORDER,
    MAX(ORDER_DATE)   AS LATEST_ORDER,
    DATEDIFF(YEAR, MIN(ORDER_DATE), MAX(ORDER_DATE)) AS YEARS_COVERED
FROM RAW_SALES.LANDING.ORDERS;
-- Expected: ~7 years of order history (1992-1998 for TPC-H SF1)

-- Revenue summary
SELECT
    COUNT(DISTINCT CUSTOMER_ID)     AS UNIQUE_CUSTOMERS,
    COUNT(DISTINCT ORDER_ID)        AS TOTAL_ORDERS,
    ROUND(SUM(ORDER_AMOUNT), 2)     AS TOTAL_REVENUE,
    ROUND(AVG(ORDER_AMOUNT), 2)     AS AVG_ORDER_VALUE,
    ROUND(MIN(ORDER_AMOUNT), 2)     AS MIN_ORDER,
    ROUND(MAX(ORDER_AMOUNT), 2)     AS MAX_ORDER
FROM RAW_SALES.LANDING.ORDERS;

-- Customer segments
SELECT SEGMENT, COUNT(*) AS CUSTOMERS
FROM RAW_SALES.LANDING.CUSTOMERS
GROUP BY SEGMENT
ORDER BY CUSTOMERS DESC;

-- Territory distribution
SELECT REGION, COUNT(*) AS TERRITORY_COUNT
FROM RAW_SALES.LANDING.TERRITORIES
GROUP BY REGION
ORDER BY REGION;

-- Order status distribution
SELECT STATUS, COUNT(*) AS COUNT, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS PCT
FROM RAW_SALES.LANDING.ORDERS
GROUP BY STATUS;


-- ============================================================================
-- PHASE 1 COMPLETE ✓
-- Expected results summary:
-- • 6 tables created in RAW_SALES.LANDING
-- • ~7.86 million total rows ingested
-- • 0 NULLs in critical ID/date columns
-- • Date range: 1992–1998
-- • 5 customer segments, 5 regions, 25 territories
--
-- NEXT STEP: Run 02_phase2_bronze.sql
-- ============================================================================
