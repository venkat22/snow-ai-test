-- ============================================================================
-- PHASE 2C: GOLD LAYER — Dimensional Model + BI Reporting Tables
-- Purpose: Build star schema and pre-aggregated tables for BI & reporting
-- Prerequisite: 03_phase2_silver.sql completed successfully
-- Estimated runtime: ~10-15 minutes on XS warehouse (scanning 6M+ rows)
-- Tip: Use MEDIUM warehouse to speed up Gold build (~2 min), then switch back to XS
-- ============================================================================

-- If unsure of your warehouse name, run: SHOW WAREHOUSES;
-- Tip: Resize to MEDIUM for faster Gold build: ALTER WAREHOUSE ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';
--      Then resize back after:               ALTER WAREHOUSE ANALYTICS_WH SET WAREHOUSE_SIZE = 'X-SMALL';
USE DATABASE RAW_SALES;
USE SCHEMA GOLD;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- DIMENSION: dim_dates
-- Generated date spine from 1992-01-01 to 2000-01-01 (covers TPC-H data range)
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_dates AS
WITH date_spine AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '1992-01-01'::DATE) AS DATE_VALUE
    FROM TABLE(GENERATOR(ROWCOUNT => 3000))  -- ~8 years of dates
)
SELECT
    DATE_VALUE                                          AS DATE_KEY,
    EXTRACT(YEAR FROM DATE_VALUE)                       AS YEAR,
    EXTRACT(MONTH FROM DATE_VALUE)                      AS MONTH,
    EXTRACT(QUARTER FROM DATE_VALUE)                    AS QUARTER,
    EXTRACT(WEEK FROM DATE_VALUE)                       AS WEEK_OF_YEAR,
    EXTRACT(DOY FROM DATE_VALUE)                        AS DAY_OF_YEAR,
    EXTRACT(DAY FROM DATE_VALUE)                        AS DAY_OF_MONTH,
    DAYNAME(DATE_VALUE)                                 AS DAY_NAME,
    TO_CHAR(DATE_VALUE, 'MMMM')                         AS MONTH_NAME,
    TO_CHAR(DATE_VALUE, 'YYYY-MM')                      AS YEAR_MONTH,
    CASE WHEN DAYNAME(DATE_VALUE) IN ('Sat', 'Sun')
         THEN FALSE ELSE TRUE END                       AS IS_WEEKDAY,
    CURRENT_TIMESTAMP()                                 AS _REFRESHED_AT
FROM date_spine
WHERE DATE_VALUE <= '2000-01-01';


-- ============================================================================
-- DIMENSION: dim_territories
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_territories AS
SELECT
    TERRITORY_ID,
    TERRITORY_NAME,
    REGION_ID,
    REGION,
    MANAGER_ID,
    CURRENT_TIMESTAMP()             AS _REFRESHED_AT
FROM RAW_SALES.SILVER.territories;


-- ============================================================================
-- DIMENSION: dim_products
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_products AS
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
    CURRENT_TIMESTAMP()             AS _REFRESHED_AT
FROM RAW_SALES.SILVER.products;


-- ============================================================================
-- DIMENSION: dim_sales_reps
-- Enhance with territory info via NATION_KEY join
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_sales_reps AS
SELECT
    SR.REP_ID,
    SR.NAME,
    SR.NATION_KEY,
    T.TERRITORY_NAME,
    T.REGION,
    SR.QUOTA,
    SR.STATUS,
    CURRENT_TIMESTAMP()             AS _REFRESHED_AT
FROM RAW_SALES.SILVER.sales_reps SR
LEFT JOIN RAW_SALES.SILVER.territories T
    ON SR.NATION_KEY = T.TERRITORY_ID;


-- ============================================================================
-- DIMENSION: dim_customers
-- Enriched with order statistics for LTV calculations
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_customers AS
SELECT
    C.CUSTOMER_ID,
    C.NAME,
    C.ADDRESS,
    C.NATION_KEY,
    T.TERRITORY_NAME,
    T.REGION,
    C.PHONE,
    C.ACCOUNT_BALANCE,
    C.SEGMENT,
    COUNT(DISTINCT O.ORDER_ID)                                  AS LIFETIME_ORDER_COUNT,
    COALESCE(SUM(O.ORDER_AMOUNT), 0)                            AS LIFETIME_VALUE,
    MIN(O.ORDER_DATE)                                           AS FIRST_ORDER_DATE,
    MAX(O.ORDER_DATE)                                           AS LAST_ORDER_DATE,
    DATEDIFF(DAY, MIN(O.ORDER_DATE), MAX(O.ORDER_DATE))         AS CUSTOMER_TENURE_DAYS,
    CURRENT_TIMESTAMP()                                         AS _REFRESHED_AT
FROM RAW_SALES.SILVER.customers C
LEFT JOIN RAW_SALES.SILVER.orders O
    ON C.CUSTOMER_ID = O.CUSTOMER_ID
LEFT JOIN RAW_SALES.SILVER.territories T
    ON C.NATION_KEY = T.TERRITORY_ID
GROUP BY C.CUSTOMER_ID, C.NAME, C.ADDRESS, C.NATION_KEY,
         T.TERRITORY_NAME, T.REGION, C.PHONE, C.ACCOUNT_BALANCE, C.SEGMENT;


-- ============================================================================
-- FACT: fact_orders
-- Central fact table — one row per order line item
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.fact_orders AS
SELECT
    OI.ORDER_ITEM_ID                AS ORDER_ITEM_KEY,
    O.ORDER_ID,
    O.CUSTOMER_ID,
    OI.PRODUCT_ID,
    -- Map SALES_CLERK → REP_ID via a numeric hash (TPC-H clerks are in 'Clerk#0000XXXXX' format)
    TRY_CAST(REGEXP_SUBSTR(O.SALES_CLERK, '[0-9]+') AS INT) AS REP_ID,
    C.NATION_KEY                    AS TERRITORY_ID,
    O.ORDER_DATE                    AS DATE_KEY,
    OI.QUANTITY,
    OI.UNIT_PRICE,
    OI.DISCOUNT_RATE,
    OI.LINE_TOTAL                   AS REVENUE,
    O.ORDER_AMOUNT                  AS ORDER_TOTAL,
    O.STATUS                        AS ORDER_STATUS,
    OI.RETURN_FLAG,
    OI.LINE_STATUS,
    OI.SHIP_MODE,
    CURRENT_TIMESTAMP()             AS _REFRESHED_AT
FROM RAW_SALES.SILVER.order_items OI
JOIN RAW_SALES.SILVER.orders O
    ON OI.ORDER_ID = O.ORDER_ID
JOIN RAW_SALES.SILVER.customers C
    ON O.CUSTOMER_ID = C.CUSTOMER_ID;

-- Add clustering key (improves BI query performance on this large table)
ALTER TABLE RAW_SALES.GOLD.fact_orders
    CLUSTER BY (DATE_KEY, TERRITORY_ID);


-- ============================================================================
-- BI TABLE 1: monthly_sales_summary
-- Revenue & volume aggregated by month × territory
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.monthly_sales_summary AS
SELECT
    D.YEAR,
    D.MONTH,
    D.MONTH_NAME,
    D.YEAR_MONTH,
    D.QUARTER,
    T.TERRITORY_ID,
    T.TERRITORY_NAME,
    T.REGION,
    COUNT(DISTINCT F.ORDER_ID)                          AS TOTAL_ORDERS,
    COUNT(DISTINCT F.CUSTOMER_ID)                       AS UNIQUE_CUSTOMERS,
    COUNT(F.ORDER_ITEM_KEY)                             AS TOTAL_LINE_ITEMS,
    ROUND(SUM(F.REVENUE), 2)                            AS TOTAL_REVENUE,
    ROUND(AVG(F.REVENUE), 2)                            AS AVG_LINE_ITEM_VALUE,
    ROUND(SUM(F.ORDER_TOTAL) / COUNT(DISTINCT F.ORDER_ID), 2) AS AVG_ORDER_VALUE,
    ROUND(MAX(F.ORDER_TOTAL), 2)                        AS MAX_ORDER_VALUE,
    ROUND(MIN(F.ORDER_TOTAL), 2)                        AS MIN_ORDER_VALUE,
    CURRENT_TIMESTAMP()                                 AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_dates D
    ON F.DATE_KEY = D.DATE_KEY
JOIN RAW_SALES.GOLD.dim_territories T
    ON F.TERRITORY_ID = T.TERRITORY_ID
GROUP BY D.YEAR, D.MONTH, D.MONTH_NAME, D.YEAR_MONTH, D.QUARTER,
         T.TERRITORY_ID, T.TERRITORY_NAME, T.REGION;

ALTER TABLE RAW_SALES.GOLD.monthly_sales_summary
    CLUSTER BY (YEAR, MONTH, TERRITORY_ID);


-- ============================================================================
-- BI TABLE 2: customer_lifetime_value
-- RFM scoring + engagement status per customer
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_lifetime_value AS
SELECT
    C.CUSTOMER_ID,
    C.NAME,
    C.SEGMENT,
    C.TERRITORY_NAME,
    C.REGION,
    C.LIFETIME_VALUE,
    C.LIFETIME_ORDER_COUNT,
    C.FIRST_ORDER_DATE,
    C.LAST_ORDER_DATE,
    C.CUSTOMER_TENURE_DAYS,
    -- RFM: Recency (days since last order within dataset)
    DATEDIFF(DAY, C.LAST_ORDER_DATE,
        (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) AS RECENCY_DAYS,
    -- Value segment based on percentile
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY C.LIFETIME_VALUE)  >= 0.75 THEN 'High-Value'
        WHEN PERCENT_RANK() OVER (ORDER BY C.LIFETIME_VALUE)  >= 0.50 THEN 'Medium-Value'
        WHEN PERCENT_RANK() OVER (ORDER BY C.LIFETIME_VALUE)  >= 0.25 THEN 'Low-Value'
        ELSE 'Minimal-Value'
    END AS VALUE_SEGMENT,
    -- Engagement status based on days since last order
    CASE
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE,
            (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) > 365 THEN 'Churned'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE,
            (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) > 180 THEN 'At-Risk'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE,
            (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) > 90 THEN 'Dormant'
        ELSE 'Active'
    END AS ENGAGEMENT_STATUS,
    CURRENT_TIMESTAMP()                                 AS _REFRESHED_AT
FROM RAW_SALES.GOLD.dim_customers C;


-- ============================================================================
-- BI TABLE 3: product_performance
-- Revenue, volume, and ranking per product
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.product_performance AS
SELECT
    P.PRODUCT_ID,
    P.PRODUCT_NAME,
    P.CATEGORY,
    P.MANUFACTURER,
    P.BRAND,
    P.UNIT_PRICE,
    COUNT(DISTINCT F.ORDER_ID)                          AS TOTAL_ORDERS,
    ROUND(SUM(F.QUANTITY), 0)                           AS TOTAL_UNITS_SOLD,
    ROUND(SUM(F.REVENUE), 2)                            AS TOTAL_REVENUE,
    ROUND(AVG(F.REVENUE), 2)                            AS AVG_REVENUE_PER_LINE,
    ROUND(SUM(F.QUANTITY) / NULLIF(COUNT(DISTINCT F.ORDER_ID), 0), 2) AS AVG_UNITS_PER_ORDER,
    COUNT(CASE WHEN F.RETURN_FLAG = 'R' THEN 1 END)     AS RETURNED_ITEMS,
    ROUND(100.0 * COUNT(CASE WHEN F.RETURN_FLAG = 'R' THEN 1 END)
          / NULLIF(COUNT(*), 0), 2)                     AS RETURN_RATE_PCT,
    DENSE_RANK() OVER (ORDER BY SUM(F.REVENUE) DESC)    AS REVENUE_RANK,
    DENSE_RANK() OVER (ORDER BY SUM(F.QUANTITY) DESC)   AS VOLUME_RANK,
    CASE
        WHEN DENSE_RANK() OVER (ORDER BY SUM(F.REVENUE) DESC) <= 20 THEN 'Top 20'
        WHEN DENSE_RANK() OVER (ORDER BY SUM(F.REVENUE) DESC) <= 100 THEN 'Top 100'
        ELSE 'Long Tail'
    END AS PRODUCT_FLAG,
    CURRENT_TIMESTAMP()                                 AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_products P
    ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME, P.CATEGORY, P.MANUFACTURER, P.BRAND, P.UNIT_PRICE;


-- ============================================================================
-- BI TABLE 4: sales_rep_scorecard
-- Rep performance KPIs aggregated over full history
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.sales_rep_scorecard AS
SELECT
    SR.REP_ID,
    SR.NAME,
    SR.TERRITORY_NAME,
    SR.REGION,
    SR.QUOTA,
    COUNT(DISTINCT F.CUSTOMER_ID)                       AS UNIQUE_CUSTOMERS,
    COUNT(DISTINCT F.ORDER_ID)                          AS TOTAL_ORDERS,
    ROUND(SUM(F.REVENUE), 2)                            AS TOTAL_REVENUE,
    ROUND(AVG(F.REVENUE), 2)                            AS AVG_REVENUE_PER_LINE,
    ROUND(MAX(F.ORDER_TOTAL), 2)                        AS LARGEST_ORDER,
    ROUND(SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0), 4)     AS QUOTA_ATTAINMENT_RATIO,
    ROUND(SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) * 100, 1) AS QUOTA_ATTAINMENT_PCT,
    ROUND(SUM(F.QUANTITY), 0)                           AS TOTAL_UNITS_SOLD,
    CASE
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 1.0   THEN 'Exceeds Quota'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.8   THEN 'On Track'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.5   THEN 'Below Target'
        ELSE 'At Risk'
    END AS PERFORMANCE_STATUS,
    CURRENT_TIMESTAMP()                                 AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_sales_reps SR
    ON F.REP_ID = SR.REP_ID
GROUP BY SR.REP_ID, SR.NAME, SR.TERRITORY_NAME, SR.REGION, SR.QUOTA;


-- ============================================================================
-- BI TABLE 5: customer_segmentation
-- RFM scoring + derived segment for each customer
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_segmentation AS
WITH rfm AS (
    SELECT
        F.CUSTOMER_ID,
        DATEDIFF(DAY, MAX(F.DATE_KEY),
            (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) AS RECENCY_DAYS,
        COUNT(DISTINCT F.ORDER_ID)          AS FREQUENCY,
        ROUND(SUM(F.REVENUE), 2)            AS MONETARY
    FROM RAW_SALES.GOLD.fact_orders F
    GROUP BY F.CUSTOMER_ID
),
rfm_scored AS (
    SELECT *,
        -- Score each dimension 1-5 using NTILE
        NTILE(5) OVER (ORDER BY RECENCY_DAYS ASC)   AS R_SCORE,   -- Lower recency = higher score
        NTILE(5) OVER (ORDER BY FREQUENCY DESC)      AS F_SCORE,
        NTILE(5) OVER (ORDER BY MONETARY DESC)       AS M_SCORE
    FROM rfm
)
SELECT
    R.CUSTOMER_ID,
    C.NAME,
    C.SEGMENT,
    C.TERRITORY_NAME,
    C.REGION,
    R.RECENCY_DAYS,
    R.FREQUENCY,
    R.MONETARY,
    R.R_SCORE,
    R.F_SCORE,
    R.M_SCORE,
    (R.R_SCORE + R.F_SCORE + R.M_SCORE)            AS RFM_TOTAL_SCORE,
    CASE
        WHEN R.R_SCORE >= 4 AND R.F_SCORE >= 4 AND R.M_SCORE >= 4 THEN 'VIP'
        WHEN R.R_SCORE >= 3 AND R.F_SCORE >= 3 THEN 'Loyal'
        WHEN R.R_SCORE >= 4 AND R.F_SCORE <= 2 THEN 'New Customer'
        WHEN R.R_SCORE <= 2 AND R.M_SCORE >= 4 THEN 'At-Risk High-Value'
        WHEN R.R_SCORE <= 2 AND R.F_SCORE <= 2 THEN 'Churned'
        ELSE 'Growing'
    END AS CUSTOMER_SEGMENT,
    CURRENT_TIMESTAMP()                             AS _REFRESHED_AT
FROM rfm_scored R
JOIN RAW_SALES.GOLD.dim_customers C
    ON R.CUSTOMER_ID = C.CUSTOMER_ID;


-- ============================================================================
-- PERFORMANCE CAPABILITY: SEARCH OPTIMIZATION
-- ============================================================================

ALTER TABLE RAW_SALES.GOLD.fact_orders
    ADD SEARCH OPTIMIZATION ON EQUALITY(CUSTOMER_ID, PRODUCT_ID, TERRITORY_ID, ORDER_ID);

ALTER TABLE RAW_SALES.GOLD.dim_customers
    ADD SEARCH OPTIMIZATION ON EQUALITY(CUSTOMER_ID, NATION_KEY);

ALTER TABLE RAW_SALES.GOLD.dim_products
    ADD SEARCH OPTIMIZATION ON EQUALITY(PRODUCT_ID, CATEGORY);


-- ============================================================================
-- AI-READY STUBS: semantic metadata + retrieval scaffolding
-- These are architecture-level stubs, not production RAG implementation.
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.ai_semantic_metadata (
    ENTITY_NAME              VARCHAR(200),
    COLUMN_NAME              VARCHAR(200),
    BUSINESS_DEFINITION      VARCHAR(2000),
    EXAMPLE_VALUE            VARCHAR(500),
    EMBEDDING_READY_TEXT     VARCHAR(4000),
    EMBEDDING_VECTOR_STUB    VARIANT,
    _REFRESHED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT OVERWRITE INTO RAW_SALES.GOLD.ai_semantic_metadata
SELECT
    'fact_orders'                                        AS ENTITY_NAME,
    'REVENUE'                                            AS COLUMN_NAME,
    'Line-level revenue after discount from order items' AS BUSINESS_DEFINITION,
    '1234.56'                                            AS EXAMPLE_VALUE,
    'Revenue is monetary value for a sold order item after discount.' AS EMBEDDING_READY_TEXT,
    PARSE_JSON('[0.0, 0.0, 0.0]')                       AS EMBEDDING_VECTOR_STUB,
    CURRENT_TIMESTAMP()                                  AS _REFRESHED_AT
UNION ALL
SELECT
    'customer_lifetime_value',
    'LIFETIME_VALUE',
    'Cumulative spending by customer across all recorded orders',
    '98765.43',
    'Lifetime value is the total historical spend of a customer.',
    PARSE_JSON('[0.0, 0.0, 0.0]'),
    CURRENT_TIMESTAMP()
UNION ALL
SELECT
    'sales_rep_monthly_performance',
    'QUOTA_ATTAINMENT_PCT',
    'Percent of quota achieved by a sales representative in a month',
    '84.1',
    'Quota attainment percent compares generated revenue to assigned quota.',
    PARSE_JSON('[0.0, 0.0, 0.0]'),
    CURRENT_TIMESTAMP();

CREATE OR REPLACE TABLE RAW_SALES.GOLD.ai_retrieval_index_stub (
    DOC_ID                    VARCHAR(200),
    DOC_TYPE                  VARCHAR(100),
    SOURCE_TABLE              VARCHAR(200),
    SOURCE_KEY                VARCHAR(200),
    CHUNK_TEXT                VARCHAR(4000),
    EMBEDDING_VECTOR_STUB     VARIANT,
    _REFRESHED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT OVERWRITE INTO RAW_SALES.GOLD.ai_retrieval_index_stub
SELECT
    'doc_customer_ltv_1' AS DOC_ID,
    'metric_definition'  AS DOC_TYPE,
    'GOLD.customer_lifetime_value' AS SOURCE_TABLE,
    'CUSTOMER_ID=1'      AS SOURCE_KEY,
    'Customer lifetime value summarizes total spend and can be used for value-based segmentation.' AS CHUNK_TEXT,
    PARSE_JSON('[0.0, 0.0, 0.0]') AS EMBEDDING_VECTOR_STUB,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
UNION ALL
SELECT
    'doc_sales_perf_1',
    'kpi_definition',
    'GOLD.sales_rep_monthly_performance',
    'REP_ID=1',
    'Quota attainment indicates monthly progress toward sales targets for a representative.',
    PARSE_JSON('[0.0, 0.0, 0.0]'),
    CURRENT_TIMESTAMP();

CREATE OR REPLACE VIEW RAW_SALES.GOLD.ai_rag_query_path_stub AS
SELECT
    M.ENTITY_NAME,
    M.COLUMN_NAME,
    M.BUSINESS_DEFINITION,
    I.DOC_ID,
    I.CHUNK_TEXT,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.GOLD.ai_semantic_metadata M
LEFT JOIN RAW_SALES.GOLD.ai_retrieval_index_stub I
    ON I.SOURCE_TABLE ILIKE '%' || M.ENTITY_NAME || '%';


-- ============================================================================
-- GOLD VERIFICATION
-- ============================================================================

-- 1. Row count summary
SELECT 'dim_dates'              TABLE_NAME, COUNT(*) ROW_COUNT FROM RAW_SALES.GOLD.dim_dates          UNION ALL
SELECT 'dim_customers',                     COUNT(*) FROM RAW_SALES.GOLD.dim_customers                UNION ALL
SELECT 'dim_products',                      COUNT(*) FROM RAW_SALES.GOLD.dim_products                 UNION ALL
SELECT 'dim_sales_reps',                    COUNT(*) FROM RAW_SALES.GOLD.dim_sales_reps               UNION ALL
SELECT 'dim_territories',                   COUNT(*) FROM RAW_SALES.GOLD.dim_territories              UNION ALL
SELECT 'fact_orders',                       COUNT(*) FROM RAW_SALES.GOLD.fact_orders                  UNION ALL
SELECT 'monthly_sales_summary',             COUNT(*) FROM RAW_SALES.GOLD.monthly_sales_summary        UNION ALL
SELECT 'customer_lifetime_value',           COUNT(*) FROM RAW_SALES.GOLD.customer_lifetime_value      UNION ALL
SELECT 'product_performance',               COUNT(*) FROM RAW_SALES.GOLD.product_performance          UNION ALL
SELECT 'sales_rep_scorecard',               COUNT(*) FROM RAW_SALES.GOLD.sales_rep_scorecard          UNION ALL
SELECT 'customer_segmentation',             COUNT(*) FROM RAW_SALES.GOLD.customer_segmentation
ORDER BY TABLE_NAME;


-- 2. Revenue sanity check (Gold must equal Silver total revenue)
SELECT
    'Silver total revenue' AS SOURCE,
    ROUND(SUM(LINE_TOTAL), 2) AS TOTAL_REVENUE
FROM RAW_SALES.SILVER.order_items
UNION ALL
SELECT
    'Gold fact_orders revenue',
    ROUND(SUM(REVENUE), 2)
FROM RAW_SALES.GOLD.fact_orders;
-- Must be equal (or very close — minor variance from NULL REP_ID joins is acceptable)


-- 3. Sample BI query: Revenue by year and quarter
SELECT YEAR, QUARTER, ROUND(SUM(TOTAL_REVENUE), 2) AS QUARTERLY_REVENUE
FROM RAW_SALES.GOLD.monthly_sales_summary
GROUP BY YEAR, QUARTER
ORDER BY YEAR, QUARTER;


-- 4. Top 10 customers by lifetime value
SELECT CUSTOMER_ID, NAME, SEGMENT, LIFETIME_VALUE, ENGAGEMENT_STATUS, VALUE_SEGMENT
FROM RAW_SALES.GOLD.customer_lifetime_value
ORDER BY LIFETIME_VALUE DESC
LIMIT 10;


-- 5. Customer segment distribution
SELECT CUSTOMER_SEGMENT, COUNT(*) AS CUSTOMERS,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS PCT
FROM RAW_SALES.GOLD.customer_segmentation
GROUP BY CUSTOMER_SEGMENT
ORDER BY CUSTOMERS DESC;


-- ============================================================================
-- PHASE 2 COMPLETE ✓
-- You now have a complete Bronze → Silver → Gold medallion architecture
-- Expected:
--   • 11 Gold tables/views created
--   • Revenue in Gold matches Silver (sanity check)
--   • BI queries return meaningful results
--   • All DQ checks logged in MONITORING.data_quality_log
--
-- NEXT STEP: Run 05_phase3_data_products.sql
-- ============================================================================
