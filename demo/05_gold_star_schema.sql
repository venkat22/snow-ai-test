-- ============================================================================
-- DEMO STEP 5: GOLD LAYER — Star Schema + BI Tables
-- ============================================================================
-- What this does:
--   Builds a Kimball-style dimensional model:
--     5 dimensions: dates, territories, products, sales_reps, customers (w/ LTV)
--     1 fact table: fact_orders (one row per order line item)
--     5 BI tables:  monthly_sales_summary, customer_lifetime_value,
--                   product_performance, sales_rep_scorecard, customer_segmentation
--   Adds clustering keys and search optimization for fast BI queries.
--
-- Talk track:
--   "Gold is our consumption layer — a proper star schema. The fact table has
--    ~6M rows clustered by date and territory for fast BI. We pre-aggregate
--    monthly summaries and compute RFM customer segmentation so analysts
--    get sub-second query times without writing complex joins."
--
-- Runtime: ~10-15 min on XS (or ~2 min on MEDIUM)
-- Prerequisites: Step 04 completed
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA GOLD;
USE WAREHOUSE ANALYTICS_WH;

-- Tip: Resize for faster Gold build, then resize back
-- ALTER WAREHOUSE ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';


-- ============================================================================
-- DIMENSIONS
-- ============================================================================

-- dim_dates: Date spine 1992-2000
CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_dates AS
WITH date_spine AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '1992-01-01'::DATE) AS DATE_VALUE
    FROM TABLE(GENERATOR(ROWCOUNT => 3000))
)
SELECT
    DATE_VALUE AS DATE_KEY,
    EXTRACT(YEAR FROM DATE_VALUE) AS YEAR,
    EXTRACT(MONTH FROM DATE_VALUE) AS MONTH,
    EXTRACT(QUARTER FROM DATE_VALUE) AS QUARTER,
    DAYNAME(DATE_VALUE) AS DAY_NAME,
    TO_CHAR(DATE_VALUE, 'MMMM') AS MONTH_NAME,
    TO_CHAR(DATE_VALUE, 'YYYY-MM') AS YEAR_MONTH,
    CASE WHEN DAYNAME(DATE_VALUE) IN ('Sat', 'Sun') THEN FALSE ELSE TRUE END AS IS_WEEKDAY,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM date_spine WHERE DATE_VALUE <= '2000-01-01';

-- dim_territories
CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_territories AS
SELECT TERRITORY_ID, TERRITORY_NAME, REGION_ID, REGION, MANAGER_ID,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.SILVER.territories;

-- dim_products
CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_products AS
SELECT PRODUCT_ID, PRODUCT_NAME, MANUFACTURER, BRAND, CATEGORY,
    FULL_TYPE, PRODUCT_SIZE, CONTAINER_TYPE, UNIT_PRICE,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.SILVER.products;

-- dim_sales_reps (enriched with territory)
CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_sales_reps AS
SELECT SR.REP_ID, SR.NAME, SR.NATION_KEY, T.TERRITORY_NAME, T.REGION,
    SR.QUOTA, SR.STATUS, CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.SILVER.sales_reps SR
LEFT JOIN RAW_SALES.SILVER.territories T ON SR.NATION_KEY = T.TERRITORY_ID;

-- dim_customers (enriched with LTV stats)
CREATE OR REPLACE TABLE RAW_SALES.GOLD.dim_customers AS
SELECT
    C.CUSTOMER_ID, C.NAME, C.ADDRESS, C.NATION_KEY,
    T.TERRITORY_NAME, T.REGION, C.PHONE, C.ACCOUNT_BALANCE, C.SEGMENT,
    COUNT(DISTINCT O.ORDER_ID) AS LIFETIME_ORDER_COUNT,
    COALESCE(SUM(O.ORDER_AMOUNT), 0) AS LIFETIME_VALUE,
    MIN(O.ORDER_DATE) AS FIRST_ORDER_DATE,
    MAX(O.ORDER_DATE) AS LAST_ORDER_DATE,
    DATEDIFF(DAY, MIN(O.ORDER_DATE), MAX(O.ORDER_DATE)) AS CUSTOMER_TENURE_DAYS,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.SILVER.customers C
LEFT JOIN RAW_SALES.SILVER.orders O ON C.CUSTOMER_ID = O.CUSTOMER_ID
LEFT JOIN RAW_SALES.SILVER.territories T ON C.NATION_KEY = T.TERRITORY_ID
GROUP BY C.CUSTOMER_ID, C.NAME, C.ADDRESS, C.NATION_KEY,
         T.TERRITORY_NAME, T.REGION, C.PHONE, C.ACCOUNT_BALANCE, C.SEGMENT;


-- ============================================================================
-- FACT TABLE
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.fact_orders AS
SELECT
    OI.ORDER_ITEM_ID AS ORDER_ITEM_KEY,
    O.ORDER_ID, O.CUSTOMER_ID, OI.PRODUCT_ID,
    TRY_CAST(REGEXP_SUBSTR(O.SALES_CLERK, '[0-9]+') AS INT) AS REP_ID,
    C.NATION_KEY AS TERRITORY_ID,
    O.ORDER_DATE AS DATE_KEY,
    OI.QUANTITY, OI.UNIT_PRICE, OI.DISCOUNT_RATE,
    OI.LINE_TOTAL AS REVENUE,
    O.ORDER_AMOUNT AS ORDER_TOTAL,
    O.STATUS AS ORDER_STATUS,
    OI.RETURN_FLAG, OI.LINE_STATUS, OI.SHIP_MODE,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.SILVER.order_items OI
JOIN RAW_SALES.SILVER.orders O ON OI.ORDER_ID = O.ORDER_ID
JOIN RAW_SALES.SILVER.customers C ON O.CUSTOMER_ID = C.CUSTOMER_ID;

ALTER TABLE RAW_SALES.GOLD.fact_orders CLUSTER BY (DATE_KEY, TERRITORY_ID);


-- ============================================================================
-- BI TABLE: monthly_sales_summary
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.monthly_sales_summary AS
SELECT
    D.YEAR, D.MONTH, D.MONTH_NAME, D.YEAR_MONTH, D.QUARTER,
    T.TERRITORY_ID, T.TERRITORY_NAME, T.REGION,
    COUNT(DISTINCT F.ORDER_ID) AS TOTAL_ORDERS,
    COUNT(DISTINCT F.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    ROUND(SUM(F.REVENUE), 2) AS TOTAL_REVENUE,
    ROUND(AVG(F.REVENUE), 2) AS AVG_LINE_ITEM_VALUE,
    ROUND(SUM(F.ORDER_TOTAL) / COUNT(DISTINCT F.ORDER_ID), 2) AS AVG_ORDER_VALUE,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_dates D ON F.DATE_KEY = D.DATE_KEY
JOIN RAW_SALES.GOLD.dim_territories T ON F.TERRITORY_ID = T.TERRITORY_ID
GROUP BY D.YEAR, D.MONTH, D.MONTH_NAME, D.YEAR_MONTH, D.QUARTER,
         T.TERRITORY_ID, T.TERRITORY_NAME, T.REGION;


-- ============================================================================
-- BI TABLE: customer_lifetime_value (with RFM + engagement)
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_lifetime_value AS
SELECT
    C.CUSTOMER_ID, C.NAME, C.SEGMENT, C.TERRITORY_NAME, C.REGION,
    C.LIFETIME_VALUE, C.LIFETIME_ORDER_COUNT,
    C.FIRST_ORDER_DATE, C.LAST_ORDER_DATE, C.CUSTOMER_TENURE_DAYS,
    DATEDIFF(DAY, C.LAST_ORDER_DATE, (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) AS RECENCY_DAYS,
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY C.LIFETIME_VALUE) >= 0.75 THEN 'High-Value'
        WHEN PERCENT_RANK() OVER (ORDER BY C.LIFETIME_VALUE) >= 0.50 THEN 'Medium-Value'
        WHEN PERCENT_RANK() OVER (ORDER BY C.LIFETIME_VALUE) >= 0.25 THEN 'Low-Value'
        ELSE 'Minimal-Value'
    END AS VALUE_SEGMENT,
    CASE
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) > 365 THEN 'Churned'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) > 180 THEN 'At-Risk'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) > 90 THEN 'Dormant'
        ELSE 'Active'
    END AS ENGAGEMENT_STATUS,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.GOLD.dim_customers C;


-- ============================================================================
-- BI TABLE: product_performance
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.product_performance AS
SELECT
    P.PRODUCT_ID, P.PRODUCT_NAME, P.CATEGORY, P.MANUFACTURER, P.BRAND, P.UNIT_PRICE,
    COUNT(DISTINCT F.ORDER_ID) AS TOTAL_ORDERS,
    ROUND(SUM(F.QUANTITY), 0) AS TOTAL_UNITS_SOLD,
    ROUND(SUM(F.REVENUE), 2) AS TOTAL_REVENUE,
    ROUND(AVG(F.REVENUE), 2) AS AVG_REVENUE_PER_LINE,
    COUNT(CASE WHEN F.RETURN_FLAG = 'R' THEN 1 END) AS RETURNED_ITEMS,
    ROUND(100.0 * COUNT(CASE WHEN F.RETURN_FLAG = 'R' THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS RETURN_RATE_PCT,
    DENSE_RANK() OVER (ORDER BY SUM(F.REVENUE) DESC) AS REVENUE_RANK,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_products P ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME, P.CATEGORY, P.MANUFACTURER, P.BRAND, P.UNIT_PRICE;


-- ============================================================================
-- BI TABLE: sales_rep_scorecard
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.sales_rep_scorecard AS
SELECT
    SR.REP_ID, SR.NAME, SR.TERRITORY_NAME, SR.REGION, SR.QUOTA,
    COUNT(DISTINCT F.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    COUNT(DISTINCT F.ORDER_ID) AS TOTAL_ORDERS,
    ROUND(SUM(F.REVENUE), 2) AS TOTAL_REVENUE,
    ROUND(AVG(F.REVENUE), 2) AS AVG_REVENUE_PER_LINE,
    ROUND(MAX(F.ORDER_TOTAL), 2) AS LARGEST_ORDER,
    ROUND(SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) * 100, 1) AS QUOTA_ATTAINMENT_PCT,
    CASE
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 1.0 THEN 'Exceeds Quota'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.8 THEN 'On Track'
        WHEN SUM(F.REVENUE) / NULLIF(SR.QUOTA, 0) >= 0.5 THEN 'Below Target'
        ELSE 'At Risk'
    END AS PERFORMANCE_STATUS,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_sales_reps SR ON F.REP_ID = SR.REP_ID
GROUP BY SR.REP_ID, SR.NAME, SR.TERRITORY_NAME, SR.REGION, SR.QUOTA;


-- ============================================================================
-- BI TABLE: customer_segmentation (RFM scoring)
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.GOLD.customer_segmentation AS
WITH rfm AS (
    SELECT F.CUSTOMER_ID,
        DATEDIFF(DAY, MAX(F.DATE_KEY), (SELECT MAX(ORDER_DATE) FROM RAW_SALES.SILVER.orders)) AS RECENCY_DAYS,
        COUNT(DISTINCT F.ORDER_ID) AS FREQUENCY,
        ROUND(SUM(F.REVENUE), 2) AS MONETARY
    FROM RAW_SALES.GOLD.fact_orders F GROUP BY F.CUSTOMER_ID
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY RECENCY_DAYS ASC) AS R_SCORE,
        NTILE(5) OVER (ORDER BY FREQUENCY DESC) AS F_SCORE,
        NTILE(5) OVER (ORDER BY MONETARY DESC) AS M_SCORE
    FROM rfm
)
SELECT R.CUSTOMER_ID, C.NAME, C.SEGMENT, C.TERRITORY_NAME, C.REGION,
    R.RECENCY_DAYS, R.FREQUENCY, R.MONETARY,
    R.R_SCORE, R.F_SCORE, R.M_SCORE,
    (R.R_SCORE + R.F_SCORE + R.M_SCORE) AS RFM_TOTAL_SCORE,
    CASE
        WHEN R.R_SCORE >= 4 AND R.F_SCORE >= 4 AND R.M_SCORE >= 4 THEN 'VIP'
        WHEN R.R_SCORE >= 3 AND R.F_SCORE >= 3 THEN 'Loyal'
        WHEN R.R_SCORE >= 4 AND R.F_SCORE <= 2 THEN 'New Customer'
        WHEN R.R_SCORE <= 2 AND R.M_SCORE >= 4 THEN 'At-Risk High-Value'
        WHEN R.R_SCORE <= 2 AND R.F_SCORE <= 2 THEN 'Churned'
        ELSE 'Growing'
    END AS CUSTOMER_SEGMENT,
    CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM rfm_scored R JOIN RAW_SALES.GOLD.dim_customers C ON R.CUSTOMER_ID = C.CUSTOMER_ID;


-- ============================================================================
-- PERFORMANCE: Search optimization + AI semantic stubs
-- ============================================================================

ALTER TABLE RAW_SALES.GOLD.fact_orders
    ADD SEARCH OPTIMIZATION ON EQUALITY(CUSTOMER_ID, PRODUCT_ID, TERRITORY_ID, ORDER_ID);

CREATE OR REPLACE TABLE RAW_SALES.GOLD.ai_semantic_metadata (
    ENTITY_NAME VARCHAR(200), COLUMN_NAME VARCHAR(200),
    BUSINESS_DEFINITION VARCHAR(2000), EXAMPLE_VALUE VARCHAR(500),
    EMBEDDING_READY_TEXT VARCHAR(4000), EMBEDDING_VECTOR_STUB VARIANT,
    _REFRESHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT OVERWRITE INTO RAW_SALES.GOLD.ai_semantic_metadata
SELECT 'fact_orders', 'REVENUE', 'Line-level revenue after discount', '1234.56',
    'Revenue is monetary value for a sold order item after discount.', PARSE_JSON('[0.0]'), CURRENT_TIMESTAMP()
UNION ALL
SELECT 'customer_lifetime_value', 'LIFETIME_VALUE', 'Cumulative customer spending', '98765.43',
    'Lifetime value is the total historical spend of a customer.', PARSE_JSON('[0.0]'), CURRENT_TIMESTAMP();


-- ============================================================================
-- VALIDATION
-- ============================================================================

SELECT 'dim_dates' AS T, COUNT(*) AS ROWS FROM RAW_SALES.GOLD.dim_dates          UNION ALL
SELECT 'dim_customers',    COUNT(*) FROM RAW_SALES.GOLD.dim_customers             UNION ALL
SELECT 'dim_products',     COUNT(*) FROM RAW_SALES.GOLD.dim_products              UNION ALL
SELECT 'dim_sales_reps',   COUNT(*) FROM RAW_SALES.GOLD.dim_sales_reps            UNION ALL
SELECT 'dim_territories',  COUNT(*) FROM RAW_SALES.GOLD.dim_territories           UNION ALL
SELECT 'fact_orders',      COUNT(*) FROM RAW_SALES.GOLD.fact_orders               UNION ALL
SELECT 'monthly_summary',  COUNT(*) FROM RAW_SALES.GOLD.monthly_sales_summary     UNION ALL
SELECT 'customer_ltv',     COUNT(*) FROM RAW_SALES.GOLD.customer_lifetime_value   UNION ALL
SELECT 'product_perf',     COUNT(*) FROM RAW_SALES.GOLD.product_performance       UNION ALL
SELECT 'rep_scorecard',    COUNT(*) FROM RAW_SALES.GOLD.sales_rep_scorecard       UNION ALL
SELECT 'cust_segments',    COUNT(*) FROM RAW_SALES.GOLD.customer_segmentation
ORDER BY T;

-- Revenue sanity check: Gold must equal Silver
SELECT 'Silver' AS SRC, ROUND(SUM(LINE_TOTAL), 2) AS REVENUE FROM RAW_SALES.SILVER.order_items
UNION ALL
SELECT 'Gold', ROUND(SUM(REVENUE), 2) FROM RAW_SALES.GOLD.fact_orders;

-- Quick BI test
SELECT YEAR, QUARTER, ROUND(SUM(TOTAL_REVENUE), 2) AS QUARTERLY_REVENUE
FROM RAW_SALES.GOLD.monthly_sales_summary
GROUP BY YEAR, QUARTER ORDER BY YEAR, QUARTER;


-- ============================================================================
-- CHECKPOINT: Star schema complete. 5 dims + 1 fact + 5 BI tables.
-- NEXT: Run 06_data_products_and_governance.sql
-- ============================================================================
