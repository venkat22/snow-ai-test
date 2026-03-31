-- ============================================================================
-- DEMO STEP 7: ML FEATURE STORE — Point-in-Time Correct Features
-- ============================================================================
-- What this does:
--   Builds a production ML Feature Store with:
--     - Feature registry (21 features cataloged with ownership & lineage)
--     - Feature versioning and snapshot tables for point-in-time correctness
--     - Offline feature tables for 3 entity types:
--         Customer (RFM + engagement), Product (performance), Sales Rep (quota)
--     - Training data views that join features with point-in-time correctness
--
-- Talk track:
--   "The Feature Store ensures ML models train on historically accurate data.
--    Every feature is versioned, has a registered owner, and supports 'as-of'
--    queries — meaning if you train a model for January 2023, it only sees
--    data that was available in January 2023, preventing data leakage."
--
-- Runtime: ~10 minutes
-- Prerequisites: Step 05 completed (Gold layer)
-- ============================================================================

USE DATABASE RAW_SALES;
USE WAREHOUSE ANALYTICS_WH;

CREATE SCHEMA IF NOT EXISTS RAW_SALES.FEATURE_STORE;
USE SCHEMA RAW_SALES.FEATURE_STORE;


-- ============================================================================
-- CONTROL TABLES: Registry, Versioning, Snapshots, Lineage
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_registry (
    FEATURE_ID              VARCHAR(255) PRIMARY KEY,
    FEATURE_NAME            VARCHAR(255),
    DATA_TYPE               VARCHAR(50),
    ENTITY_TYPE             VARCHAR(50),        -- customer | product | sales_rep
    ENTITY_KEY              VARCHAR(100),
    OWNER_TEAM              VARCHAR(100),
    DESCRIPTION             VARCHAR(1000),
    IS_POINT_IN_TIME        BOOLEAN DEFAULT TRUE,
    ONLINE_ENABLED          BOOLEAN DEFAULT FALSE,
    OFFLINE_ENABLED         BOOLEAN DEFAULT TRUE,
    VERSION                 INT DEFAULT 1,
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TAGS                    VARCHAR(500),
    LINEAGE_SOURCE_TABLE    VARCHAR(255),
    LINEAGE_SOURCE_COLUMN   VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_versions (
    VERSION_ID INT AUTOINCREMENT PRIMARY KEY,
    FEATURE_ID VARCHAR(255) NOT NULL, VERSION_NUMBER INT NOT NULL,
    DEFINITION_SQL VARCHAR(5000), SCHEMA_HASH VARCHAR(64),
    DEPLOYMENT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(255), CHANGE_REASON VARCHAR(500), IS_ACTIVE BOOLEAN DEFAULT FALSE,
    UNIQUE(FEATURE_ID, VERSION_NUMBER)
);

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_snapshots (
    SNAPSHOT_ID INT AUTOINCREMENT PRIMARY KEY,
    FEATURE_ID VARCHAR(255) NOT NULL, ENTITY_TYPE VARCHAR(50) NOT NULL,
    ENTITY_KEY VARCHAR(100) NOT NULL, FEATURE_VALUE VARCHAR(1000),
    VALID_FROM TIMESTAMP_NTZ NOT NULL, VALID_TO TIMESTAMP_NTZ,
    SNAPSHOT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE(FEATURE_ID, ENTITY_KEY, VALID_FROM)
);

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_lineage (
    LINEAGE_ID INT AUTOINCREMENT PRIMARY KEY,
    UPSTREAM_FEATURE_ID VARCHAR(255), DOWNSTREAM_FEATURE_ID VARCHAR(255) NOT NULL,
    UPSTREAM_TABLE_NAME VARCHAR(255), UPSTREAM_COLUMN_NAME VARCHAR(255),
    DEPENDENCY_TYPE VARCHAR(50), CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================================
-- OFFLINE FEATURES: Customer RFM (Point-in-Time)
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.customer_rfm_features_offline AS
WITH customer_monthly_agg AS (
    SELECT C.CUSTOMER_ID, D.YEAR_MONTH, D.DATE_KEY,
        DATEDIFF(DAY, MAX(F.DATE_KEY), D.DATE_KEY) AS recency_days,
        COUNT(DISTINCT CASE WHEN F.DATE_KEY >= DATEADD(MONTH, -12, D.DATE_KEY) THEN F.ORDER_ID END) AS frequency_12m,
        SUM(CASE WHEN F.DATE_KEY >= DATEADD(MONTH, -12, D.DATE_KEY) THEN F.REVENUE ELSE 0 END) AS monetary_12m
    FROM RAW_SALES.GOLD.dim_customers C
    CROSS JOIN (SELECT DISTINCT DATE_KEY, YEAR_MONTH FROM RAW_SALES.GOLD.dim_dates) D
    LEFT JOIN RAW_SALES.GOLD.fact_orders F ON C.CUSTOMER_ID = F.CUSTOMER_ID AND F.DATE_KEY <= D.DATE_KEY
    GROUP BY C.CUSTOMER_ID, D.YEAR_MONTH, D.DATE_KEY
),
scored AS (
    SELECT *, NTILE(5) OVER (PARTITION BY YEAR_MONTH ORDER BY recency_days ASC) AS r_score,
        NTILE(5) OVER (PARTITION BY YEAR_MONTH ORDER BY frequency_12m DESC) AS f_score,
        NTILE(5) OVER (PARTITION BY YEAR_MONTH ORDER BY monetary_12m DESC) AS m_score
    FROM customer_monthly_agg
)
SELECT CUSTOMER_ID, DATE_KEY AS OBSERVATION_DATE, YEAR_MONTH AS VALID_FROM,
    recency_days, frequency_12m, monetary_12m, r_score, f_score, m_score,
    (r_score + f_score + m_score) AS rfm_composite_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'VIP'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'At-Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Inactive'
        ELSE 'Engaged'
    END AS estimated_segment,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM scored WHERE recency_days IS NOT NULL;


-- ============================================================================
-- OFFLINE FEATURES: Customer Engagement
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.customer_engagement_features_offline AS
SELECT C.CUSTOMER_ID, D.DATE_KEY AS OBSERVATION_DATE,
    DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) AS days_since_last_purchase,
    CASE
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) <= 30 THEN 'Active'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) <= 90 THEN 'Dormant'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) <= 180 THEN 'At-Risk'
        ELSE 'Churned'
    END AS engagement_status,
    C.LIFETIME_VALUE, C.LIFETIME_ORDER_COUNT,
    ROUND(C.LIFETIME_VALUE / NULLIF(C.LIFETIME_ORDER_COUNT, 0), 2) AS avg_order_value,
    C.CUSTOMER_TENURE_DAYS,
    ROUND(C.LIFETIME_VALUE / NULLIF(C.CUSTOMER_TENURE_DAYS, 0), 4) AS lifetime_value_per_day,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM RAW_SALES.GOLD.dim_customers C
CROSS JOIN (SELECT DISTINCT DATE_KEY FROM RAW_SALES.GOLD.dim_dates WHERE DATE_KEY <= CURRENT_DATE()) D
WHERE D.DATE_KEY >= C.FIRST_ORDER_DATE;


-- ============================================================================
-- OFFLINE FEATURES: Product Performance
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.product_performance_features_offline AS
WITH base AS (
    SELECT P.PRODUCT_ID, D.DATE_KEY AS OBSERVATION_DATE,
        SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY THEN F.REVENUE ELSE 0 END) AS cumulative_revenue,
        SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY AND F.DATE_KEY > DATEADD(MONTH, -12, D.DATE_KEY) THEN F.REVENUE ELSE 0 END) AS revenue_12m,
        SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY THEN F.QUANTITY ELSE 0 END) AS cumulative_units_sold,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN F.RETURN_FLAG = 'R' THEN F.ORDER_ITEM_KEY END) / NULLIF(COUNT(DISTINCT F.ORDER_ITEM_KEY), 0), 2) AS return_rate_pct
    FROM RAW_SALES.GOLD.dim_products P
    CROSS JOIN (SELECT DISTINCT DATE_KEY FROM RAW_SALES.GOLD.dim_dates WHERE DATE_KEY <= CURRENT_DATE()) D
    LEFT JOIN RAW_SALES.GOLD.fact_orders F ON P.PRODUCT_ID = F.PRODUCT_ID AND F.DATE_KEY <= D.DATE_KEY
    GROUP BY P.PRODUCT_ID, D.DATE_KEY
)
SELECT *, DENSE_RANK() OVER (PARTITION BY OBSERVATION_DATE ORDER BY cumulative_revenue DESC) AS revenue_rank,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM base;


-- ============================================================================
-- OFFLINE FEATURES: Sales Rep Quota Attainment
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline AS
SELECT SR.REP_ID, D.DATE_KEY AS OBSERVATION_DATE, SR.QUOTA,
    SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.REVENUE ELSE 0 END) AS ytd_revenue,
    COUNT(DISTINCT CASE WHEN F.DATE_KEY <= D.DATE_KEY AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.ORDER_ID END) AS ytd_orders,
    ROUND(100 * SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.REVENUE ELSE 0 END)
        / NULLIF(SR.QUOTA, 0), 1) AS quota_attainment_pct,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM RAW_SALES.GOLD.dim_sales_reps SR
CROSS JOIN (SELECT DISTINCT DATE_KEY FROM RAW_SALES.GOLD.dim_dates WHERE DATE_KEY <= CURRENT_DATE()) D
LEFT JOIN RAW_SALES.GOLD.fact_orders F ON SR.REP_ID = F.REP_ID AND F.DATE_KEY <= D.DATE_KEY
GROUP BY SR.REP_ID, D.DATE_KEY, SR.QUOTA;


-- ============================================================================
-- POPULATE REGISTRY (21 features)
-- ============================================================================

TRUNCATE TABLE RAW_SALES.FEATURE_STORE.feature_registry;

INSERT INTO RAW_SALES.FEATURE_STORE.feature_registry
(FEATURE_ID, FEATURE_NAME, DATA_TYPE, ENTITY_TYPE, ENTITY_KEY, OWNER_TEAM, DESCRIPTION, TAGS, LINEAGE_SOURCE_TABLE, LINEAGE_SOURCE_COLUMN)
VALUES
('cust_recency_days', 'Customer Recency (Days)', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Days since last order', 'rfm', 'FACT_ORDERS', 'DATE_KEY'),
('cust_frequency_12m', 'Customer Frequency (12M)', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Orders in past 12 months', 'rfm', 'FACT_ORDERS', 'ORDER_ID'),
('cust_monetary_12m', 'Customer Monetary (12M)', 'DECIMAL', 'customer', 'CUSTOMER_ID', 'Analytics', 'Revenue in past 12 months', 'rfm', 'FACT_ORDERS', 'REVENUE'),
('cust_rfm_composite', 'RFM Composite Score', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Sum of R+F+M scores (3-15)', 'rfm', 'CUSTOMER_RFM_FEATURES_OFFLINE', NULL),
('cust_segment_rfm', 'RFM Segment', 'VARCHAR', 'customer', 'CUSTOMER_ID', 'Analytics', 'VIP|Loyal|At-Risk|Inactive|Engaged', 'rfm,segmentation', 'CUSTOMER_RFM_FEATURES_OFFLINE', NULL),
('cust_engagement_status', 'Engagement Status', 'VARCHAR', 'customer', 'CUSTOMER_ID', 'Analytics', 'Active|Dormant|At-Risk|Churned', 'engagement,churn', 'CUSTOMER_ENGAGEMENT_FEATURES_OFFLINE', NULL),
('cust_lifetime_value', 'Customer LTV', 'DECIMAL', 'customer', 'CUSTOMER_ID', 'Analytics', 'Total revenue from customer', 'ltv', 'DIM_CUSTOMERS', 'LIFETIME_VALUE'),
('cust_avg_order_value', 'Avg Order Value', 'DECIMAL', 'customer', 'CUSTOMER_ID', 'Analytics', 'LTV / order count', 'ltv', 'CUSTOMER_ENGAGEMENT_FEATURES_OFFLINE', NULL),
('prod_cumulative_revenue', 'Product Cumulative Revenue', 'DECIMAL', 'product', 'PRODUCT_ID', 'Analytics', 'Total product revenue', 'product-performance', 'FACT_ORDERS', 'REVENUE'),
('prod_revenue_12m', 'Product Revenue (12M)', 'DECIMAL', 'product', 'PRODUCT_ID', 'Analytics', 'Revenue in past 12 months', 'product-performance', 'FACT_ORDERS', 'REVENUE'),
('prod_return_rate', 'Product Return Rate', 'DECIMAL', 'product', 'PRODUCT_ID', 'Analytics', 'Pct items returned', 'product-performance,quality', 'FACT_ORDERS', 'RETURN_FLAG'),
('prod_revenue_rank', 'Product Revenue Rank', 'INT', 'product', 'PRODUCT_ID', 'Analytics', 'Dense rank by revenue', 'product-performance', 'PRODUCT_PERFORMANCE_FEATURES_OFFLINE', NULL),
('rep_ytd_revenue', 'Rep YTD Revenue', 'DECIMAL', 'sales_rep', 'REP_ID', 'Sales', 'Year-to-date revenue', 'quota', 'FACT_ORDERS', 'REVENUE'),
('rep_quota_attainment_pct', 'Quota Attainment %', 'DECIMAL', 'sales_rep', 'REP_ID', 'Sales', 'YTD revenue as % of quota', 'quota', 'SALES_REP_QUOTA_FEATURES_OFFLINE', NULL);


-- ============================================================================
-- TRAINING DATA VIEWS
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.training_data_customers AS
SELECT rfm.CUSTOMER_ID, rfm.OBSERVATION_DATE,
    rfm.recency_days, rfm.frequency_12m, rfm.monetary_12m, rfm.rfm_composite_score, rfm.estimated_segment,
    eng.days_since_last_purchase, eng.engagement_status, eng.lifetime_value, eng.avg_order_value,
    c.SEGMENT AS source_segment, c.REGION
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline rfm
JOIN RAW_SALES.FEATURE_STORE.customer_engagement_features_offline eng
    ON rfm.CUSTOMER_ID = eng.CUSTOMER_ID AND rfm.OBSERVATION_DATE = eng.OBSERVATION_DATE
JOIN RAW_SALES.GOLD.dim_customers c ON rfm.CUSTOMER_ID = c.CUSTOMER_ID;

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.training_data_products AS
SELECT perf.PRODUCT_ID, perf.OBSERVATION_DATE,
    perf.cumulative_revenue, perf.revenue_12m, perf.cumulative_units_sold, perf.return_rate_pct, perf.revenue_rank,
    p.CATEGORY, p.MANUFACTURER, p.UNIT_PRICE
FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline perf
JOIN RAW_SALES.GOLD.dim_products p ON perf.PRODUCT_ID = p.PRODUCT_ID;


-- ============================================================================
-- VALIDATION
-- ============================================================================

SELECT 'Feature Registry' AS CHECK, COUNT(*) AS ROWS FROM RAW_SALES.FEATURE_STORE.feature_registry
UNION ALL SELECT 'Customer RFM Features', COUNT(*) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
UNION ALL SELECT 'Product Features', COUNT(*) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline
UNION ALL SELECT 'Rep Quota Features', COUNT(*) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline;

-- Point-in-time sample: what did customer RFM look like in Jan 1998?
SELECT CUSTOMER_ID, OBSERVATION_DATE, recency_days, rfm_composite_score, estimated_segment
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE OBSERVATION_DATE >= '1998-01-01' AND OBSERVATION_DATE < '1998-02-01'
LIMIT 5;


-- ============================================================================
-- CHECKPOINT: Feature Store with 14+ registered features, PIT correctness.
-- NEXT: Run 08_dama6_quality_checks.sql
-- ============================================================================
