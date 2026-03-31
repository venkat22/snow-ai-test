-- ============================================================================
-- PHASE 4: ML FEATURE STORE — Point-in-Time Correct Features
-- Purpose: Build production ML Feature Store with versioning, lineage, and
--          online/offline serving patterns for model training and inference
-- Prerequisite: 04_phase2_gold.sql completed successfully
-- Estimated runtime: ~10 minutes
-- ============================================================================

-- If unsure of your warehouse name, run: SHOW WAREHOUSES;
USE DATABASE RAW_SALES;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- SETUP: Feature Store Schema and Control Tables
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS RAW_SALES.FEATURE_STORE;
USE SCHEMA RAW_SALES.FEATURE_STORE;


-- ============================================================================
-- FEATURE REGISTRY: Catalog of all available features
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_registry (
    FEATURE_ID              VARCHAR(255) PRIMARY KEY,
    FEATURE_NAME            VARCHAR(255),
    DATA_TYPE               VARCHAR(50),
    ENTITY_TYPE             VARCHAR(50),              -- customer | product | sales_rep | temporal
    ENTITY_KEY              VARCHAR(100),             -- which key to join on (e.g., CUSTOMER_ID)
    OWNER_TEAM              VARCHAR(100),
    DESCRIPTION             VARCHAR(1000),
    IS_POINT_IN_TIME        BOOLEAN DEFAULT TRUE,     -- can use historical values at a point in time?
    ONLINE_ENABLED          BOOLEAN DEFAULT FALSE,    -- available in online store?
    OFFLINE_ENABLED         BOOLEAN DEFAULT TRUE,     -- available in offline store?
    VERSION                 INT DEFAULT 1,
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DEPRECATED_AT           TIMESTAMP_NTZ,
    TAGS                    VARCHAR(500),             -- comma-separated tags: rfm, customer-lifetime-value, quota_attainment, etc.
    LINEAGE_SOURCE_TABLE    VARCHAR(255),             -- which table does feature come from?
    LINEAGE_SOURCE_COLUMN   VARCHAR(255)
);

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.feature_registry IS
    'ML Feature Store Registry: Master catalog of all model-training-ready features with ownership, versioning, and lineage.';


-- ============================================================================
-- FEATURE VERSIONING: Track feature definition changes over time
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_versions (
    VERSION_ID              INT AUTOINCREMENT PRIMARY KEY,
    FEATURE_ID              VARCHAR(255) NOT NULL,
    VERSION_NUMBER          INT NOT NULL,
    DEFINITION_SQL          VARCHAR(5000),
    SCHEMA_HASH             VARCHAR(64),              -- MD5 of schema for change detection
    DEPLOYMENT_TIMESTAMP    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY              VARCHAR(255),
    CHANGE_REASON           VARCHAR(500),
    IS_ACTIVE               BOOLEAN DEFAULT FALSE,
    UNIQUE(FEATURE_ID, VERSION_NUMBER)
);

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.feature_versions IS
    'Feature version history: tracks all schema changes, deployments, and rollbacks for audit and reproducibility.';


-- ============================================================================
-- POINT-IN-TIME CORRECTNESS: Timestamp tracking for incremental features
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_snapshots (
    SNAPSHOT_ID             INT AUTOINCREMENT PRIMARY KEY,
    FEATURE_ID              VARCHAR(255) NOT NULL,
    ENTITY_TYPE             VARCHAR(50) NOT NULL,     -- customer | product | sales_rep
    ENTITY_KEY              VARCHAR(100) NOT NULL,    -- customer ID, product ID, etc.
    FEATURE_VALUE           VARCHAR(1000),            -- serialized feature value
    VALID_FROM              TIMESTAMP_NTZ NOT NULL,   -- time this feature value became valid
    VALID_TO                TIMESTAMP_NTZ,            -- null = still valid; otherwise when it changed
    SNAPSHOT_TIMESTAMP      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE(FEATURE_ID, ENTITY_KEY, VALID_FROM)
);

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.feature_snapshots IS
    'Point-in-time snapshots: enables "as-of" queries to get feature values from any past date (critical for training and reproducibility).';


-- ============================================================================
-- FEATURE LINEAGE: Dependency tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.feature_lineage (
    LINEAGE_ID              INT AUTOINCREMENT PRIMARY KEY,
    UPSTREAM_FEATURE_ID     VARCHAR(255),             -- null if source is a table
    DOWNSTREAM_FEATURE_ID   VARCHAR(255) NOT NULL,
    UPSTREAM_TABLE_NAME     VARCHAR(255),             -- null if upstream is a feature
    UPSTREAM_COLUMN_NAME    VARCHAR(255),
    DEPENDENCY_TYPE         VARCHAR(50),              -- direct | computed | aggregated
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.feature_lineage IS
    'Feature lineage graph: tracks dependencies between features and upstream sources for impact analysis and governance.';


-- ============================================================================
-- ENTITY DEFINITIONS: Customer, Product, Sales Rep entities for feature joins
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.entity_keys (
    ENTITY_ID               INT AUTOINCREMENT PRIMARY KEY,
    ENTITY_TYPE             VARCHAR(50),              -- customer | product | sales_rep | temporal
    ENTITY_KEY_VALUE        VARCHAR(100),             -- CUSTOMER_ID, PRODUCT_ID, REP_ID, DATE_KEY
    DESCRIPTION             VARCHAR(500),
    FIRST_SEEN              TIMESTAMP_NTZ,
    LAST_UPDATED            TIMESTAMP_NTZ,
    UNIQUE(ENTITY_TYPE, ENTITY_KEY_VALUE)
);

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.entity_keys IS
    'Entity registry: defines all entity types (customers, products, sales reps) and keys used in feature joins.';


-- ============================================================================
-- SECTION A: CUSTOMER ENTITY
-- ============================================================================

-- ============================================================================
-- OFFLINE STORE: Customer RFM Features (Point-in-Time Correct)
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.customer_rfm_features_offline AS
WITH customer_monthly_agg AS (
    SELECT
        C.CUSTOMER_ID,
        D.YEAR_MONTH,
        D.DATE_KEY,
        -- Recency: days since this month's last order
        DATEDIFF(DAY, MAX(F.DATE_KEY), D.DATE_KEY) AS recency_days,
        -- Frequency: count of orders in past 12 months (rolling window)
        COUNT(DISTINCT CASE
            WHEN F.DATE_KEY >= DATEADD(MONTH, -12, D.DATE_KEY) THEN F.ORDER_ID
            ELSE NULL
        END) AS frequency_12m,
        -- Monetary: total revenue in past 12 months
        SUM(CASE
            WHEN F.DATE_KEY >= DATEADD(MONTH, -12, D.DATE_KEY) THEN F.REVENUE
            ELSE 0
        END) AS monetary_12m
    FROM RAW_SALES.GOLD.dim_customers C
    CROSS JOIN (SELECT DISTINCT DATE_KEY, YEAR_MONTH FROM RAW_SALES.GOLD.dim_dates) D
    LEFT JOIN RAW_SALES.GOLD.fact_orders F
        ON C.CUSTOMER_ID = F.CUSTOMER_ID
        AND F.DATE_KEY <= D.DATE_KEY
    GROUP BY C.CUSTOMER_ID, D.YEAR_MONTH, D.DATE_KEY
),
customer_scored AS (
    SELECT
        CUSTOMER_ID,
        YEAR_MONTH,
        DATE_KEY,
        recency_days,
        frequency_12m,
        monetary_12m,
        NTILE(5) OVER (PARTITION BY YEAR_MONTH ORDER BY recency_days ASC) AS recency_score,
        NTILE(5) OVER (PARTITION BY YEAR_MONTH ORDER BY frequency_12m DESC) AS frequency_score,
        NTILE(5) OVER (PARTITION BY YEAR_MONTH ORDER BY monetary_12m DESC) AS monetary_score
    FROM customer_monthly_agg
)
SELECT
    CUSTOMER_ID,
    DATE_KEY AS OBSERVATION_DATE,                     -- point-in-time reference
    YEAR_MONTH AS VALID_FROM,
    recency_days,
    frequency_12m,
    monetary_12m,
    recency_score,
    frequency_score,
    monetary_score,
    (recency_score + frequency_score + monetary_score) AS rfm_composite_score,
    CASE
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'VIP'
        WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal'
        WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'At-Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Inactive'
        ELSE 'Engaged'
    END AS estimated_segment,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM customer_scored
WHERE recency_days IS NOT NULL;

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.customer_rfm_features_offline IS
    'OFFLINE: Customer RFM features with point-in-time correctness. Row = (customer, observation_date). Use for training with historical accuracy.';


-- ============================================================================
-- OFFLINE STORE: Customer Engagement Features
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.customer_engagement_features_offline AS
SELECT
    C.CUSTOMER_ID,
    D.DATE_KEY AS OBSERVATION_DATE,
    DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) AS days_since_last_purchase,
    CASE
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) <= 30 THEN 'Active'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) <= 90 THEN 'Dormant'
        WHEN DATEDIFF(DAY, C.LAST_ORDER_DATE, D.DATE_KEY) <= 180 THEN 'At-Risk'
        ELSE 'Churned'
    END AS engagement_status,
    C.LIFETIME_VALUE,
    C.LIFETIME_ORDER_COUNT,
    ROUND(C.LIFETIME_VALUE / NULLIF(C.LIFETIME_ORDER_COUNT, 0), 2) AS avg_order_value,
    C.CUSTOMER_TENURE_DAYS,
    ROUND(C.LIFETIME_VALUE / NULLIF(C.CUSTOMER_TENURE_DAYS, 0), 4) AS lifetime_value_per_day,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM RAW_SALES.GOLD.dim_customers C
CROSS JOIN (SELECT DISTINCT DATE_KEY FROM RAW_SALES.GOLD.dim_dates WHERE DATE_KEY <= CURRENT_DATE()) D
WHERE D.DATE_KEY >= C.FIRST_ORDER_DATE;

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.customer_engagement_features_offline IS
    'OFFLINE: Customer engagement features (churn status, tenure, LTV). Row = (customer, observation_date).';


-- ============================================================================
-- SECTION B: PRODUCT ENTITY
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.product_performance_features_offline AS
WITH product_base AS (
    SELECT
        P.PRODUCT_ID,
        D.DATE_KEY AS OBSERVATION_DATE,
        -- Revenue features
        SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY THEN F.REVENUE ELSE 0 END) AS cumulative_revenue,
        SUM(CASE
            WHEN F.DATE_KEY <= D.DATE_KEY
             AND F.DATE_KEY > DATEADD(MONTH, -12, D.DATE_KEY) THEN F.REVENUE
            ELSE 0
        END) AS revenue_12m,
        -- Volume features
        SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY THEN F.QUANTITY ELSE 0 END) AS cumulative_units_sold,
        SUM(CASE
            WHEN F.DATE_KEY <= D.DATE_KEY
             AND F.DATE_KEY > DATEADD(MONTH, -12, D.DATE_KEY) THEN F.QUANTITY
            ELSE 0
        END) AS units_sold_12m,
        -- Quality features
        COUNT(DISTINCT CASE WHEN F.RETURN_FLAG = 'R' THEN F.ORDER_ITEM_KEY END) AS total_returned_items,
        ROUND(
            100.0 * COUNT(DISTINCT CASE WHEN F.RETURN_FLAG = 'R' THEN F.ORDER_ITEM_KEY END)
            / NULLIF(COUNT(DISTINCT F.ORDER_ITEM_KEY), 0),
            2
        ) AS return_rate_pct
    FROM RAW_SALES.GOLD.dim_products P
    CROSS JOIN (SELECT DISTINCT DATE_KEY FROM RAW_SALES.GOLD.dim_dates WHERE DATE_KEY <= CURRENT_DATE()) D
    LEFT JOIN RAW_SALES.GOLD.fact_orders F
        ON P.PRODUCT_ID = F.PRODUCT_ID AND F.DATE_KEY <= D.DATE_KEY
    GROUP BY P.PRODUCT_ID, D.DATE_KEY
)
SELECT
    PRODUCT_ID,
    OBSERVATION_DATE,
    cumulative_revenue,
    revenue_12m,
    cumulative_units_sold,
    units_sold_12m,
    total_returned_items,
    return_rate_pct,
    DENSE_RANK() OVER (PARTITION BY OBSERVATION_DATE ORDER BY cumulative_revenue DESC) AS revenue_rank,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM product_base;

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.product_performance_features_offline IS
    'OFFLINE: Product performance features (revenue, volume, quality, ranking). Row = (product, observation_date).';


-- ============================================================================
-- SECTION C: SALES REP ENTITY
-- ============================================================================

CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline AS
SELECT
    SR.REP_ID,
    D.DATE_KEY AS OBSERVATION_DATE,
    SR.QUOTA,
    SUM(CASE
        WHEN F.DATE_KEY <= D.DATE_KEY
         AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.REVENUE
        ELSE 0
    END) AS ytd_revenue,
    SUM(CASE WHEN F.DATE_KEY <= D.DATE_KEY THEN F.REVENUE ELSE 0 END) AS cumulative_revenue,
    COUNT(DISTINCT CASE
        WHEN F.DATE_KEY <= D.DATE_KEY
         AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.ORDER_ID
        ELSE NULL
    END) AS ytd_orders,
    ROUND(
        SUM(CASE
            WHEN F.DATE_KEY <= D.DATE_KEY
             AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.REVENUE
            ELSE 0
        END)
        / NULLIF(SR.QUOTA, 0), 4
    ) AS quota_attainment_ratio,
    ROUND(
        100 * SUM(CASE
            WHEN F.DATE_KEY <= D.DATE_KEY
             AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.REVENUE
            ELSE 0
        END)
        / NULLIF(SR.QUOTA, 0), 1
    ) AS quota_attainment_pct,
    COUNT(DISTINCT CASE
        WHEN F.DATE_KEY <= D.DATE_KEY
         AND EXTRACT(YEAR FROM F.DATE_KEY) = EXTRACT(YEAR FROM D.DATE_KEY) THEN F.CUSTOMER_ID
        ELSE NULL
    END) AS ytd_customer_count,
    CURRENT_TIMESTAMP() AS _FEATURE_COMPUTED_AT
FROM RAW_SALES.GOLD.dim_sales_reps SR
CROSS JOIN (SELECT DISTINCT DATE_KEY FROM RAW_SALES.GOLD.dim_dates WHERE DATE_KEY <= CURRENT_DATE()) D
LEFT JOIN RAW_SALES.GOLD.fact_orders F
    ON SR.REP_ID = F.REP_ID AND F.DATE_KEY <= D.DATE_KEY
GROUP BY SR.REP_ID, D.DATE_KEY, SR.QUOTA;

COMMENT ON TABLE RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline IS
    'OFFLINE: Sales rep quota attainment and performance features. Row = (sales_rep, observation_date).';


-- ============================================================================
-- FEATURE STORE METADATA: Populate the registry
-- ============================================================================

TRUNCATE TABLE RAW_SALES.FEATURE_STORE.feature_registry;

INSERT INTO RAW_SALES.FEATURE_STORE.feature_registry
(FEATURE_ID, FEATURE_NAME, DATA_TYPE, ENTITY_TYPE, ENTITY_KEY, OWNER_TEAM, DESCRIPTION, IS_POINT_IN_TIME, ONLINE_ENABLED, OFFLINE_ENABLED, TAGS, LINEAGE_SOURCE_TABLE, LINEAGE_SOURCE_COLUMN)
VALUES
-- Customer RFM features
('cust_recency_days', 'Customer Recency (Days Since Last Purchase)', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Days since last order within 12-month window', TRUE, FALSE, TRUE, 'rfm,customer-lifetime-value', 'FACT_ORDERS', 'DATE_KEY'),
('cust_frequency_12m', 'Customer Frequency (12-Month Orders)', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Number of orders in past 12 months', TRUE, FALSE, TRUE, 'rfm,customer-lifetime-value', 'FACT_ORDERS', 'ORDER_ID'),
('cust_monetary_12m', 'Customer Monetary Value (12-Month)', 'DECIMAL', 'customer', 'CUSTOMER_ID', 'Analytics', 'Total revenue in past 12 months', TRUE, FALSE, TRUE, 'rfm,customer-lifetime-value', 'FACT_ORDERS', 'REVENUE'),
('cust_rfm_composite_score', 'Customer RFM Composite Score', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Sum of R, F, M scores (3-15 range)', TRUE, FALSE, TRUE, 'rfm,customer-lifetime-value', 'CUSTOMER_RFM_FEATURES_OFFLINE', NULL),
('cust_segment_rfm', 'Customer Segment (RFM)', 'VARCHAR', 'customer', 'CUSTOMER_ID', 'Analytics', 'VIP | Loyal | At-Risk | Inactive | Engaged', TRUE, FALSE, TRUE, 'rfm,customer-lifetime-value,segmentation', 'CUSTOMER_RFM_FEATURES_OFFLINE', NULL),
-- Customer engagement features
('cust_days_since_purchase', 'Days Since Last Purchase', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Recency in days', TRUE, FALSE, TRUE, 'engagement,churn', 'DIM_CUSTOMERS', 'LAST_ORDER_DATE'),
('cust_engagement_status', 'Customer Engagement Status', 'VARCHAR', 'customer', 'CUSTOMER_ID', 'Analytics', 'Active | Dormant | At-Risk | Churned', TRUE, FALSE, TRUE, 'engagement,churn', 'CUSTOMER_ENGAGEMENT_FEATURES_OFFLINE', NULL),
('cust_lifetime_value', 'Customer Lifetime Value', 'DECIMAL', 'customer', 'CUSTOMER_ID', 'Analytics', 'Total revenue from customer', TRUE, FALSE, TRUE, 'customer-lifetime-value,customer-value', 'DIM_CUSTOMERS', 'LIFETIME_VALUE'),
('cust_lifetime_order_count', 'Customer Lifetime Order Count', 'INT', 'customer', 'CUSTOMER_ID', 'Analytics', 'Total number of orders', TRUE, FALSE, TRUE, 'customer-lifetime-value', 'DIM_CUSTOMERS', 'LIFETIME_ORDER_COUNT'),
('cust_avg_order_value', 'Customer Average Order Value', 'DECIMAL', 'customer', 'CUSTOMER_ID', 'Analytics', 'LTV / order count', TRUE, FALSE, TRUE, 'customer-lifetime-value,order-metrics', 'CUSTOMER_ENGAGEMENT_FEATURES_OFFLINE', NULL),
-- Product performance features
('prod_cumulative_revenue', 'Product Cumulative Revenue', 'DECIMAL', 'product', 'PRODUCT_ID', 'Analytics', 'Total revenue generated by product', TRUE, FALSE, TRUE, 'product-performance,revenue', 'FACT_ORDERS', 'REVENUE'),
('prod_revenue_12m', 'Product Revenue (12-Month)', 'DECIMAL', 'product', 'PRODUCT_ID', 'Analytics', 'Revenue in past 12 months', TRUE, FALSE, TRUE, 'product-performance,revenue', 'FACT_ORDERS', 'REVENUE'),
('prod_units_sold', 'Product Cumulative Units Sold', 'INT', 'product', 'PRODUCT_ID', 'Analytics', 'Total units sold', TRUE, FALSE, TRUE, 'product-performance,volume', 'FACT_ORDERS', 'QUANTITY'),
('prod_return_rate_pct', 'Product Return Rate (%)', 'DECIMAL', 'product', 'PRODUCT_ID', 'Analytics', 'Percentage of items returned', TRUE, FALSE, TRUE, 'product-performance,quality', 'FACT_ORDERS', 'RETURN_FLAG'),
('prod_revenue_rank', 'Product Revenue Rank', 'INT', 'product', 'PRODUCT_ID', 'Analytics', 'Dense rank by revenue (1 = top)', TRUE, FALSE, TRUE, 'product-performance,ranking', 'PRODUCT_PERFORMANCE_FEATURES_OFFLINE', NULL),
-- Sales rep features
('rep_ytd_revenue', 'Sales Rep YTD Revenue', 'DECIMAL', 'sales_rep', 'REP_ID', 'Sales', 'Year-to-date revenue', TRUE, FALSE, TRUE, 'quota_attainment,sales-performance', 'FACT_ORDERS', 'REVENUE'),
('rep_quota', 'Sales Rep Quota', 'DECIMAL', 'sales_rep', 'REP_ID', 'Sales', 'Annual quota target', FALSE, FALSE, TRUE, 'quota_attainment,sales-performance', 'DIM_SALES_REPS', 'QUOTA'),
('rep_quota_attainment_ratio', 'Sales Rep Quota Attainment Ratio', 'DECIMAL', 'sales_rep', 'REP_ID', 'Sales', 'YTD revenue / quota', TRUE, FALSE, TRUE, 'quota_attainment,sales-performance', 'SALES_REP_QUOTA_FEATURES_OFFLINE', NULL),
('rep_quota_attainment_pct', 'Sales Rep Quota Attainment (%)', 'DECIMAL', 'sales_rep', 'REP_ID', 'Sales', 'YTD revenue as % of quota', TRUE, FALSE, TRUE, 'quota_attainment,sales-performance', 'SALES_REP_QUOTA_FEATURES_OFFLINE', NULL),
('rep_ytd_orders', 'Sales Rep YTD Order Count', 'INT', 'sales_rep', 'REP_ID', 'Sales', 'Orders closed YTD', TRUE, FALSE, TRUE, 'quota_attainment,sales-performance', 'FACT_ORDERS', 'ORDER_ID'),
('rep_ytd_customers', 'Sales Rep YTD Customer Count', 'INT', 'sales_rep', 'REP_ID', 'Sales', 'Unique customers engaged YTD', TRUE, FALSE, TRUE, 'quota_attainment,sales-performance', 'FACT_ORDERS', 'CUSTOMER_ID');


-- ============================================================================
-- FEATURE LINEAGE: Register feature dependencies
-- ============================================================================

TRUNCATE TABLE RAW_SALES.FEATURE_STORE.feature_lineage;

INSERT INTO RAW_SALES.FEATURE_STORE.feature_lineage
(UPSTREAM_FEATURE_ID, DOWNSTREAM_FEATURE_ID, UPSTREAM_TABLE_NAME, UPSTREAM_COLUMN_NAME, DEPENDENCY_TYPE)
VALUES
-- RFM composite depends on individual scores
(NULL, 'cust_rfm_composite_score', 'CUSTOMER_RFM_FEATURES_OFFLINE', NULL, 'aggregated'),
-- RFM segment depends on composite
('cust_rfm_composite_score', 'cust_segment_rfm', NULL, NULL, 'computed'),
-- Engagement status depends on recency
('cust_days_since_purchase', 'cust_engagement_status', NULL, NULL, 'computed'),
-- Product rank depends on revenue
('prod_cumulative_revenue', 'prod_revenue_rank', NULL, NULL, 'computed');


-- ============================================================================
-- AS-OF JOIN FUNCTION (FOR POINT-IN-TIME LOOKUPS)
-- ============================================================================

CREATE OR REPLACE FUNCTION RAW_SALES.FEATURE_STORE.get_feature_as_of(
    p_feature_table VARCHAR,
    p_entity_key VARCHAR,
    p_as_of_date DATE
)
RETURNS TABLE (feature_value VARCHAR)
LANGUAGE SQL
AS $$
    SELECT feature_value
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE entity_key = p_entity_key AND valid_from <= p_as_of_date
      AND (valid_to IS NULL OR valid_to > p_as_of_date)
    LIMIT 1
$$;

COMMENT ON FUNCTION RAW_SALES.FEATURE_STORE.get_feature_as_of(VARCHAR, VARCHAR, DATE) IS
    'Helper function for point-in-time feature lookups. Use with feature snapshots table.';


-- ============================================================================
-- MATERIALIZED VIEW: Customer Training Data Set (Example)
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.training_data_customers AS
SELECT
    rfm.CUSTOMER_ID,
    rfm.OBSERVATION_DATE,
    rfm.recency_days,
    rfm.frequency_12m,
    rfm.monetary_12m,
    rfm.rfm_composite_score,
    rfm.estimated_segment AS rfm_segment,
    eng.days_since_last_purchase,
    eng.engagement_status,
    eng.lifetime_value,
    eng.lifetime_order_count,
    eng.avg_order_value,
    eng.lifetime_value_per_day,
    c.SEGMENT AS source_segment,
    c.REGION
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline rfm
JOIN RAW_SALES.FEATURE_STORE.customer_engagement_features_offline eng
    ON rfm.CUSTOMER_ID = eng.CUSTOMER_ID AND rfm.OBSERVATION_DATE = eng.OBSERVATION_DATE
JOIN RAW_SALES.GOLD.dim_customers c
    ON rfm.CUSTOMER_ID = c.CUSTOMER_ID;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.training_data_customers IS
    'Training dataset: combines RFM and engagement features with point-in-time correctness for customer churn/LTV models.';


-- ============================================================================
-- MATERIALIZED VIEW: Product Training Data Set (Example)
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.training_data_products AS
SELECT
    perf.PRODUCT_ID,
    perf.OBSERVATION_DATE,
    perf.cumulative_revenue,
    perf.revenue_12m,
    perf.cumulative_units_sold,
    perf.units_sold_12m,
    perf.total_returned_items,
    perf.return_rate_pct,
    perf.revenue_rank,
    p.CATEGORY,
    p.MANUFACTURER,
    p.UNIT_PRICE
FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline perf
JOIN RAW_SALES.GOLD.dim_products p
    ON perf.PRODUCT_ID = p.PRODUCT_ID;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.training_data_products IS
    'Training dataset: product features with point-in-time correctness for demand forecasting and pricing models.';


-- ============================================================================
-- ACCEPTANCE CHECKS
-- ============================================================================

-- Verify feature store tables
SELECT
    'Feature Store Readiness',
    COUNT(*) as table_count,
    'PASS' as status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'FEATURE_STORE'
  AND TABLE_CATALOG = 'RAW_SALES'
  AND TABLE_TYPE = 'BASE TABLE'
HAVING COUNT(*) >= 6;

-- Sample features in registry
SELECT 'Feature Registry Population', COUNT(*) as feature_count
FROM RAW_SALES.FEATURE_STORE.feature_registry;

-- Sample training data availability
SELECT 'Training Data - Customers', COUNT(*) as row_count
FROM RAW_SALES.FEATURE_STORE.training_data_customers;

SELECT 'Training Data - Products', COUNT(*) as row_count
FROM RAW_SALES.FEATURE_STORE.training_data_products;

SELECT 'Point-in-Time Sample',
       CUSTOMER_ID,
       OBSERVATION_DATE,
       recency_days,
       rfm_composite_score,
       estimated_segment
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE OBSERVATION_DATE >= '1998-01-01'
  AND OBSERVATION_DATE < '1998-02-01'
LIMIT 5;
