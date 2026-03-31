-- ============================================================================
-- ML FEATURE STORE — INTERACTIVE EXPLORATION QUERIES
-- Run these individually to explore the feature store
-- Paste into Snowflake SQL Editor and execute
-- ============================================================================

USE DATABASE RAW_SALES;

-- ============================================================================
-- 🎯 DASHBOARD 1: FEATURE STORE AT A GLANCE
-- ============================================================================

SELECT
    'FEATURE STORE SUMMARY DASHBOARD' AS dashboard_title,
    CURRENT_TIMESTAMP() AS generated_at;

-- Key Stats
SELECT
    m.METRIC,
    m.VALUE,
    CASE
        WHEN m.METRIC LIKE '%Features%' THEN '📊'
        WHEN m.METRIC LIKE '%Training%' THEN '🎓'
        WHEN m.METRIC LIKE '%Entities%' THEN '🔑'
        ELSE '📈'
    END AS icon
FROM RAW_SALES.FEATURE_STORE.v_feature_store_summary m
ORDER BY 
    CASE WHEN METRIC = 'Features Registered' THEN 0
         WHEN METRIC = 'Entities' THEN 1
         WHEN METRIC = 'Owner Teams' THEN 2
         ELSE 3
    END;


-- ============================================================================
-- 🎯 DASHBOARD 2: FEATURE CATALOG (Browse All Features)
-- ============================================================================

SELECT
    FEATURE_ID,
    FEATURE_NAME,
    '|' AS sep1,
    ENTITY_TYPE,
    '|' AS sep2,
    OWNER_TEAM,
    '|' AS sep3,
    TAGS,
    '|' AS sep4,
    CASE WHEN IS_POINT_IN_TIME THEN '✓ PIT-Correct' ELSE '✗ Not PIT' END AS point_in_time_correctness
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
ORDER BY ENTITY_TYPE, FEATURE_NAME;


-- ============================================================================
-- 🎯 DASHBOARD 3: BY ENTITY TYPE (Customer, Product, SalesRep)
-- ============================================================================

SELECT
    '📊 FEATURES BY ENTITY TYPE' AS section;

SELECT
    ENTITY_TYPE,
    FEATURE_COUNT,
    FEATURE_NAMES
FROM RAW_SALES.FEATURE_STORE.v_features_by_entity
ORDER BY FEATURE_COUNT DESC;


-- ============================================================================
-- 🎯 DASHBOARD 4: TEAM OWNERSHIP & SLA CONTACTS
-- ============================================================================

SELECT
    '👥 FEATURE OWNERSHIP & SLA CONTACTS' AS section;

SELECT
    OWNER_TEAM,
    ENTITY_TYPE,
    FEATURE_COUNT,
    FEATURE_IDS
FROM RAW_SALES.FEATURE_STORE.v_feature_ownership
ORDER BY OWNER_TEAM, FEATURE_COUNT DESC;


-- ============================================================================
-- 🎯 DASHBOARD 5: DATA QUALITY & COVERAGE
-- ============================================================================

SELECT
    '✅ FEATURE TABLE HEALTH & COVERAGE' AS section;

SELECT
    FEATURE_TABLE,
    TOTAL_ROWS AS total_feature_rows,
    UNIQUE_ENTITIES,
    DATE_SNAPSHOTS AS date_coverage,
    EARLIEST_DATE || ' → ' || LATEST_DATE AS date_range,
    DAYS_COVERED
FROM RAW_SALES.FEATURE_STORE.v_feature_table_health
ORDER BY TOTAL_ROWS DESC;


-- ============================================================================
-- 🎯 DASHBOARD 6: TRAINING DATA READY FOR ML
-- ============================================================================

SELECT
    '🎓 TRAINING DATA AVAILABILITY' AS section;

SELECT
    ENTITY_TYPE,
    TRAINING_SAMPLES,
    UNIQUE_ENTITIES,
    EARLIEST_DATE || ' → ' || LATEST_DATE AS date_range,
    SNAPSHOT_COUNT AS num_snapshots,
    DATE_RANGE_DAYS AS days_covered
FROM RAW_SALES.FEATURE_STORE.v_training_data_availability
ORDER BY TRAINING_SAMPLES DESC;


-- ============================================================================
-- 🎯 DASHBOARD 7: LINEAGE & DEPENDENCIES
-- ============================================================================

SELECT
    '🔗 FEATURE LINEAGE & DEPENDENCIES' AS section;

SELECT
    UPSTREAM,
    arrow,
    DOWNSTREAM,
    DEPENDENCY_TYPE
FROM RAW_SALES.FEATURE_STORE.v_feature_lineage_tree
LIMIT 30;


-- ============================================================================
-- 🔍 SEARCH & FILTER QUERIES (Copy-Paste Ready)
-- ============================================================================

-- Search 1: Find features with a specific tag
SELECT
    '🔎 QUERY: Find features tagged with RFM' AS query_title;

SELECT
    FEATURE_ID,
    FEATURE_NAME,
    ENTITY_TYPE,
    TAGS,
    DESCRIPTION
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE TAGS LIKE '%rfm%'
ORDER BY FEATURE_NAME;


-- Search 2: Find all customer features
SELECT
    '🔎 QUERY: All Customer Features' AS query_title;

SELECT
    FEATURE_ID,
    FEATURE_NAME,
    OWNER_TEAM,
    IS_POINT_IN_TIME,
    TAGS
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE ENTITY_TYPE = 'customer'
ORDER BY FEATURE_NAME;


-- Search 3: Features owned by specific team
SELECT
    '🔎 QUERY: Features Owned by Analytics Team' AS query_title;

SELECT
    FEATURE_ID,
    FEATURE_NAME,
    ENTITY_TYPE,
    TAGS
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE OWNER_TEAM = 'Analytics'
ORDER BY FEATURE_NAME;


-- Search 4: Point-in-time correct features
SELECT
    '🔎 QUERY: Point-in-Time Correct Features (No Data Leakage)' AS query_title;

SELECT
    FEATURE_ID,
    FEATURE_NAME,
    ENTITY_TYPE,
    OWNER_TEAM
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE IS_POINT_IN_TIME = TRUE
ORDER BY FEATURE_NAME;


-- ============================================================================
-- 📊 SAMPLE DATA PREVIEW
-- ============================================================================

SELECT '📊 SAMPLE: Customer RFM Features (Last 5 Days)' AS sample_title;

SELECT
    CUSTOMER_ID,
    OBSERVATION_DATE,
    recency_days,
    frequency_12m,
    monetary_12m,
    rfm_composite_score,
    estimated_segment
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE OBSERVATION_DATE >= CURRENT_DATE() - 5
LIMIT 20;


SELECT '📊 SAMPLE: Product Performance Features (High Revenue Products)' AS sample_title;

SELECT
    PRODUCT_ID,
    OBSERVATION_DATE,
    cumulative_revenue,
    revenue_12m,
    cumulative_units_sold,
    return_rate_pct,
    revenue_rank
FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline
WHERE cumulative_revenue > 10000000
  AND OBSERVATION_DATE = (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline)
LIMIT 20;


SELECT '📊 SAMPLE: Sales Rep Quota Features (Top Performers)' AS sample_title;

SELECT
    REP_ID,
    OBSERVATION_DATE,
    quota,
    ytd_revenue,
    quota_attainment_ratio,
    ytd_orders,
    ytd_customer_count
FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline
WHERE OBSERVATION_DATE = (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline)
ORDER BY quota_attainment_ratio DESC
LIMIT 20;


-- ============================================================================
-- 🎓 POINT-IN-TIME CORRECTNESS DEMO
-- Purpose: Show how to use features as of a specific past date
-- ============================================================================

SELECT
    '📅 DEMO: Point-in-Time Feature Lookup' AS demo_title,
    'Get customer features as they WERE on 2000-01-01 (no future data leakage)' AS demo_description;

SELECT
    CUSTOMER_ID,
    OBSERVATION_DATE,
    recency_days,
    frequency_12m,
    monetary_12m,
    rfm_composite_score,
    estimated_segment
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE OBSERVATION_DATE = '2000-01-01'
LIMIT 10;


-- ============================================================================
-- 🎯 TRAINING DATA RECIPE: Customer Churn Model
-- ============================================================================

SELECT
    '🎓 TRAINING DATA: Ready for Customer Churn Model' AS recipe_title;

SELECT
    CUSTOMER_ID,
    OBSERVATION_DATE,
    recency_days,
    frequency_12m,
    monetary_12m,
    rfm_composite_score,
    engagement_status,
    lifetime_value,
    avg_order_value
FROM RAW_SALES.FEATURE_STORE.training_data_customers
WHERE OBSERVATION_DATE >= '1999-01-01'
  AND OBSERVATION_DATE < '2000-01-01'
LIMIT 100;

-- This data can be exported for scikit-learn, XGBoost, LightGBM, etc.


-- ============================================================================
-- 🎯 TRAINING DATA RECIPE: Product Demand Forecast
-- ============================================================================

SELECT
    '🎓 TRAINING DATA: Ready for Product Demand Forecast' AS recipe_title;

SELECT
    PRODUCT_ID,
    OBSERVATION_DATE,
    revenue_12m,
    units_sold_12m,
    return_rate_pct,
    revenue_rank,
    CATEGORY,
    MANUFACTURER,
    UNIT_PRICE
FROM RAW_SALES.FEATURE_STORE.training_data_products
WHERE OBSERVATION_DATE >= '1998-01-01'
  AND OBSERVATION_DATE < '2000-01-01'
ORDER BY PRODUCT_ID, OBSERVATION_DATE
LIMIT 100;


-- ============================================================================
-- 🔧 ADMIN: Feature Store Health Check
-- ============================================================================

SELECT
    '🔧 FEATURE STORE HEALTH CHECK' AS admin_title;

-- Check 1: All tables exist?
SELECT
    'CHECK' || ' ' || table_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'FEATURE_STORE'
          AND TABLE_CATALOG = 'RAW_SALES'
          AND TABLE_NAME = split_part(check_name, ' ', 2)
    ) THEN '✓ EXISTS' ELSE '✗ MISSING' END AS status
FROM (
    SELECT 'TABLE customer_rfm_features_offline' as check_name
    UNION ALL SELECT 'TABLE customer_engagement_features_offline'
    UNION ALL SELECT 'TABLE product_performance_features_offline'
    UNION ALL SELECT 'TABLE sales_rep_quota_features_offline'
    UNION ALL SELECT 'TABLE feature_registry'
    UNION ALL SELECT 'TABLE feature_versions'
    UNION ALL SELECT 'TABLE feature_lineage'
);

-- Check 2: Feature count in registry
SELECT
    'Feature Registry Population',
    COUNT(*) AS feature_count,
    CASE WHEN COUNT(*) >= 20 THEN '✓ HEALTHY' ELSE '✗ LOW' END AS status
FROM RAW_SALES.FEATURE_STORE.feature_registry;

-- Check 3: Training data availability
SELECT
    'Training Data Volume',
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.training_data_customers) +
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.training_data_products) AS total_training_rows,
    CASE WHEN total_training_rows > 100000 THEN '✓ HEALTHY' ELSE '✗ LOW' END AS status;

-- Check 4: Latest data freshness
SELECT
    'Latest Snapshot Date',
    MAX(last_date) AS most_recent_data,
    DATEDIFF(DAY, MAX(last_date), CURRENT_DATE()) AS days_old
FROM (
    SELECT MAX(OBSERVATION_DATE) AS last_date FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
    UNION ALL
    SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline
);


-- ============================================================================
-- 📋 DOCUMENTATION & LINKS (Copy-Paste Friendly)
-- ============================================================================

SELECT
    'Below are key resources for the ML Feature Store:' as resource_title;

SELECT
    'Resource Name' AS name,
    'Location' AS location,
    'Description' AS description
FROM (VALUES
    ('ML Feature Store README', 'ML_FEATURE_STORE_README.md', 'Overview & quick start'),
    ('Feature Store Summary', 'FEATURE_STORE_SUMMARY.md', 'Architecture & catalog'),
    ('Feature Store Guide', 'FEATURE_STORE_GUIDE.md', 'Technical deep dive'),
    ('Quick Start (5 Recipes)', 'FEATURE_STORE_QUICKSTART.md', 'For data scientists'),
    ('Architecture Diagram', 'FEATURE_STORE_ARCHITECTURE.md', 'Integration with challenge'),
    ('Python API', 'feature_store.py', 'Programmatic access'),
    ('SQL Implementation', '06_feature_store_ml.sql', 'Feature store schema'),
    ('Exploration Views', '07_feature_store_explore.sql', 'Snowflake views & queries')
) AS docs;
