-- ============================================================================
-- ML FEATURE STORE — EXPLORATION & MONITORING VIEWS
-- Purpose: Create queryable views and queries to explore the entire feature store
--          within Snowflake (no Python needed)
-- Run after: 06_feature_store_ml.sql
-- ============================================================================

USE DATABASE RAW_SALES;
USE SCHEMA FEATURE_STORE;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- VIEW 1: FEATURE REGISTRY BROWSER
-- Purpose: Searchable feature catalog with all metadata
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_feature_registry_browser AS
SELECT
    FEATURE_ID,
    FEATURE_NAME,
    ENTITY_TYPE,
    DATA_TYPE,
    OWNER_TEAM,
    IS_POINT_IN_TIME,
    OFFLINE_ENABLED,
    ONLINE_ENABLED,
    TAGS,
    DESCRIPTION,
    LINEAGE_SOURCE_TABLE,
    VERSION,
    CREATED_AT
FROM RAW_SALES.FEATURE_STORE.feature_registry
ORDER BY ENTITY_TYPE, FEATURE_NAME;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_feature_registry_browser IS
    'Feature Registry: Browse all 21 available features with metadata, ownership, versioning.';


-- ============================================================================
-- VIEW 2: FEATURE LINEAGE TREE
-- Purpose: Show feature dependencies in a readable format
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_feature_lineage_tree AS
WITH lineage_tree AS (
    SELECT
        COALESCE(UPSTREAM_FEATURE_ID, UPSTREAM_TABLE_NAME, 'SOURCE') AS UPSTREAM,
        DOWNSTREAM_FEATURE_ID AS DOWNSTREAM,
        DEPENDENCY_TYPE,
        CREATED_AT
    FROM RAW_SALES.FEATURE_STORE.feature_lineage
)
SELECT
    UPSTREAM,
    ' → ' AS arrow,
    DOWNSTREAM,
    DEPENDENCY_TYPE,
    CREATED_AT
FROM lineage_tree
ORDER BY DOWNSTREAM, UPSTREAM;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_feature_lineage_tree IS
    'Feature Lineage: Dependency graph showing which features depend on which sources or other features.';


-- ============================================================================
-- VIEW 3: FEATURE OWNERSHIP & TEAM RESPONSIBILITY
-- Purpose: Who owns which features, contact point for SLA issues
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_feature_ownership AS
SELECT
    OWNER_TEAM,
    ENTITY_TYPE,
    COUNT(*) AS FEATURE_COUNT,
    LISTAGG(FEATURE_ID, ', ') WITHIN GROUP (ORDER BY FEATURE_ID) AS FEATURE_IDS,
    LISTAGG(FEATURE_NAME, ' | ') WITHIN GROUP (ORDER BY FEATURE_NAME) AS FEATURE_NAMES
FROM RAW_SALES.FEATURE_STORE.feature_registry
GROUP BY OWNER_TEAM, ENTITY_TYPE
ORDER BY OWNER_TEAM, ENTITY_TYPE;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_feature_ownership IS
    'Feature Ownership: SLA contacts and accountability by team and entity type.';


-- ============================================================================
-- VIEW 4: FEATURE DATA QUALITY & COVERAGE
-- Purpose: Row counts, date ranges, null percentages for each feature table
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_feature_table_health AS
SELECT
    'customer_rfm_features_offline' AS FEATURE_TABLE,
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline) AS TOTAL_ROWS,
    (SELECT COUNT(DISTINCT CUSTOMER_ID) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline) AS UNIQUE_ENTITIES,
    (SELECT COUNT(DISTINCT OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline) AS DATE_SNAPSHOTS,
    (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline) AS EARLIEST_DATE,
    (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline) AS LATEST_DATE,
    DATEDIFF(DAY, 
        (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline),
        (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline)
    ) AS DAYS_COVERED
UNION ALL
SELECT
    'customer_engagement_features_offline',
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.customer_engagement_features_offline),
    (SELECT COUNT(DISTINCT CUSTOMER_ID) FROM RAW_SALES.FEATURE_STORE.customer_engagement_features_offline),
    (SELECT COUNT(DISTINCT OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_engagement_features_offline),
    (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_engagement_features_offline),
    (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_engagement_features_offline),
    DATEDIFF(DAY,
        (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_engagement_features_offline),
        (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_engagement_features_offline)
    )
UNION ALL
SELECT
    'product_performance_features_offline',
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline),
    (SELECT COUNT(DISTINCT PRODUCT_ID) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline),
    (SELECT COUNT(DISTINCT OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline),
    (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline),
    (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline),
    DATEDIFF(DAY,
        (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline),
        (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline)
    )
UNION ALL
SELECT
    'sales_rep_quota_features_offline',
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline),
    (SELECT COUNT(DISTINCT REP_ID) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline),
    (SELECT COUNT(DISTINCT OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline),
    (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline),
    (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline),
    DATEDIFF(DAY,
        (SELECT MIN(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline),
        (SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline)
    );

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_feature_table_health IS
    'Feature Table Health: Row counts, entity count, date coverage, and spans for all offline store tables.';


-- ============================================================================
-- VIEW 5: FEATURES BY ENTITY TYPE & TAGS
-- Purpose: Discover features by entity, searchable by tags
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_features_by_entity AS
SELECT
    ENTITY_TYPE,
    COUNT(*) AS FEATURE_COUNT,
    LISTAGG(DISTINCT TAGS, ' | ') WITHIN GROUP (ORDER BY TAGS) AS ALL_TAGS,
    LISTAGG(FEATURE_ID, ', ') WITHIN GROUP (ORDER BY FEATURE_ID) AS FEATURE_IDS,
    LISTAGG(FEATURE_NAME, ' → ') WITHIN GROUP (ORDER BY FEATURE_NAME) AS FEATURE_NAMES
FROM RAW_SALES.FEATURE_STORE.feature_registry
GROUP BY ENTITY_TYPE
ORDER BY ENTITY_TYPE;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_features_by_entity IS
    'Features by Entity: Organized view of features grouped by entity type (customer, product, sales rep).';


-- ============================================================================
-- VIEW 6: TRAINING DATA AVAILABILITY
-- Purpose: Show which training data is available and ready
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_training_data_availability AS
SELECT
    'customer' AS ENTITY_TYPE,
    COUNT(*) AS TRAINING_SAMPLES,
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_ENTITIES,
    MIN(OBSERVATION_DATE) AS EARLIEST_DATE,
    MAX(OBSERVATION_DATE) AS LATEST_DATE,
    DATEDIFF(DAY, MIN(OBSERVATION_DATE), MAX(OBSERVATION_DATE)) AS DATE_RANGE_DAYS,
    COUNT(DISTINCT OBSERVATION_DATE) AS SNAPSHOT_COUNT,
    ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.training_data_customers), 0), 1) AS PCT_OF_TOTAL
FROM RAW_SALES.FEATURE_STORE.training_data_customers
UNION ALL
SELECT
    'product',
    COUNT(*),
    COUNT(DISTINCT PRODUCT_ID),
    MIN(OBSERVATION_DATE),
    MAX(OBSERVATION_DATE),
    DATEDIFF(DAY, MIN(OBSERVATION_DATE), MAX(OBSERVATION_DATE)),
    COUNT(DISTINCT OBSERVATION_DATE),
    ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.training_data_products), 0), 1)
FROM RAW_SALES.FEATURE_STORE.training_data_products;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_training_data_availability IS
    'Training Data Availability: Shows volume and date coverage available for ML training by entity type.';


-- ============================================================================
-- VIEW 7: FEATURE VERSION HISTORY (AUDIT TRAIL)
-- Purpose: Track all feature schema changes over time
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_feature_version_history AS
SELECT
    FV.FEATURE_ID,
    FR.FEATURE_NAME,
    FV.VERSION_NUMBER,
    FV.IS_ACTIVE,
    FV.DEPLOYMENT_TIMESTAMP,
    FV.CREATED_BY,
    FV.CHANGE_REASON
FROM RAW_SALES.FEATURE_STORE.feature_versions FV
LEFT JOIN RAW_SALES.FEATURE_STORE.feature_registry FR
    ON FV.FEATURE_ID = FR.FEATURE_ID
ORDER BY FV.FEATURE_ID, FV.VERSION_NUMBER DESC;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_feature_version_history IS
    'Feature Version History: Audit trail showing all schema changes, deployments, and rollbacks per feature.';


-- ============================================================================
-- VIEW 8: FEATURE STORE SUMMARY DASHBOARD
-- Purpose: High-level metrics for all feature store objects
-- ============================================================================

CREATE OR REPLACE VIEW RAW_SALES.FEATURE_STORE.v_feature_store_summary AS
WITH metrics AS (
    SELECT
        'Features Registered' AS METRIC,
        CAST(COUNT(*) AS VARCHAR) AS VALUE
    FROM RAW_SALES.FEATURE_STORE.feature_registry
    UNION ALL
    SELECT 'Entities', CAST(COUNT(DISTINCT ENTITY_TYPE) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_registry
    UNION ALL
    SELECT 'Owner Teams', CAST(COUNT(DISTINCT OWNER_TEAM) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_registry
    UNION ALL
    SELECT 'Customer Features', CAST(COUNT(*) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_registry WHERE ENTITY_TYPE = 'customer'
    UNION ALL
    SELECT 'Product Features', CAST(COUNT(*) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_registry WHERE ENTITY_TYPE = 'product'
    UNION ALL
    SELECT 'SalesRep Features', CAST(COUNT(*) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_registry WHERE ENTITY_TYPE = 'sales_rep'
    UNION ALL
    SELECT 'PIT-Correct Features', CAST(COUNT(*) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_registry WHERE IS_POINT_IN_TIME = TRUE
    UNION ALL
    SELECT 'Feature Versions', CAST(COUNT(DISTINCT VERSION_NUMBER) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_versions
    UNION ALL
    SELECT 'Lineage Dependencies', CAST(COUNT(*) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.feature_lineage
    UNION ALL
    SELECT 'Customer Training Rows', CAST(COUNT(*) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.training_data_customers
    UNION ALL
    SELECT 'Product Training Rows', CAST(COUNT(*) AS VARCHAR) FROM RAW_SALES.FEATURE_STORE.training_data_products
)
SELECT * FROM metrics;

COMMENT ON VIEW RAW_SALES.FEATURE_STORE.v_feature_store_summary IS
    'Feature Store Summary: Key metrics and counts for dashboard/monitoring.';


-- ============================================================================
-- QUICK QUERIES FOR EXPLORATION
-- ============================================================================

-- Query 1: Browse All Features
SELECT
    '--- QUERY 1: BROWSE ALL FEATURES ---' AS query_name;

SELECT
    FEATURE_ID,
    FEATURE_NAME,
    ENTITY_TYPE,
    OWNER_TEAM,
    IS_POINT_IN_TIME,
    TAGS,
    DESCRIPTION
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
ORDER BY ENTITY_TYPE, FEATURE_NAME;


-- Query 2: Features by Owner Team
SELECT
    '--- QUERY 2: OWNERSHIP & TEAM RESPONSIBILITY ---' AS query_name;

SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_ownership
ORDER BY OWNER_TEAM;


-- Query 3: Feature Store Health
SELECT
    '--- QUERY 3: FEATURE TABLE HEALTH ---' AS query_name;

SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_table_health;


-- Query 4: Training Data Available
SELECT
    '--- QUERY 4: TRAINING DATA AVAILABILITY ---' AS query_name;

SELECT * FROM RAW_SALES.FEATURE_STORE.v_training_data_availability;


-- Query 5: Feature Store Summary
SELECT
    '--- QUERY 5: FEATURE STORE SUMMARY ---' AS query_name;

SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_store_summary;


-- Query 6: Feature Dependencies
SELECT
    '--- QUERY 6: FEATURE LINEAGE / DEPENDENCIES ---' AS query_name;

SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_lineage_tree;


-- Query 7: Feature Version History (Audit)
SELECT
    '--- QUERY 7: VERSION HISTORY (AUDIT TRAIL) ---' AS query_name;

SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_version_history
LIMIT 20;


-- ============================================================================
-- UTILITY QUERIES (COPY-PASTE READY)
-- ============================================================================

-- Find features by tag
SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE TAGS LIKE '%rfm%' OR TAGS LIKE '%quota_attainment%';

-- Find features for a specific entity
SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE ENTITY_TYPE = 'customer';

-- Show feature owner contact points
SELECT DISTINCT OWNER_TEAM, COUNT(*) as feature_count
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
GROUP BY OWNER_TEAM
ORDER BY feature_count DESC;

-- Check point-in-time correctness (should be all TRUE for offline store)
SELECT 
    IS_POINT_IN_TIME,
    COUNT(*) as feature_count
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
GROUP BY IS_POINT_IN_TIME;

-- Size estimation for each entity
SELECT
    ENTITY_TYPE,
    COUNT(DISTINCT FEATURE_ID) as feature_count,
    LISTAGG(FEATURE_ID, ', ') WITHIN GROUP (ORDER BY FEATURE_ID) as features
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
GROUP BY ENTITY_TYPE
ORDER BY ENTITY_TYPE;


-- ============================================================================
-- ADVANCED: FEATURE IMPACT ANALYSIS
-- Purpose: If a source table changes, which features are affected?
-- ============================================================================

-- Example: If FACT_ORDERS is corrupted, what features are affected?
SELECT
    'Impact Analysis: If FACT_ORDERS is updated' AS analysis_title;

WITH affected_features AS (
    SELECT DISTINCT DOWNSTREAM_FEATURE_ID
    FROM RAW_SALES.FEATURE_STORE.feature_lineage
    WHERE UPSTREAM_TABLE_NAME = 'FACT_ORDERS'
       OR UPSTREAM_FEATURE_ID IN (
           SELECT DISTINCT DOWNSTREAM_FEATURE_ID
           FROM RAW_SALES.FEATURE_STORE.feature_lineage
           WHERE UPSTREAM_TABLE_NAME = 'FACT_ORDERS'
       )
)
SELECT
    fr.FEATURE_ID,
    fr.FEATURE_NAME,
    fr.OWNER_TEAM,
    fr.ENTITY_TYPE,
    'NEEDS RECOMPUTE' as action
FROM RAW_SALES.FEATURE_STORE.feature_registry fr
WHERE fr.FEATURE_ID IN (SELECT * FROM affected_features)
ORDER BY fr.OWNER_TEAM, fr.FEATURE_NAME;


-- ============================================================================
-- ACCEPTANCE VERIFICATION
-- ============================================================================

SELECT
    '✓ Feature Store Views Created Successfully' AS status,
    CURRENT_TIMESTAMP() AS created_at;

SELECT 'Available Views:' AS view_list;
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'FEATURE_STORE'
  AND TABLE_CATALOG = 'RAW_SALES'
  AND TABLE_NAME LIKE 'V_%'
ORDER BY TABLE_NAME;
