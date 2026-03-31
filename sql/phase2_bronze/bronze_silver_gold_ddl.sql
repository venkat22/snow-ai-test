-- ============================================================================
-- Compatibility Wrapper: Bronze/Silver/Gold DDL Entry
-- ============================================================================
-- Canonical implementation moved to phase scripts:
--   02_phase2_bronze.sql
--   03_phase2_silver.sql
--   04_phase2_gold.sql
--   05_phase3_data_products.sql
--
-- This wrapper is kept so existing references do not break.
-- It returns run guidance and validates baseline context only.
-- ============================================================================

USE DATABASE RAW_SALES;
USE WAREHOUSE ANALYTICS_WH;

SELECT
  'Use canonical phase scripts in order: 02 -> 03 -> 04 -> 05' AS guidance,
  'Mandatory capabilities: Streams, Dynamic Tables, Tasks, Snowpark telemetry' AS required_capabilities,
  CURRENT_TIMESTAMP() AS checked_at;

-- Optional readiness snapshot
SELECT
  (SELECT COUNT(*) FROM RAW_SALES.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'LANDING') AS landing_tables,
  (SELECT COUNT(*) FROM RAW_SALES.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'BRONZE') AS bronze_tables,
  (SELECT COUNT(*) FROM RAW_SALES.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SILVER') AS silver_tables,
  (SELECT COUNT(*) FROM RAW_SALES.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD') AS gold_tables;