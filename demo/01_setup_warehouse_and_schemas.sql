-- ============================================================================
-- DEMO STEP 1: SETUP WAREHOUSE & SCHEMAS
-- ============================================================================
-- What this does:
--   Creates an XS warehouse, the RAW_SALES database, and 5 medallion schemas
--   (LANDING, BRONZE, SILVER, GOLD, MONITORING).
--
-- Talk track:
--   "We start by creating the compute and storage foundation. A single XS
--    warehouse auto-suspends after 60 seconds to keep costs near zero.
--    The 5 schemas map to our medallion architecture layers."
--
-- Runtime: ~10 seconds
-- Prerequisites: SYSADMIN or ACCOUNTADMIN role
-- ============================================================================

-- 1. Create a dedicated warehouse (1 credit/hr, auto-suspends when idle)
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE    = 'X-SMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT           = 'Warehouse for RAW_SALES medallion architecture project';

SHOW WAREHOUSES LIKE 'ANALYTICS_WH';


-- 2. Create database and medallion schemas
CREATE DATABASE IF NOT EXISTS RAW_SALES;

CREATE SCHEMA IF NOT EXISTS RAW_SALES.LANDING;      -- Raw ingestion target
CREATE SCHEMA IF NOT EXISTS RAW_SALES.BRONZE;        -- Full-fidelity copy + metadata
CREATE SCHEMA IF NOT EXISTS RAW_SALES.SILVER;        -- Cleansed, deduplicated, quality-checked
CREATE SCHEMA IF NOT EXISTS RAW_SALES.GOLD;          -- Star schema, BI tables, data products
CREATE SCHEMA IF NOT EXISTS RAW_SALES.MONITORING;    -- DQ logs, SLA tracking, task audit

SHOW SCHEMAS IN DATABASE RAW_SALES;


-- 3. Set session context for the rest of the demo
USE DATABASE RAW_SALES;
USE SCHEMA LANDING;
USE WAREHOUSE ANALYTICS_WH;


-- ============================================================================
-- CHECKPOINT: You should see ANALYTICS_WH warehouse and 5 schemas listed.
-- NEXT: Run 02_ingest_landing_data.sql
-- ============================================================================
