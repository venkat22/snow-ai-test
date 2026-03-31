# Phase 1: Snowflake Foundation Setup Guide (TPCH Canonical Baseline)

Goal: Activate Snowflake, establish RAW_SALES schemas, and validate TPCH source mapping for downstream Bronze, Silver, and Gold implementation.

Duration: About 1 day

---

## Step 1: Activate Snowflake Trial

1. Sign up at https://signup.snowflake.com.
2. Select AWS and a US region unless your constraints require otherwise.
3. Confirm account activation and sign in at https://app.snowflake.com.
4. Verify a warehouse exists:
   - Preferred: ANALYTICS_WH
   - Size: XS
   - Auto-suspend: 60 seconds

Verification:
- You can run SELECT CURRENT_WAREHOUSE(); and receive a non-null value.

---

## Step 2: Confirm Canonical Source Baseline

This project uses one mandatory source baseline only:

- Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1

Run:

```sql
SELECT COUNT(*) AS CUSTOMER_ROWS
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

SELECT COUNT(*) AS ORDERS_ROWS
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

SELECT COUNT(*) AS LINEITEM_ROWS
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;
```

Verification:
- All counts are greater than 0.

---

## Step 3: Create RAW_SALES Database and Schemas

Run:

```sql
CREATE DATABASE IF NOT EXISTS RAW_SALES;

CREATE SCHEMA IF NOT EXISTS RAW_SALES.LANDING;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.BRONZE;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.SILVER;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.GOLD;
CREATE SCHEMA IF NOT EXISTS RAW_SALES.MONITORING;

SHOW SCHEMAS IN DATABASE RAW_SALES;
```

Verification:
- LANDING, BRONZE, SILVER, GOLD, and MONITORING exist.

---

## Step 4: Materialize LANDING Tables for Downstream Phases

Downstream scripts require RAW_SALES.LANDING table objects. Run the foundation script to create them from TPCH with canonical column names.

```sql
-- Run full script in worksheet:
-- 01_phase1_foundation.sql

-- Minimum verification
SELECT COUNT(*) FROM RAW_SALES.LANDING.CUSTOMERS;
SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDERS;
SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS;
SELECT COUNT(*) FROM RAW_SALES.LANDING.PRODUCTS;
SELECT COUNT(*) FROM RAW_SALES.LANDING.SALES_REPS;
SELECT COUNT(*) FROM RAW_SALES.LANDING.TERRITORIES;
```

Optional: create source helper views for lineage/debug only.

```sql
USE DATABASE RAW_SALES;
USE SCHEMA LANDING;

CREATE OR REPLACE VIEW customers_src AS
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

CREATE OR REPLACE VIEW orders_src AS
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

CREATE OR REPLACE VIEW order_items_src AS
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;

CREATE OR REPLACE VIEW products_src AS
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;

CREATE OR REPLACE VIEW suppliers_src AS
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;

CREATE OR REPLACE VIEW nations_src AS
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

CREATE OR REPLACE VIEW regions_src AS
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;
```

Source-to-domain mapping:

- CUSTOMER -> customers
- ORDERS -> orders
- LINEITEM -> order_items
- PART -> products
- SUPPLIER, NATION, REGION -> supplier and territory proxy

Verification:

```sql
SHOW TABLES IN SCHEMA RAW_SALES.LANDING;
SHOW VIEWS IN SCHEMA RAW_SALES.LANDING;

SELECT COUNT(*) FROM RAW_SALES.LANDING.CUSTOMERS;
SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDERS;
SELECT COUNT(*) FROM RAW_SALES.LANDING.ORDER_ITEMS;
```

---

## Step 5: Create Baseline Validation Snapshot

Capture baseline row counts for traceability.

```sql
CREATE OR REPLACE TABLE RAW_SALES.MONITORING.foundation_row_counts AS
SELECT CURRENT_TIMESTAMP() AS captured_at, 'CUSTOMERS' AS object_name, COUNT(*) AS row_count
FROM RAW_SALES.LANDING.CUSTOMERS
UNION ALL
SELECT CURRENT_TIMESTAMP(), 'ORDERS', COUNT(*)
FROM RAW_SALES.LANDING.ORDERS
UNION ALL
SELECT CURRENT_TIMESTAMP(), 'ORDER_ITEMS', COUNT(*)
FROM RAW_SALES.LANDING.ORDER_ITEMS
UNION ALL
SELECT CURRENT_TIMESTAMP(), 'PRODUCTS', COUNT(*)
FROM RAW_SALES.LANDING.PRODUCTS;

SELECT *
FROM RAW_SALES.MONITORING.foundation_row_counts
ORDER BY object_name;
```

Verification:
- Monitoring snapshot table exists and contains one row per mapped object.

---

## Step 6: Foundation Exit Criteria

Phase 1 is complete only when all checks pass:

1. TPCH source baseline validated with non-zero counts.
2. RAW_SALES schemas created.
3. LANDING canonical tables created and queryable.
4. Source-to-domain mapping documented.
5. Baseline monitoring snapshot created.

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| No access to SNOWFLAKE_SAMPLE_DATA | Ensure your role can read shared sample data; switch to ACCOUNTADMIN for setup if needed |
| Warehouse suspended errors | Resume warehouse or enable auto-resume |
| View creation fails | Confirm database and schema context before CREATE VIEW |
| Empty result sets | Verify you are using TPCH_SF1, not another schema level |

---

## Next Steps

1. Proceed to Bronze implementation in [02_phase2_bronze.sql](02_phase2_bronze.sql).
2. Implement Silver quality and standardization in [03_phase2_silver.sql](03_phase2_silver.sql).
3. Complete Gold marts and dimensional model in [04_phase2_gold.sql](04_phase2_gold.sql).