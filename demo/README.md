# Snowflake Sales Platform — Step-by-Step Demo

Run these 10 scripts in order in a Snowflake Worksheet. Each builds on the previous step.

## Prerequisites
- Snowflake account with SYSADMIN or ACCOUNTADMIN role
- Access to `SNOWFLAKE_SAMPLE_DATA` (built-in TPC-H data)
- No CSV uploads or external files needed

## Demo Scripts

| # | File | What It Does | Runtime |
|---|------|-------------|---------|
| 01 | `01_setup_warehouse_and_schemas.sql` | Create XS warehouse + 5 medallion schemas | ~10s |
| 02 | `02_ingest_landing_data.sql` | Load ~7.8M rows from TPC-H into Landing | ~2 min |
| 03 | `03_bronze_layer_with_metadata.sql` | Full-fidelity copy with audit columns + CDC Streams | ~3 min |
| 04 | `04_silver_layer_dama6_quality.sql` | DAMA 6 quality checks, dedup, Dynamic Tables | ~5 min |
| 05 | `05_gold_star_schema.sql` | Star schema: 5 dims + 1 fact + 5 BI tables | ~10 min |
| 06 | `06_data_products_and_governance.sql` | 3 data products, RBAC roles, SLA monitoring, Tasks | ~5 min |
| 07 | `07_feature_store_ml.sql` | ML Feature Store with point-in-time correctness | ~10 min |
| 08 | `08_dama6_quality_checks.sql` | Standalone DAMA 6 audit (run anytime after step 04) | ~2 min |
| 09 | `09_bi_analytics_queries.sql` | 10 ready-to-run BI queries on Gold layer | ~1 min |
| 10 | `10_acceptance_gates.sql` | Pass/fail release gates across all phases | ~1 min |

**Total estimated runtime: ~40 minutes on XS warehouse**

> Tip: Resize to MEDIUM for step 05 (`ALTER WAREHOUSE ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM'`) to cut Gold build time to ~2 minutes, then resize back to XS.

## Architecture

```
Landing (Raw)  -->  Bronze (+ Audit)  -->  Silver (DAMA 6 Cleansed)  -->  Gold (Star Schema)
  6 tables          6 tables + Streams      6 tables + Dynamic Tables     5 dims + 1 fact
                                                                          5 BI tables
                                                                          3 Data Products
                                                                          ML Feature Store
```

## Key Concepts Demonstrated

- **Medallion Architecture** — Landing > Bronze > Silver > Gold
- **DAMA 6 Data Quality** — Completeness, Uniqueness, Timeliness, Validity, Accuracy, Consistency
- **Snowflake Platform** — Streams (CDC), Dynamic Tables, Tasks, Search Optimization, Clustering
- **Dimensional Modeling** — Kimball star schema with fact/dimension separation
- **Data Products** — SLA-governed, role-specific tables with RBAC
- **ML Feature Store** — Point-in-time correctness, feature registry, versioning, lineage
- **Governance** — Acceptance gates, DQ monitoring, rejection audit trails
