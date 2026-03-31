# ML Feature Store — Architecture & Implementation Guide

## Overview

This document describes the **Production ML Feature Store** built on Snowflake for the Sales Analytics Platform. The feature store provides:

1. **Point-in-Time (PIT) Correctness** — Get feature values as they were at any historical date (critical for training data reproducibility)
2. **Feature Versioning & Lineage** — Track schema changes, dependencies, and data lineage
3. **Offline & Online Stores** — Offline store for batch training; online store pattern for real-time serving (stubs)
4. **Governance & Metadata** — Feature registry, ownership, tags, SLAs
5. **Reusable Python API** — Simple interface for data scientists

---

## Architecture

### Layers

```
┌─────────────────────────────────────────────────────────┐
│ Training ML Models (Offline)                            │
│ - Customer churn prediction                              │
│ - Revenue forecasting                                    │
│ - Product demand modeling                                │
└────────┬────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│ Feature Store PIT Views / Training Data Sets             │
│ - training_data_customers                                │
│ - training_data_products                                 │
│ - training_data_sales_reps (stub)                        │
└────────┬────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│ Offline Store: Precomputed Feature Tables                │
│ - customer_rfm_features_offline (point-in-time)         │
│ - customer_engagement_features_offline                   │
│ - product_performance_features_offline                   │
│ - sales_rep_quota_features_offline                       │
└────────┬────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│ Gold Layer (Source): Dimensions & Facts                  │
│ - fact_orders, dim_customers, dim_products, etc.        │
└─────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. Feature Registry (`feature_registry`)
**Purpose**: Master catalog of all ML-ready features  
**Columns**:
- `FEATURE_ID`: Unique feature identifier (e.g., `cust_recency_days`)
- `FEATURE_NAME`: Human-readable name
- `DATA_TYPE`: INT, DECIMAL, VARCHAR, etc.
- `ENTITY_TYPE`: customer | product | sales_rep | temporal
- `ENTITY_KEY`: Join key (CUSTOMER_ID, PRODUCT_ID, etc.)
- `OWNER_TEAM`: Accountability
- `IS_POINT_IN_TIME`: Can be used historically? (T/F)
- `ONLINE_ENABLED`: Available in real-time store? (F for now; can expand)
- `OFFLINE_ENABLED`: Available in batch training? (T)
- `TAGS`: Searchable tags (rfm, customer-lifetime-value, quota_attainment, etc.)
- `LINEAGE_SOURCE_TABLE`: Where does feature come from?
- `LINEAGE_SOURCE_COLUMN`: Which column in source?

**Example**:
```sql
SELECT * FROM feature_registry WHERE ENTITY_TYPE = 'customer' ORDER BY CREATION_DATE DESC;
```

#### 2. Point-in-Time Feature Tables

**Customer RFM Features** (`customer_rfm_features_offline`)
- **Row Grain**: (CUSTOMER_ID, OBSERVATION_DATE)
- **Features**:
  - `recency_days`: Days since last order (12-month window)
  - `frequency_12m`: Order count in past 12 months
  - `monetary_12m`: Revenue in past 12 months
  - `recency_score`, `frequency_score`, `monetary_score`: 1-5 bucketing
  - `rfm_composite_score`: Sum of three scores (3-15 range)
  - `estimated_segment`: VIP | Loyal | At-Risk | Inactive | Engaged
- **Usage**: Training customer churn, LTV, propensity models

**Customer Engagement Features** (`customer_engagement_features_offline`)
- **Row Grain**: (CUSTOMER_ID, OBSERVATION_DATE)
- **Features**:
  - `days_since_last_purchase`: Recency
  - `engagement_status`: Active | Dormant | At-Risk | Churned
  - `lifetime_value`: Total revenue
  - `lifetime_order_count`: Total orders
  - `avg_order_value`: LTV / order count
  - `lifetime_value_per_day`: Revenue velocity
- **Usage**: Churn prediction, customer health scoring

**Product Performance Features** (`product_performance_features_offline`)
- **Row Grain**: (PRODUCT_ID, OBSERVATION_DATE)
- **Features**:
  - `cumulative_revenue`: All-time revenue
  - `revenue_12m`: 12-month rolling revenue
  - `cumulative_units_sold`: All-time volume
  - `units_sold_12m`: 12-month rolling volume
  - `total_returned_items`: Return count
  - `return_rate_pct`: Return percentage
  - `revenue_rank`: Dense rank by revenue
- **Usage**: Demand forecasting, product health scoring

**Sales Rep Quota Features** (`sales_rep_quota_features_offline`)
- **Row Grain**: (REP_ID, OBSERVATION_DATE)
- **Features**:
  - `quota`: Annual quota
  - `ytd_revenue`: Year-to-date revenue
  - `quota_attainment_ratio`: YTD / quota
  - `quota_attainment_pct`: As percentage
  - `ytd_orders`: Orders closed YTD
  - `ytd_customer_count`: Unique customers engaged
- **Usage**: Rep performance prediction, quota forecasting

#### 3. Feature Versioning (`feature_versions`)
**Purpose**: Track all schema changes and deployments  
**Columns**:
- `FEATURE_ID`: Which feature
- `VERSION_NUMBER`: 1, 2, 3, ...
- `DEFINITION_SQL`: Full SQL definition of feature
- `SCHEMA_HASH`: MD5 hash for change detection
- `DEPLOYMENT_TIMESTAMP`: When deployed
- `IS_ACTIVE`: Current version? (T/F)
- `CHANGE_REASON`: Why did schema change?

**Example**: If `cust_rfm_composite_score` formula changes from `(R+F+M)` to `(2R+2F+M)`:
```sql
INSERT INTO feature_versions (FEATURE_ID, VERSION_NUMBER, DEFINITION_SQL, CHANGE_REASON)
VALUES ('cust_rfm_composite_score', 2, '<new SQL...>', 'Weighted scoring per analytics team');
```

#### 4. Feature Lineage (`feature_lineage`)
**Purpose**: Dependency graph for impact analysis
**Columns**:
- `UPSTREAM_FEATURE_ID`: Dependency (NULL if source table)
- `DOWNSTREAM_FEATURE_ID`: Derived feature
- `UPSTREAM_TABLE_NAME`: Source table (NULL if upstream is feature)
- `DEPENDENCY_TYPE`: direct | computed | aggregated

**Example**: If `cust_segment_rfm` depends on `cust_rfm_composite_score`:
```sql
-- Changing composite formula impacts segment → need to retrain churn model
SELECT * FROM feature_lineage WHERE DOWNSTREAM_FEATURE_ID = 'cust_segment_rfm';
```

#### 5. Training Data Views

**`training_data_customers`** — Full-featured customer training set
- Combines RFM + engagement features
- Includes customer segment + region
- **Grain**: One row per (customer, observation_date)
- **Usage**: Churn, LTV, propensity modeling

**`training_data_products`** — Full-featured product training set
- Includes performance + category + unit price
- **Grain**: One row per (product, observation_date)
- **Usage**: Demand forecasting, pricing models

---

## Point-in-Time (PIT) Correctness Explained

### Problem
If you train a churn model on 2000-01-01 data but use feature values computed from *all history*, you create **data leakage**. You're using information from after the prediction date.

### Solution
Each feature table has an `OBSERVATION_DATE` column. When you want training data for a model trained on 2000-01-01:

```sql
-- WRONG: Uses all history, leakage!
SELECT * FROM customer_rfm_features_offline WHERE CUSTOMER_ID = 123;

-- CORRECT: Only features available as of 2000-01-01
SELECT * FROM customer_rfm_features_offline 
WHERE CUSTOMER_ID = 123 AND OBSERVATION_DATE = '2000-01-01';
```

The features are computed *as if you were standing at that date*, using only data up to and including that date.

---

## Using the Feature Store

### 1. SQL: Direct Queries

**Get RFM snapshot for a customer on a specific date**:
```sql
SELECT
    CUSTOMER_ID,
    OBSERVATION_DATE,
    recency_days,
    frequency_12m,
    monetary_12m,
    rfm_composite_score,
    estimated_segment
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE CUSTOMER_ID = 12345
  AND OBSERVATION_DATE = '2000-01-01';
```

**Generate training data for customer churn model (1999 only)**:
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_customers
WHERE OBSERVATION_DATE >= '1999-01-01' AND OBSERVATION_DATE < '2000-01-01'
LIMIT 10000;
```

**Find all features with a specific tag**:
```sql
SELECT FEATURE_ID, FEATURE_NAME, DESCRIPTION, OWNER_TEAM
FROM RAW_SALES.FEATURE_STORE.feature_registry
WHERE TAGS LIKE '%customer-lifetime-value%'
ORDER BY FEATURE_NAME;
```

### 2. Python: Using the Feature Store API

```python
from snowflake.snowpark.session import Session
from feature_store import FeatureStore

# Initialize
session = Session.builder.config(...).create()
fs = FeatureStore(session)

# Get customer features as of a date
features_df = fs.get_customer_features_as_of(
    customer_ids=[1, 2, 3, 4, 5],
    observation_date='2000-01-01'
)
# Returns: DataFrame with 5 rows, all features

# Get training dataset for churn model
training_data = fs.get_training_dataset(
    entity_type='customer',
    date_range=('1999-01-01', '1999-12-31'),
    sample_fraction=0.3  # Sample 30% for quick iteration
)

# Validate training data
checks = fs.validate_training_data(training_data, 'customer')
if checks['has_rows'] and checks['no_nulls_in_keys']:
    print("✓ Training data is valid")

# Simple feature importance
importance = fs.estimate_feature_importance(
    training_df=training_data,
    target_column='engagement_status'
)
print(importance)

# Explore available features
fs.list_features(entity_type='customer', tags='rfm')

# Understand lineage
lineage = fs.get_feature_lineage('cust_segment_rfm')
print(lineage)
```

---

## Feature Store Schema

### Tables (Physical)

| Table | Row Grain | Purpose | Counts |
|-------|-----------|---------|--------|
| `customer_rfm_features_offline` | (CUSTOMER_ID, OBSERVATION_DATE) | RFM for all customers over all dates | ~150K rows |
| `customer_engagement_features_offline` | (CUSTOMER_ID, OBSERVATION_DATE) | Engagement status + LTV over time | ~150K rows |
| `product_performance_features_offline` | (PRODUCT_ID, OBSERVATION_DATE) | Revenue, volume, quality by date | ~1.5M rows |
| `sales_rep_quota_features_offline` | (REP_ID, OBSERVATION_DATE) | Quota attainment by date | ~50K rows |
| `feature_registry` | FEATURE_ID | Metadata for all 21 features | 21 rows |
| `feature_versions` | (FEATURE_ID, VERSION_NUMBER) | Version history | ~21 rows (v1) |
| `feature_lineage` | LINEAGE_ID | Dependency graph | 4 rows |

### Views (Logical)

| View | Purpose | Grain |
|------|---------|-------|
| `training_data_customers` | Customer churn/LTV training set | (CUSTOMER_ID, OBSERVATION_DATE) |
| `training_data_products` | Product demand/pricing training set | (PRODUCT_ID, OBSERVATION_DATE) |

---

## Use Cases & Examples

### Use Case 1: Customer Churn Prediction

**Objective**: Predict which customers will churn in the next 30 days

**Training Data**:
```sql
SELECT
    CUSTOMER_ID,
    recency_days,
    frequency_12m,
    monetary_12m,
    engagement_status,
    lifetime_value,
    -- LABEL (created from future 30-day activity)
    CASE WHEN ... THEN 1 ELSE 0 END AS CHURNED_30_DAYS
FROM training_data_customers
WHERE OBSERVATION_DATE >= '1998-01-01' AND OBSERVATION_DATE < '1999-01-01'
```

**Key Feature**: `engagement_status` is **PIT correct** — you know their status on that date, not today.

---

### Use Case 2: Customer Lifetime Value (LTV) Regression

**Objective**: Predict total LTV for new customers

**Training Data**:
```sql
SELECT
    CUSTOMER_ID,
    OBSERVATION_DATE,
    lifetime_value AS TARGET,
    customer_tenure_days,
    lifetime_order_count,
    avg_order_value,
    rfm_composite_score
FROM training_data_customers
WHERE CUSTOMER_TENURE_DAYS > 0  -- Only mature customers
```

---

### Use Case 3: Product Demand Forecasting

**Objective**: Forecast next quarter revenue by product

**Training Data**:
```sql
SELECT
    PRODUCT_ID,
    OBSERVATION_DATE,
    revenue_12m,
    revenue_rank,
    return_rate_pct,
    category
FROM training_data_products
WHERE OBSERVATION_DATE >= '1998-01-01' AND OBSERVATION_DATE < '2000-01-01'
```

---

## Performance Considerations

### Indexing Strategy
- Cluster keys on feature tables: `(OBSERVATION_DATE, ENTITY_KEY)`
- Ensures PIT lookups are fast

### Query Optimization
- Pre-aggregate features at fixed intervals (monthly for customers, daily for products)
- Materialized views avoid recomputation

### Storage
- Feature snapshots stored with `VALID_FROM` / `VALID_TO` for SCD Type 2 tracking (future expansion)
- Current implementation uses denormalized wide tables (simpler, faster for training)

---

## Governance

### Ownership & SLAs
Each feature in the registry has an owner and implicit SLA:
- **Customer RFM**: Analytics Team, refresh daily
- **Product Performance**: Analytics Team, refresh daily
- **Sales Rep Quota**: Sales Ops Team, refresh daily

### Change Management
When a feature definition changes:
1. Insert new row into `feature_versions` with new SQL
2. Update `feature_registry` SET VERSION = new_version
3. Retrain any models using that feature
4. Audit: keep history for reproducibility

### Data Lineage
Feature lineage tracks all dependencies. Use to:
- Identify impact of data quality issues (e.g., if FACT_ORDERS is wrong, which features are affected?)
- Plan feature deprecations
- Understand feature provenance

---

## Scaling & Next Steps

### Current Implementation
✅ Offline store (batch training data)  
✅ Point-in-time correctness  
✅ Feature versioning & lineage  
✅ Python API  

### Future Enhancements
- [ ] Online store (real-time serving; requires separate architecture)
- [ ] Feature freshness monitoring (SLA enforcement)
- [ ] Automated feature importance ranking
- [ ] Model registry integration (MLflow)
- [ ] Feature monitoring (drift detection)
- [ ] ML pipeline orchestration (Airflow/dbt)

---

## Troubleshooting

### "Cannot join features from different observation dates"
**Issue**: Trying to combine features with mismatched OBSERVATION_DATE values
**Solution**: Ensure all features are at the same observation date in your training join
```sql
-- WRONG
SELECT c.rfm, p.revenue FROM customer_rfm_features c JOIN product_perf p ...
-- (dates may not match)

-- RIGHT
SELECT c.rfm, p.revenue FROM customer_rfm_features c 
JOIN product_perf p 
  ON c.OBSERVATION_DATE = p.OBSERVATION_DATE 
  AND ... other joins
```

### "Feature returns NULL values"
**Issue**: Observation date is before customer's first order
**Solution**: Filter for entities that existed at the observation date
```sql
WHERE OBSERVATION_DATE >= (SELECT MIN(FIRST_ORDER_DATE) FROM dim_customers)
```

---

## Further Reading

- [point-in-time correctness in ML](https://tecton.ai/blog/what-is-point-in-time-correctness/)
- [Feast Feature Store](https://feast.dev/) (open-source inspiration)
- [Snowflake Streams & Dynamic Tables](https://docs.snowflake.com/en/sql-reference/statements/create-stream.html)
