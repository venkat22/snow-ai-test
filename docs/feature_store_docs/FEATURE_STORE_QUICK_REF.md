# ML Feature Store — Quick Reference Card

## 🎯 Your Feature Store in 1 Page

**Location**: `RAW_SALES.FEATURE_STORE`  
**Status**: ✅ Ready to use (run `python python/orchestration/run_all.py` to initialize)  
**Contains**: 21 features across 3 entity types, 2.5M+ training rows

---

## 🔥 Top 5 Queries (Copy & Paste)

### 1️⃣ See All 21 Features
```sql
SELECT FEATURE_NAME, ENTITY_TYPE, OWNER_TEAM, TAGS, DESCRIPTION
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
ORDER BY ENTITY_TYPE, FEATURE_NAME;
```

### 2️⃣ Get Training Data for ML (Customers)
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_customers
WHERE OBSERVATION_DATE >= '1999-01-01' 
  AND OBSERVATION_DATE < '2000-01-01'
LIMIT 100000;
-- Click "Download as CSV" to export
```

### 3️⃣ Get Training Data for ML (Products)
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_products
WHERE OBSERVATION_DATE >= '1999-01-01' 
  AND OBSERVATION_DATE < '2000-01-01'
LIMIT 1500000;
-- Click "Download as CSV" to export
```

### 4️⃣ Check Data Quality (Health Check)
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_table_health;
```

### 5️⃣ Understand Dependencies (Impact Analysis)
```sql
SELECT SOURCE_TABLE, count(*) as impacted_features
FROM RAW_SALES.FEATURE_STORE.feature_lineage
GROUP BY SOURCE_TABLE;
```

---

## 📊 Available Tables

| Table | Entity | Rows | Observation Dates | Use Case |
|-------|--------|------|-------------------|----------|
| `customer_rfm_features_offline` | Customer | ~150K | 1992-2000 | Segmentation, churn, LTV |
| `customer_engagement_features_offline` | Customer | ~150K | 1992-2000 | Engagement scoring |
| `product_performance_features_offline` | Product | ~1.5M | 1992-2000 | Demand, pricing, discount |
| `sales_rep_quota_features_offline` | SalesRep | ~50K | 1992-2000 | Performance ranking |
| `training_data_customers` | Customer | ~2.5M | 1992-2000 | ML training (customer) |
| `training_data_products` | Product | ~1.5M | 1992-2000 | ML training (product) |

---

## 🔍 8 Pre-Built Views (Just Column Names)

| View | Purpose | Key Columns |
|------|---------|-------------|
| `v_feature_registry_browser` | Browse all 21 features | FEATURE_NAME, ENTITY_TYPE, OWNER_TEAM, TAGS |
| `v_feature_ownership` | Who owns what | OWNER_TEAM, FEATURE_COUNT |
| `v_feature_table_health` | Data quality | TOTAL_ROWS, UNIQUE_ENTITIES, DATE_RANGE |
| `v_feature_lineage_tree` | Dependencies | FEATURE_NAME, SOURCE_TABLE, IMPACTED_FEATURES |
| `v_features_by_entity` | Grouped by entity | ENTITY_TYPE, FEATURE_NAMES, FEATURE_COUNT |
| `v_training_data_availability` | Training readiness | ENTITY_TYPE, SNAPSHOT_COUNT, SAMPLE_COUNT |
| `v_feature_version_history` | Audit trail | FEATURE_ID, VERSION, CHANGED_WHEN |
| `v_feature_store_summary` | High-level stats | TOTAL_FEATURES, ENTITY_TYPES, TRAINING_ROWS |

---

## 🎓 Point-in-Time Correctness (The Secret Sauce)

**What it means**: Each feature is timestamped with `OBSERVATION_DATE`  
**Why it matters**: Different values on different dates = NO data leakage in ML  
**See it yourself**:

```sql
-- Same customer on two different dates
SELECT CUSTOMER_ID, OBSERVATION_DATE, recency_days, frequency_12m, monetary_12m
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE CUSTOMER_ID = 1 AND OBSERVATION_DATE IN ('1999-01-01', '2000-01-01');
```

---

## 🚀 3 Use Cases

### Churn Model
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_customers
WHERE OBSERVATION_DATE BETWEEN '1999-01-01' AND '1999-12-31'
  AND recency_days > 30;  -- Target: customers becoming inactive
```

### Demand Forecast
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_products
WHERE OBSERVATION_DATE BETWEEN '1999-01-01' AND '1999-12-31'
  AND cumulative_revenue > 1000000;  -- High-value products
```

### Sales Rep Performance Ranking
```sql
SELECT REP_ID, quota_attainment_pct, ytd_revenue
FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline
WHERE OBSERVATION_DATE = '2000-01-01'
ORDER BY quota_attainment_pct DESC;
```

---

## ✅ Is It Ready?

Run this health check:

```sql
SELECT
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.feature_registry) as features_registered,
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline) as customer_rows,
    (SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.training_data_customers) as training_customer_rows;
```

**Expected results:**
- `features_registered`: 21
- `customer_rows`: 150000+
- `training_customer_rows`: 2500000+

---

## 📥 Export to Python

### In Snowflake Web UI:
1. Run any query above
2. Click **"Download as CSV"** button
3. Open in Python:
   ```python
   import pandas as pd
   df = pd.read_csv('training_data_customers.csv')
   ```

### Or use Python API:
```python
from feature_store import FeatureStore
fs = FeatureStore(session)
df = fs.get_training_dataset('customer', ('1999-01-01', '1999-12-31'))
```

---

## 🔗 Search by Tag

All features tagged with: `#rfm`, `#engagement`, `#quota-attainment`, `#customer-value`, `#performance`

```sql
-- Find RFM features
SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE TAGS LIKE '%rfm%';
```

---

## 📞 Who Owns What?

```sql
SELECT DISTINCT OWNER_TEAM FROM RAW_SALES.FEATURE_STORE.v_feature_ownership;
```

**Teams**: Analytics, Sales, Product

---

## 🎯 Common Questions

**Q: How much data for training?**
```sql
SELECT COUNT(*), MIN(OBSERVATION_DATE), MAX(OBSERVATION_DATE)
FROM RAW_SALES.FEATURE_STORE.training_data_customers;
```

**Q: What if I need different date range?**
Filter by `OBSERVATION_DATE` in any query

**Q: Can I see features for one customer?**
```sql
SELECT * FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE CUSTOMER_ID = 12345;
```

**Q: What's the latest date?**
```sql
SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline;
```

---

## 📚 Full Docs

- **`FEATURE_STORE_SNOWFLAKE_ACCESS.md`** ← Start here (complete guide)
- `FEATURE_STORE_SNOWFLAKE_GUIDE.md` (detailed examples)
- `FEATURE_STORE_QUERIES.sql` (30+ query templates)
- `FEATURE_STORE_QUICKSTART.md` (5 ML recipes)

---

**That's it!** 🎉 You have access to a production-grade feature store with 2.5M+ training rows, point-in-time correctness, and governance built in.
