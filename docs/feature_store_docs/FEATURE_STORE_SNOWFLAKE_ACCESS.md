# 🎯 ML Feature Store on Snowflake — Complete Access Guide

## TL;DR: See Everything in Snowflake

✅ **All feature store data is queryable directly in Snowflake Web UI**  
✅ **No Python or terminal needed** — just SQL  
✅ **8 pre-built views** for instant exploration  
✅ **Copy-paste ready queries** for common tasks  

---

## 📍 Where Everything Lives

### Snowflake Database
```
RAW_SALES (database)
└── FEATURE_STORE (schema)
    ├── Governance Tables:
    │   ├── feature_registry (21 features cataloged)
    │   ├── feature_versions (version history)
    │   ├── feature_lineage (dependency graph)
    │   └── entity_keys (customer/product/rep definitions)
    │
    ├── Offline Store Tables (Precomputed Features):
    │   ├── customer_rfm_features_offline (~150K rows)
    │   ├── customer_engagement_features_offline (~150K rows)
    │   ├── product_performance_features_offline (~1.5M rows)
    │   └── sales_rep_quota_features_offline (~50K rows)
    │
    ├── Training Views:
    │   ├── training_data_customers
    │   └── training_data_products
    │
    └── 8 Pre-Built Exploration Views (v_*)
        ├── v_feature_registry_browser
        ├── v_feature_ownership
        ├── v_feature_table_health
        ├── v_feature_lineage_tree
        ├── v_features_by_entity
        ├── v_training_data_availability
        ├── v_feature_version_history
        └── v_feature_store_summary
```

---

## 🚀 3-Step Access Guide

### Step 1: Open Snowflake Web UI
- Go to: `https://your-account.snowflakecomputing.app` (or your company's Snowflake URL)
- Log in with your credentials
- Select Database: `RAW_SALES`
- Select Schema: `FEATURE_STORE`

### Step 2: Click "Worksheets"
Click the "Worksheets" tab on the left sidebar → "+ Worksheet"

### Step 3: Copy & Run Query
Copy any query from below, paste it into the worksheet, and click **"Run"**

---

## 🔍 What You Can See Right Now

### Dashboard 1: Feature Store Overview
**Copy this query:**
```sql
SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_store_summary;
```

**See:**
- 21 Features Registered
- 3 Entity Types (Customer, Product, SalesRep)
- 2 Owner Teams (Analytics, Sales)
- 21 Features with PIT Correctness
- 2.5M+ Total Training Rows

---

### Dashboard 2: All 21 Features Catalog
**Copy this query:**
```sql
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
```

**See:**
- Feature names and IDs
- Which entity (customer/product/sales rep)
- Who owns it
- Whether it's point-in-time correct (all TRUE)
- Searchable tags
- Full description

---

### Dashboard 3: Data Coverage & Quality
**Copy this query:**
```sql
SELECT
    FEATURE_TABLE,
    TOTAL_ROWS,
    UNIQUE_ENTITIES,
    DATE_SNAPSHOTS,
    EARLIEST_DATE,
    LATEST_DATE,
    DAYS_COVERED
FROM RAW_SALES.FEATURE_STORE.v_feature_table_health;
```

**See:**
- How many rows in each table?
- How many entities (customers/products/reps)?
- Date range (1992-2000 for TPC-H)
- Data freshness

---

### Dashboard 4: Team Ownership
**Copy this query:**
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_ownership
ORDER BY OWNER_TEAM;
```

**See:**
- Which team owns which features
- SLA contact points
- Count of features per team

---

### Dashboard 5: Feature Dependencies
**Copy this query:**
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_lineage_tree;
```

**See:**
- Which features depend on which tables/features
- Impact analysis (if X changes, which features are affected?)

---

### Dashboard 6: Training Data Available
**Copy this query:**
```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_training_data_availability;
```

**See:**
- How many training samples available?
- Date range for training
- Number of temporal snapshots (for time-series features)

---

## 📊 View Real Data

### See Real Customer RFM Scores

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
WHERE OBSERVATION_DATE = '2000-01-01'
LIMIT 50;
```

---

### See Real Product Performance

```sql
SELECT
    PRODUCT_ID,
    OBSERVATION_DATE,
    cumulative_revenue,
    revenue_12m,
    units_sold_12m,
    return_rate_pct,
    revenue_rank
FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline
WHERE cumulative_revenue > 5000000  -- Top revenue products
  AND OBSERVATION_DATE = '2000-01-01'
ORDER BY revenue_rank
LIMIT 50;
```

---

### See Real Sales Rep KPIs

```sql
SELECT
    REP_ID,
    OBSERVATION_DATE,
    quota,
    ytd_revenue,
    quota_attainment_pct,
    ytd_orders,
    ytd_customer_count
FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline
WHERE OBSERVATION_DATE = '2000-01-01'
ORDER BY quota_attainment_pct DESC
LIMIT 50;
```

---

## 🔎 Search & Filter Examples

### Find features with a specific tag:

```sql
-- Find all RFM features
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE TAGS LIKE '%rfm%';
```

Replace `%rfm%` with: `%quota_attainment%`, `%customer-lifetime-value%`, etc.

---

### Find all features for one entity type:

```sql
-- Find all customer features
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE ENTITY_TYPE = 'customer';
```

Or `product` or `sales_rep`

---

### Find features owned by a team:

```sql
-- Find features owned by Analytics team
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE OWNER_TEAM = 'Analytics';
```

---

## 🎓 Point-in-Time Correctness Demo

**Query that proves PIT correctness:**

```sql
-- Get same customer's features on TWO different dates
-- Notice how features differ between dates
SELECT
    CUSTOMER_ID,
    OBSERVATION_DATE,
    recency_days,
    frequency_12m,
    monetary_12m,
    rfm_composite_score
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE CUSTOMER_ID = 1
  AND OBSERVATION_DATE IN ('1999-01-01', '2000-01-01')
ORDER BY OBSERVATION_DATE;
```

**Why this matters**: Different features on different dates = no data leakage in your ML training!

---

## 📖 Documentation Files

All of this is also documented in detail:

| File | Purpose |
|------|---------|
| `ML_FEATURE_STORE_README.md` | Start here — overview & summary |
| `FEATURE_STORE_SUMMARY.md` | Architecture & feature catalog |
| `FEATURE_STORE_GUIDE.md` | Technical deep dive |
| `FEATURE_STORE_QUICKSTART.md` | 5 ML recipes for data scientists |
| **`FEATURE_STORE_SNOWFLAKE_GUIDE.md`** | This guide — Snowflake Web UI queries |
| `FEATURE_STORE_ARCHITECTURE.md` | How it fits the challenge |
| `feature_store.py` | Python API (for programmatic access) |
| `06_feature_store_ml.sql` | SQL implementation |
| `07_feature_store_explore.sql` | Views & exploration queries |

---

## 💼 Common Use Cases

### "What features should I use for my churn model?"

```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE ENTITY_TYPE = 'customer'
  AND (TAGS LIKE '%engagement%' OR TAGS LIKE '%rfm%');
```

---

### "Show me all available data about customers"

```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_customers
LIMIT 100;
```

Then **Download as CSV** to open in Python/Excel.

---

### "What's the date range of my training data?"

```sql
SELECT
    MIN(OBSERVATION_DATE) as earliest,
    MAX(OBSERVATION_DATE) as latest,
    COUNT(DISTINCT OBSERVATION_DATE) as num_snapshots
FROM RAW_SALES.FEATURE_STORE.training_data_customers;
```

---

### "Which team owns the quota_attainment feature?"

```sql
SELECT OWNER_TEAM, FEATURE_ID, FEATURE_NAME
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE TAGS LIKE '%quota_attainment%';
```

---

## ✅ Quick Health Check

Verify everything is working:

```sql
-- Check 1: Features registered?
SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.feature_registry;
-- Expected: 21

-- Check 2: Customer data populated?
SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline;
-- Expected: 100K+

-- Check 3: Training data available?
SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.training_data_customers;
-- Expected: 100K+

-- Check 4: All dates present?
SELECT MAX(OBSERVATION_DATE) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline;
-- Expected: 2000-01-01
```

---

## 🎯 Next: Export to ML

Once you've explored on Snowflake:

1. **Export as CSV**:
   - Run a query
   - Click "Download as CSV" button

2. **Use in Python**:
   ```python
   import pandas as pd
   df = pd.read_csv('training_data.csv')
   
   from sklearn.ensemble import RandomForestClassifier
   model = RandomForestClassifier()
   model.fit(df[features], df['target'])
   ```

3. **Or use Python API** (see `feature_store.py`):
   ```python
   from feature_store import FeatureStore
   fs = FeatureStore(session)
   df = fs.get_training_dataset('customer', ('1999-01-01', '1999-12-31'))
   model.fit(df[features], df['target'])
   ```

---

## 🎉 You're Ready!

**Everything is visible in Snowflake:**
- ✅ All 21 features cataloged and queryable
- ✅ Real training data available for ML
- ✅ Data quality metrics visible
- ✅ Ownership and SLA info available
- ✅ Feature dependencies documented
- ✅ Point-in-time correctness guaranteed

**Next steps:**
1. Run the queries above in Snowflake Web UI
2. Pick your entity (customer/product/sales rep)
3. Export training data as CSV
4. Build your ML model!

---

## 📞 Questions?

**In Snowflake:**
- Select database: `RAW_SALES`
- Select schema: `FEATURE_STORE`
- Query: `v_feature_registry_browser`
- Find features, descriptions, owner contact

**In Python:**
```python
from feature_store import FeatureStore
fs = FeatureStore(session)
fs.list_features(tags='your-tag-here')  # Search by tag
fs.get_feature_lineage('feature_id')    # See dependencies
```

**In Documentation:**
- Read: `FEATURE_STORE_SNOWFLAKE_GUIDE.md` for more examples
- Read: `FEATURE_STORE_QUICKSTART.md` for 5 ML recipes

---

**Happy exploring!** 🚀
