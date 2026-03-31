# Exploring the ML Feature Store in Snowflake Web UI

## Quick Start: No Python or Terminal Required ⚡

All feature store data is **queryable directly in Snowflake**. Use the web UI to explore everything without leaving Snowflake.

---

## 🚀 Step 1: Open Snowflake Web UI

1. Go to your Snowflake account URL (e.g., `https://xy12345.us-east-1.snowflakecomputing.app`)
2. Log in with your credentials
3. Select database: `RAW_SALES`
4. Select schema: `FEATURE_STORE`
5. Click **"Worksheets"** in the left sidebar

---

## 💡 Step 2: Run Pre-Built Queries

Copy any query below, paste into your Snowflake worksheet, and click **Run**:

### Query 1: See All 21 Features

```sql
SELECT
    FEATURE_ID,
    FEATURE_NAME,
    ENTITY_TYPE,
    OWNER_TEAM,
    TAGS
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
ORDER BY ENTITY_TYPE, FEATURE_NAME;
```

**What you'll see**: Complete catalog of all 21 features with ownership and tags.

---

### Query 2: Features by Team

```sql
SELECT
    OWNER_TEAM,
    ENTITY_TYPE,
    FEATURE_COUNT,
    FEATURE_NAMES
FROM RAW_SALES.FEATURE_STORE.v_feature_ownership
ORDER BY OWNER_TEAM;
```

**What you'll see**: Which team owns which features (for SLA contacts).

---

### Query 3: Feature Store Summary Dashboard

```sql
SELECT * FROM RAW_SALES.FEATURE_STORE.v_feature_store_summary;
```

**What you'll see**: Key metrics:
- 21 Features Registered
- 3 Entities (customer, product, sales rep)
- 150K+ Customer training rows
- 1.5M+ Product training rows

---

### Query 4: Data Quality & Coverage

```sql
SELECT
    FEATURE_TABLE,
    TOTAL_ROWS,
    UNIQUE_ENTITIES,
    DATE_SNAPSHOTS,
    EARLIEST_DATE,
    LATEST_DATE
FROM RAW_SALES.FEATURE_STORE.v_feature_table_health;
```

**What you'll see**: 
- How many rows in each feature table?
- Date range covered (1992-2000)
- Number of date snapshots available

---

### Query 5: Training Data Ready for ML

```sql
SELECT
    ENTITY_TYPE,
    TRAINING_SAMPLES,
    UNIQUE_ENTITIES,
    EARLIEST_DATE,
    LATEST_DATE,
    SNAPSHOT_COUNT
FROM RAW_SALES.FEATURE_STORE.v_training_data_availability;
```

**What you'll see**: Volume and dates of training data available.

---

### Query 6: Feature Dependencies (Lineage)

```sql
SELECT
    UPSTREAM,
    DOWNSTREAM,
    DEPENDENCY_TYPE
FROM RAW_SALES.FEATURE_STORE.v_feature_lineage_tree
LIMIT 20;
```

**What you'll see**: Which features depend on which sources or other features.

---

## 🔍 Step 3: Search & Filter

### Find specific features by tag:

```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE TAGS LIKE '%rfm%';
```

Replace `rfm` with: `quota_attainment`, `customer-lifetime-value`, etc.

---

### Find all customer features:

```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE ENTITY_TYPE = 'customer';
```

---

### Find features owned by a team:

```sql
SELECT *
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE OWNER_TEAM = 'Analytics';
```

---

## 📊 Step 4: Preview Sample Data

### See actual customer features:

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
LIMIT 20;
```

**What you'll see**: Real RFM scores & segment classifications for customers.

---

### See product features:

```sql
SELECT
    PRODUCT_ID,
    OBSERVATION_DATE,
    revenue_12m,
    units_sold_12m,
    return_rate_pct,
    revenue_rank
FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline
WHERE OBSERVATION_DATE = '2000-01-01'
ORDER BY revenue_rank
LIMIT 20;
```

**What you'll see**: Top 20 products by revenue with quality metrics.

---

### See sales rep features:

```sql
SELECT
    REP_ID,
    OBSERVATION_DATE,
    quota,
    ytd_revenue,
    quota_attainment_pct,
    ytd_orders
FROM RAW_SALES.FEATURE_STORE.sales_rep_quota_features_offline
WHERE OBSERVATION_DATE = '2000-01-01'
ORDER BY quota_attainment_pct DESC
LIMIT 20;
```

**What you'll see**: Top-performing sales reps with quota metrics.

---

## 🎓 Step 5: Point-in-Time Demo

Copy-paste this to see how **point-in-time correctness** works:

```sql
-- Get CUSTOMER #1's features as of TWO different dates
-- This shows features don't change between dates (stable snapshots)

SELECT
    CUSTOMER_ID,
    OBSERVATION_DATE,
    recency_days,
    frequency_12m,
    monetary_12m,
    estimated_segment
FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
WHERE CUSTOMER_ID = 1
  AND OBSERVATION_DATE IN ('1999-01-01', '2000-01-01')
ORDER BY OBSERVATION_DATE;
```

**Key insight**: Same customer has *different* feature values on different dates (that's PIT correctness working!).

---

## 📈 Step 6: Create a Personal Dashboard

Snowflake Web UI tip: **Bookmark your favorite queries for quick access!**

1. Run a query
2. Click the **"Save as"** button
3. Name it (e.g., "Feature Store Overview")
4. Click **"Save"**

Now you can quickly re-run it without copying/pasting.

---

## 🎯 Use Case Examples

### Use Case 1: "Which features are available for customer churn modeling?"

```sql
SELECT
    FEATURE_NAME,
    DESCRIPTION
FROM RAW_SALES.FEATURE_STORE.v_feature_registry_browser
WHERE ENTITY_TYPE = 'customer'
  AND (TAGS LIKE '%engagement%' OR TAGS LIKE '%rfm%')
ORDER BY FEATURE_NAME;
```

---

### Use Case 2: "How much training data do I have?"

```sql
SELECT
    ENTITY_TYPE,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CASE WHEN OBSERVATION_DATE BETWEEN '1999-01-01' AND '1999-12-31' THEN OBSERVATION_DATE END) AS days_in_1999
FROM (
    SELECT CUSTOMER_ID as ENTITY_ID, 'customer' as ENTITY_TYPE, OBSERVATION_DATE FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline
    UNION ALL
    SELECT PRODUCT_ID, 'product', OBSERVATION_DATE FROM RAW_SALES.FEATURE_STORE.product_performance_features_offline
)
GROUP BY ENTITY_TYPE;
```

---

### Use Case 3: "Show me the feature version history"

```sql
SELECT
    FEATURE_ID,
    VERSION_NUMBER,
    IS_ACTIVE,
    DEPLOYMENT_TIMESTAMP,
    CHANGE_REASON
FROM RAW_SALES.FEATURE_STORE.v_feature_version_history
WHERE IS_ACTIVE = TRUE
ORDER BY FEATURE_ID;
```

---

## 📋 All Available Views in Snowflake

Here's what's available to query directly:

| View Name | Purpose |
|-----------|---------|
| `v_feature_registry_browser` | Browse all 21 features with metadata |
| `v_feature_ownership` | See who owns each feature |
| `v_feature_table_health` | Data quality & coverage for each table |
| `v_feature_lineage_tree` | Dependency graph |
| `v_features_by_entity` | Features grouped by entity type |
| `v_training_data_availability` | Training data volume & dates |
| `v_feature_version_history` | Audit trail of all schema changes |
| `v_feature_store_summary` | Key metrics (for dashboarding) |

**To see all views:**
```sql
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'FEATURE_STORE'
  AND TABLE_NAME LIKE 'V_%'
ORDER BY TABLE_NAME;
```

---

## 🔧 Admin: Check Feature Store Health

Run this to verify everything is working:

```sql
-- Health Check 1: Feature count
SELECT COUNT(*) as feature_count FROM RAW_SALES.FEATURE_STORE.feature_registry;
-- Expected: 21

-- Health Check 2: Customer features populated
SELECT COUNT(*) as customer_feature_rows FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline;
-- Expected: 100K+

-- Health Check 3: Training data available
SELECT COUNT(*) as training_samples FROM RAW_SALES.FEATURE_STORE.training_data_customers;
-- Expected: 100K+

-- Health Check 4: Latest data freshness
SELECT MAX(OBSERVATION_DATE) as most_recent_data FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline;
-- Expected: 2000-01-01
```

---

## 💡 Pro Tips

### Tip 1: Export Results as CSV
In Snowflake Web UI:
1. Run a query
2. Click the **"Download as CSV"** button (bottom right)
3. Open in Excel/Pandas for further analysis

---

### Tip 2: Create a Worksheet Folder
Organize your exploration:
1. Create folder: "Feature Store Exploration"
2. Save queries in it
3. Quickly reference later

---

### Tip 3: Use Filters in Results
After running a query, click on any column header → **"Filter"** to narrow results.

---

### Tip 4: Schedule Queries to Run Automatically
Advanced: Right-click a query → **"Schedule"** → choose frequency
Useful for: Monitoring feature freshness, SLA compliance

---

## 🎓 Want to See Raw Training Data?

```sql
-- Get customer training data (1999 only)
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_customers
WHERE OBSERVATION_DATE >= '1999-01-01' AND OBSERVATION_DATE < '2000-01-01'
LIMIT 100;

-- Get product training data (1998-1999)
SELECT *
FROM RAW_SALES.FEATURE_STORE.training_data_products
WHERE OBSERVATION_DATE >= '1998-01-01' AND OBSERVATION_DATE < '2000-01-01'
LIMIT 100;
```

This data is **ready to export and use in Python/R/Scikit-learn** for ML training!

---

## 📞 Need Help?

1. **"What features should I use for my model?"** → Run Query 1, search by entity type and tags
2. **"How much data do I have?"** → Run Query 3 (summary) or Query 4 (training data)
3. **"Where does feature X come from?"** → Run Query 6 (lineage) or read `FEATURE_STORE_GUIDE.md`
4. **"Can I see sample data?"** → Run one of the "Preview Sample Data" queries above

---

## 🚀 Next Steps

1. **Run these queries** to explore your feature store
2. **Pick a feature set** that matches your use case (customer/product/sales rep)
3. **Export training data** as CSV
4. **Build your ML model** in Python/Jupyter
5. **Deploy** and iterate!

---

## Summary Table: Quick Query Reference

| What I Want | Query to Run |
|---|---|
| See all 21 features | Query 1 |
| See feature ownership | Query 2 |
| See summary stats | Query 3 |
| Check data coverage | Query 4 |
| Check training data | Query 5 |
| See dependencies | Query 6 |
| Find features by tag | Search & Filter example 1 |
| Find customer features | Search & Filter example 2 |
| Preview real data | Sample Data section |
| Check health | Admin section |

**You're all set!** 🎉 All feature store data is now fully visible in Snowflake.
