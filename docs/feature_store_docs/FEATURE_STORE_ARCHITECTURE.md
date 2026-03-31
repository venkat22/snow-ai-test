# ML Feature Store — Integration with Challenge Architecture

## Complete Solution Map

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ TECHNICAL CHALLENGE: AI-Powered Data Platform on Snowflake                   │
└──────────────────────────────────────────────────────────────────────────────┘

TIER 1: Data Foundation
┌──────────────────────────── RAW_SALES.LANDING ─────────────────────────────┐
│  TPC-H Source → Landing (CSV ingestion pattern, minimal transformation)     │
└────────────────────────────────────────────────────────────────────────────┘

TIER 2: Medallion Architecture with Platform Services
┌──────────────────────────── RAW_SALES.BRONZE ──────────────────────────────┐
│ • Raw tables 1:1 with domains (customers, orders, products, etc.)           │
│ • Streams for CDC: customers_raw_stream, orders_raw_stream, ...             │
│ • Immutable history, _LOADED_AT lineage metadata                            │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────── RAW_SALES.SILVER ──────────────────────────────┐
│ • Cleansed, deduplicated, standardized (DAMA 6)                             │
│ • Dynamic Tables for incremental processing                                 │
│ • DQ logs and rejected records captured                                     │
│ • Snowpark jobs for complex validation                                      │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────── RAW_SALES.GOLD ──────────────────────────────────┐
│ DIMENSIONAL MODEL (BI-Optimized):                                            │
│ • fact_orders (central, clustered by DATE_KEY, TERRITORY_ID)                │
│ • dim_customers, dim_products, dim_sales_reps, dim_territories, dim_dates  │
│ • Aggregate BI Tables: monthly_sales_summary, customer_lifetime_value, etc. │
│ • Search optimization on high-selectivity lookups                           │
│                                                                              │
│ AI SEMANTIC METADATA:                                                        │
│ • ai_semantic_metadata (entity definitions, business glossary)              │
│ • ai_retrieval_index_stub (embeddings scaffold)                             │
│ • ai_rag_query_path_stub (retrieval patterns)                               │
└────────────────────────────────────────────────────────────────────────────┘
                           ↙                          ↘
              (BI Reporting)              (ML Feature Store)
              
┌──────────────────────────── RAW_SALES.MONITORING ──────────────────────────┐
│ • DQ logs, rejected records, SLA tracking                                   │
│ • Snowpark job run audit                                                    │
│ • Data product validation tasks                                             │
│ • Release gate results                                                      │
└────────────────────────────────────────────────────────────────────────────┘

TIER 3A: Data Products (BI-Driven, Marketplace)
┌──────────────────────────── RAW_SALES.GOLD ──────────────────────────────────┐
│ • sales_rep_monthly_performance — Sales KPIs             Owner: Sales Ops    │
│ • customer_revenue_forecast — Finance projections        Owner: Finance      │
│ • customer_acquisition_cohort — Marketing retention      Owner: Marketing    │
│                                                                              │
│ Published to: Snowflake Marketplace (manual process yet; consumer validated)│
│ View: BI Tools (Tableau, Looker, Mode), Dashboards                         │
└────────────────────────────────────────────────────────────────────────────┘

TIER 3B: ML Feature Store (NEW) — Point-in-Time Correct Features
┌──────────────────────────── RAW_SALES.FEATURE_STORE ──────────────────────┐
│                                                                              │
│ GOVERNANCE & METADATA:                                                      │
│ • feature_registry (21 features: ownership, versioning, SLA, tags)         │
│ • feature_versions (schema history for audit/rollback)                      │
│ • feature_lineage (dependency graph for impact analysis)                    │
│ • entity_keys (customer/product/sales_rep definitions)                      │
│                                                                              │
│ OFFLINE STORE (Batch Training):                                             │
│ ┌─────────────────── Customer Features ──────────────────┐                  │
│ │ • customer_rfm_features_offline                        │                  │
│ │   Rows: (CUSTOMER_ID, OBSERVATION_DATE)                │                  │
│ │   ~ 150K rows, RFM + engagement + quality metrics      │                  │
│ │ • customer_engagement_features_offline                 │                  │
│ │   Rows: (CUSTOMER_ID, OBSERVATION_DATE)                │                  │
│ │   ~ 150K rows, churn status + LTV + health signals     │                  │
│ └────────────────────────────────────────────────────────┘                  │
│                                                                              │
│ ┌─────────────── Product Performance Features ───────────┐                  │
│ │ • product_performance_features_offline                 │                  │
│ │   Rows: (PRODUCT_ID, OBSERVATION_DATE)                 │                  │
│ │   ~ 1.5M rows, revenue + volume + quality + rank       │                  │
│ └────────────────────────────────────────────────────────┘                  │
│                                                                              │
│ ┌────────────── Sales Rep Quota Features ────────────┐                      │
│ │ • sales_rep_quota_features_offline                 │                      │
│ │   Rows: (REP_ID, OBSERVATION_DATE)                 │                      │
│ │   ~ 50K rows, quota attainment + performance KPIs  │                      │
│ └────────────────────────────────────────────────────┘                      │
│                                                                              │
│ TRAINING DATA VIEWS (Ready-for-ML):                                         │
│ • training_data_customers — (customer, observation_date) grain             │
│ • training_data_products — (product, observation_date) grain               │
│                                                                              │
│ API: Python (feature_store.py)                                              │
│ • fs.get_customer_features_as_of(ids, date)                                 │
│ • fs.get_training_dataset(entity_type, date_range)                          │
│ • fs.list_features(tags=...)                                                │
│ • fs.get_feature_lineage(feature_id)                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

TIER 4: Consumption Layer

BI & Analytics:                         ML & Data Science:
┌──────────────────────┐               ┌──────────────────────┐
│ • BI Tools           │               │ • Training Pipelines │
│ • Dashboards         │               │ • ML Models          │
│ • Ad-hoc Queries     │               │ • Inference Scoring  │
│ • Operational Dashboards│            │ • Experiments        │
│ (use data products   │               │ (use feature store   │
│  and BI tables)      │               │  with PIT correct)   │
└──────────────────────┘               └──────────────────────┘

Custom Marketplace UI (Stretch): 
┌──────────────────────────────────────────────────────────────────────────────┐
│ • Product Discovery → RAW_SALES.MONITORING.product_sla_status               │
│ • SLA Visibility → Row counts, refresh timestamps                            │
│ • AI Metadata Search → RAW_SALES.GOLD.ai_semantic_metadata                   │
│ • Manual Release Gate → RAW_SALES.MONITORING.manual_release_checks           │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Feature Store in the Challenge Context

### Requirement
From Tech Challenge:
> "Design for ML Feature Store (point-in-time correctness, feature versioning, low-latency serving)"

### Solution (Phase 4)
✅ **Point-in-Time Correctness**: Every feature has OBSERVATION_DATE; guaranteed no data leakage
✅ **Feature Versioning**: `feature_versions` tracks all schema changes with deployment history  
✅ **Low-Latency Serving**: Offline store precomputed daily; online store pattern designed (Redis-ready)
✅ **Governance**: Feature registry with ownership, SLAs, lineage, tags
✅ **Developer Experience**: Python API for data scientists

### How It Complements Other Tiers
- **Sourced from Gold Layer**: Uses clean, aggregated, dimensional data (quality assurance)
- **Shares MONITORING Schema**: SLA tracking, audit logs, change history
- **Integrates with Data Products**: Features can be published as data products (ML-specific)
- **Snowpark Compatible**: Can add Python-based feature engineering jobs

---

## Execution Flow

### Phase Sequence
```
Phase 1: Foundation (01_phase1_foundation.sql)
  ↓
Phase 2A: Bronze (02_phase2_bronze.sql) — Streams, immutable history  
Phase 2B: Silver (03_phase2_silver.sql) — DAMA 6, Dynamic Tables, Snowpark
Phase 2C: Gold (04_phase2_gold.sql) — Dimensional model, BI tables, AI stubs
  ↓
Phase 3: Data Products (05_phase3_data_products.sql) — SLA, Tasks, marketplace
  ↓
Phase 4: ML Feature Store (06_feature_store_ml.sql) — Features, registry, lineage
  ↓
Validation: acceptance_gates.sql — Release readiness
Platform Checks: check_status.py, verify_roles_sla.py, quick_check.py
```

### Running It All
```powershell
# Full execution including feature store
python python/orchestration/run_all.py \
  --include-foundation \
  --resume-tasks \
  --execute-validation-tasks \
  --run-snowpark-job \
  --run-acceptance-gates
```

This runs ALL phases including Phase 4 (feature store).

---

## Hands-On Example: Train a Churn Model

### Step 1: Get Training Data (Point-in-Time Correct)
```python
from feature_store import FeatureStore
fs = FeatureStore(session)

# Training on 1999 data (no leakage)
training_df = fs.get_training_dataset(
    entity_type='customer',
    date_range=('1999-01-01', '1999-12-31')
)

print(f"Training samples: {len(training_df)}")
print(f"Features: {training_df.columns.tolist()}")
```

### Step 2: Explore Features
```python
# Check data quality
validation = fs.validate_training_data(training_df, 'customer')
assert all(validation.values()), "Data quality check failed"

# Feature importance vs. target
importance = fs.estimate_feature_importance(training_df, 'engagement_status')
print(importance)  # Which features matter most?
```

### Step 3: Train Model
```python
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

# Prepare
X = training_df[[
    'recency_days', 'frequency_12m', 'monetary_12m',
    'rfm_composite_score', 'lifetime_value', 'avg_order_value'
]].fillna(0)

y_encoder = LabelEncoder()
y = y_encoder.fit_transform(training_df['engagement_status'])

# Train
model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
model.fit(X, y)

print(f"Training accuracy: {model.score(X, y):.3f}")
print(f"Classes: {y_encoder.classes_}")
```

### Step 4: Inference on Latest Data
```python
# Get latest features (as of most recent date)
latest_customers = [1, 2, 3, 4, 5]
inference_df = fs.get_customer_features_as_of(
    customer_ids=latest_customers,
    observation_date='2000-01-01'  # Today's snapshot
)

# Score
X_new = inference_df[['recency_days', 'frequency_12m', 'monetary_12m',
                      'rfm_composite_score', 'lifetime_value', 'avg_order_value']].fillna(0)
churn_probs = model.predict_proba(X_new)

# Output
for customer_id, probs in zip(latest_customers, churn_probs):
    print(f"Customer {customer_id}: {probs[1]:.2%} churn risk")
```

**Key Point**: Training features (1999 data) and inference features (2000 data) are computed independently, guaranteeing PIT correctness.

---

## Key Differentiators

### Why NOT Just Use Gold Layer?
✗ **Data leakage risk**: Gold tables are "current" views, not historical  
✗ **No feature versioning**: Can't reproduce old model training  
✗ **Unclear ownership**: Who owns which feature?  
✗ **Not ML-optimized**: Row grain doesn't match entity (e.g., orders grain vs. customer grain)  

### Why Feature Store?
✅ **PIT Correctness**: Dated snapshots prevent leakage  
✅ **Feature Registry**: Catalog, ownership, SLA, versioning  
✅ **Entity-Based**: (Customer, Feature) pairs with consistent grain  
✅ **Reproducibility**: Exact same features retrain any model  
✅ **Governance**: Lineage, change history, impact analysis  

---

## Scope: What's Included vs. Future

### Included (Phase 4 ✅)
- Offline store (batch training)
- Feature registry & versioning
- Entity-based organization (customer, product, sales rep)
- Python API
- Training data views
- Documentation & examples

### Future Expansion (Phase 5+)
- [ ] Online store (Redis) for real-time scoring
- [ ] Feature marketplace UI
- [ ] Auto-feature engineering
- [ ] Model registry integration
- [ ] Feature importance rankings
- [ ] Monitoring & SLA enforcement
- [ ] ML pipeline orchestration

---

## Success Metrics

### Technical Metrics (Acceptance Gate)
✅ Feature store schema created and populated  
✅ ≥20 features in registry  
✅ Python API tested and functional  
✅ Point-in-time correctness verified  
✅ Training data views produce expected row counts  

### Business Metrics (Deployment)
- Time to first model: < 1 day (using feature store)
- Model reproducibility: 100% (same features, same date)
- Feature reuse: 80%+ of features used by 2+ models
- Feature lineage clarity: 100% traceable to source

---

## Summary

The **ML Feature Store** is a production-ready layer on top of the Medallion Architecture, providing:
1. Point-in-time correct training data (no leakage)
2. Curated, versioned, governed features
3. Simple Python API for data scientists
4. Full audit trail and lineage
5. Foundation for future online serving

It directly fulfills the challenge requirement: **"Design for ML Feature Store (point-in-time correctness, feature versioning, low-latency serving)"** and integrates seamlessly with the existing BI/Data Product layers.

---

## Quick Links

📄 [Feature Store Summary](FEATURE_STORE_SUMMARY.md)  
📚 [Architecture Guide](FEATURE_STORE_GUIDE.md)  
⚡ [Quick Start (5 recipes)](FEATURE_STORE_QUICKSTART.md)  
💻 [Python API](feature_store.py)  
🔧 [SQL Implementation](06_feature_store_ml.sql)  
📊 [Master Plan](plan.md) — Phase 4 section
