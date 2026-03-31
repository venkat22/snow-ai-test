# ML Feature Store — Quick Start for Data Scientists

## TL;DR

The ML Feature Store provides **point-in-time correct features** for training machine learning models. Features are pre-computed, versioned, and ready to use.

### 5-Minute Setup

```python
# 1. Import and initialize
from feature_store import FeatureStore
from snowflake.snowpark.session import Session

session = Session.builder.config(...).create()
fs = FeatureStore(session)

# 2. Get customer features as of a historical date
customers_df = fs.get_training_dataset(
    entity_type='customer',
    date_range=('1999-01-01', '1999-12-31')
)

# 3. Train your model
from sklearn.ensemble import RandomForestClassifier
X = customers_df[['recency_days', 'frequency_12m', 'monetary_12m', 'rfm_composite_score']]
y = customers_df['engagement_status']
model = RandomForestClassifier()
model.fit(X, y)
```

Done! You now have a model trained on point-in-time correct data.

---

## What's Available

### 3 Entity Types

#### 1. Customer Features
**Table**: `customer_rfm_features_offline`  
**Key Features**: 
- `recency_days`, `frequency_12m`, `monetary_12m` (RFM)
- `rfm_composite_score` (1-15)
- `estimated_segment` (VIP | Loyal | At-Risk | Inactive | Engaged)

**Plus Engagement**:
- `days_since_last_purchase`, `engagement_status` (Active | Dormant | At-Risk | Churned)
- `lifetime_value`, `avg_order_value`

**Use Cases**: Churn prediction, LTV modeling, customer segmentation, propensity scoring

#### 2. Product Features
**Table**: `product_performance_features_offline`  
**Key Features**:
- Revenue: `cumulative_revenue`, `revenue_12m`, `revenue_rank`
- Volume: `cumulative_units_sold`, `units_sold_12m`
- Quality: `return_rate_pct`, `total_returned_items`

**Use Cases**: Demand forecasting, product health scoring, pricing optimization

#### 3. Sales Rep Features
**Table**: `sales_rep_quota_features_offline`  
**Key Features**:
- `quota`, `ytd_revenue`, `quota_attainment_ratio`, `quota_attainment_pct`
- `ytd_orders`, `ytd_customer_count`

**Use Cases**: Rep performance prediction, quota forecasting, territory planning

---

## Common Workflows

### Workflow 1: Customer Churn Prediction (Training)

```python
# Get 1-year of training data
training_df = fs.get_training_dataset(
    entity_type='customer',
    date_range=('1998-06-01', '1999-06-01'),
    sample_fraction=1.0  # Use all rows
)

# Features suitable for churn model:
# - recency_days (days since last purchase)
# - frequency_12m (purchase frequency)
# - monetary_12m (spending)
# - engagement_status (early churn signal)
# - lifetime_value (customer worth)

# Check data quality
validation = fs.validate_training_data(training_df, 'customer')
assert validation['has_rows'], "No training data found"
assert validation['no_nulls_in_keys'], "Nulls in entity keys"

# Train model
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier

encoder = LabelEncoder()
y = encoder.fit_transform(training_df['engagement_status'])
X = training_df[[
    'recency_days', 'frequency_12m', 'monetary_12m', 
    'lifetime_value', 'avg_order_value'
]].fillna(0)

model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
model.fit(X, y)

print(f"Model trained with {len(training_df)} samples")
print(f"Feature importance: {model.feature_importances_}")
```

### Workflow 2: Product Demand Forecasting (Training)

```python
# Get product features for a date range
training_df = fs.get_training_dataset(
    entity_type='product',
    date_range=('1998-01-01', '1999-12-31')
)

# Prepare for time-series forecasting
training_df_sorted = training_df.sort_values(['PRODUCT_ID', 'OBSERVATION_DATE'])

# Example: forecast next month's revenue per product
from sklearn.linear_model import LinearRegression

model = LinearRegression()
for product_id in training_df_sorted['PRODUCT_ID'].unique():
    product_data = training_df_sorted[training_df_sorted['PRODUCT_ID'] == product_id]
    if len(product_data) < 10:
        continue
    
    X = product_data[['revenue_12m', 'units_sold_12m', 'return_rate_pct']].values[:-1]
    y = product_data['cumulative_revenue'].values[1:]
    
    model.fit(X, y)
    print(f"Product {product_id}: R² = {model.score(X, y):.3f}")
```

### Workflow 3: Real-Time Scoring (Inference)

```python
# Get latest features for a batch of customers
latest_customers = [1, 2, 3, 4, 5]
latest_date = '2000-01-01'  # Today's snapshot

features_df = fs.get_customer_features_as_of(
    customer_ids=latest_customers,
    observation_date=latest_date
)

# Score with trained model
scores = model.predict_proba(features_df[['recency_days', 'frequency_12m', 'monetary_12m']].fillna(0))

# Output: churn probability per customer
for customer_id, churn_prob in zip(latest_customers, scores[:, 1]):
    print(f"Customer {customer_id}: {churn_prob:.2%} churn risk")
```

---

## Feature Metadata

### Find Features by Tag

```python
# All revenue-related features
revenue_features = fs.list_features(tags='revenue,customer-lifetime-value')
print(revenue_features[['FEATURE_ID', 'FEATURE_NAME', 'OWNER_TEAM']])
```

### Understand Feature Lineage

```python
# Where does estimated_segment come from?
lineage = fs.get_feature_lineage('cust_segment_rfm')
print(lineage)
# Output: It depends on cust_rfm_composite_score
#         which depends on recency_days, frequency_12m, monetary_12m

# Impact analysis: if FACT_ORDERS is wrong, which features are affected?
```

### Feature Importance Quick Check

```python
# Quick feature-target correlation
importance = fs.estimate_feature_importance(
    training_df=training_df,
    target_column='engagement_status'
)
print(importance)
# Output: sorted by |correlation|
```

---

## Point-in-Time Correctness: What It Means

### ❌ WRONG (Data Leakage)
```python
# This uses information from the FUTURE
customer_df = session.sql(
    "SELECT * FROM DIM_CUSTOMERS WHERE CUSTOMER_ID = 123"
).to_pandas()
# ↑ This includes data up to TODAY, not the training date
```

### ✅ CORRECT (PIT Correct)
```python
# This uses only information available on 1999-01-01
customer_df = fs.get_customer_features_as_of(
    customer_ids=[123],
    observation_date='1999-01-01'
)
# ↑ Guaranteed to not include any data after 1999-01-01
```

**Why it matters**: 
- Without PIT correctness, model overfits to future data
- When you deploy the model, performance drops (leakage didn't exist then)
- Reproducibility fails: can't retrain old models consistently

---

## Common Issues & Solutions

### Issue: "Feature returns all NULLs"
**Cause**: Customer didn't exist on the observation date  
**Solution**: Filter for entities that existed at that date
```python
# Only customers with orders before observation date
training_df = training_df[
    training_df['OBSERVATION_DATE'] >= training_df['FIRST_ORDER_DATE']
]
```

### Issue: "Different number of rows than expected"
**Cause**: Some customers/products have no activity on some dates  
**Solution**: Use OUTER JOINs or sparse feature handling
```python
# Fill sparse values
training_df = training_df.fillna({
    'recency_days': 999,  # Very long recency if no orders
    'frequency_12m': 0,
    'monetary_12m': 0
})
```

### Issue: "Model scores don't match training"
**Cause**: Using different observation date in scoring vs. training  
**Solution**: Always align training and scoring dates
```python
# Training done on 1999-01-01 data
# Scoring must reference the SAME feature snapshot

TRAINING_DATE = '1999-01-01'
SCORING_DATE = '1999-01-01'  # Use same date!

training_df = fs.get_training_dataset(entity_type='customer', date_range=(TRAINING_DATE, TRAINING_DATE))
scoring_df = fs.get_customer_features_as_of(customer_ids=[...], observation_date=SCORING_DATE)
```

---

## Next Steps

### 1. Explore the Feature Registry
```sql
SELECT FEATURE_ID, FEATURE_NAME, DATA_TYPE, OWNER_TEAM, TAGS
FROM RAW_SALES.FEATURE_STORE.feature_registry
ORDER BY FEATURE_NAME;
```

### 2. Sample Training Data
```python
sample = fs.get_training_dataset(
    entity_type='customer',
    date_range=('1999-01-01', '1999-01-31'),
    sample_fraction=0.1  # Just 10% for quick look
)
sample.head()
sample.describe()
```

### 3. Build a Model
Use your favorite ML library (scikit-learn, XGBoost, LightGBM, etc.)

### 4. Deploy Responsibly
- Always use the same observation date for scoring as training
- Monitor model performance vs. training metrics
- Re-train periodically with new data

---

## Support & Documentation

**Full Architecture Guide**: See `FEATURE_STORE_GUIDE.md`  
**SQL Examples**: See `06_feature_store_ml.sql`  
**Python API**: See `feature_store.py` docstrings  

Questions? Contact: Data Science / ML team
