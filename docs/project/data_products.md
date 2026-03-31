# Phase 3: Data Products & Governance

**Goal**: Design 3 curated data products with SLAs, governance, and publish to Snowflake Marketplace.

**Timeline**: Days 15-20 (5 days total)

---

## Product 1: Sales Rep Monthly Performance Dashboard

### Product Overview
- **Name**: `sales_rep_monthly_performance`
- **Purpose**: Enable sales managers to monitor rep performance, quota attainment, and customer engagement month-over-month
- **Audience**: Sales managers, sales leadership, ops teams
- **Update Frequency**: Daily (by 9 AM business day)

### Input Data Contract
| Source Table | Schema | Key Columns |
|---|---|---|
| `silver.orders` | RAW_SALES | ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_AMOUNT |
| `silver.sales_reps` | RAW_SALES | REP_ID, NAME, TERRITORY, QUOTA |
| `silver.territories` | RAW_SALES | TERRITORY_ID, TERRITORY_NAME, REGION |

### Output Schema

```sql
CREATE TABLE IF NOT EXISTS RAW_SALES.GOLD.sales_rep_monthly_performance AS
SELECT
  EXTRACT(YEAR FROM O.ORDER_DATE) AS YEAR,
  EXTRACT(MONTH FROM O.ORDER_DATE) AS MONTH,
  TO_CHAR(O.ORDER_DATE, 'MMMM YYYY') AS MONTH_NAME,
  SR.REP_ID,
  SR.NAME AS REP_NAME,
  SR.TERRITORY,
  SR.QUOTA,
  COUNT(DISTINCT O.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
  COUNT(DISTINCT O.ORDER_ID) AS ORDERS_CLOSED,
  SUM(O.ORDER_AMOUNT) AS REVENUE_GENERATED,
  ROUND(SUM(O.ORDER_AMOUNT) / NULLIF(SR.QUOTA, 0), 2) AS QUOTA_ATTAINMENT_RATIO,
  ROUND(SUM(O.ORDER_AMOUNT) / NULLIF(SR.QUOTA, 0) * 100, 1) AS QUOTA_ATTAINMENT_PCT,
  ROUND(AVG(O.ORDER_AMOUNT), 2) AS AVG_ORDER_VALUE,
  ROUND(MAX(O.ORDER_AMOUNT), 2) AS LARGEST_ORDER,
  LAG(SUM(O.ORDER_AMOUNT)) OVER (
    PARTITION BY SR.REP_ID ORDER BY EXTRACT(YEAR FROM O.ORDER_DATE), EXTRACT(MONTH FROM O.ORDER_DATE)
  ) AS PREV_MONTH_REVENUE,
  CASE
    WHEN SUM(O.ORDER_AMOUNT) / NULLIF(SR.QUOTA, 0) >= 1.0 THEN 'Exceeds'
    WHEN SUM(O.ORDER_AMOUNT) / NULLIF(SR.QUOTA, 0) >= 0.8 THEN 'On Track'
    WHEN SUM(O.ORDER_AMOUNT) / NULLIF(SR.QUOTA, 0) >= 0.5 THEN 'Below Target'
    ELSE 'At Risk'
  END AS PERFORMANCE_STATUS,
  CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM RAW_SALES.GOLD.fact_orders F
JOIN RAW_SALES.GOLD.dim_dates D ON F.DATE_KEY = D.DATE_KEY
JOIN RAW_SALES.GOLD.dim_sales_reps SR ON F.REP_ID = SR.REP_ID
GROUP BY D.YEAR, D.MONTH, TO_CHAR(D.DATE_KEY, 'MMMM YYYY'), SR.REP_ID, SR.NAME, SR.TERRITORY_NAME, SR.QUOTA;
```

### SLAs (Service Level Agreements)

| SLA Dimension | Target | Alert Threshold |
|---|---|---|
| **Freshness** | Daily refresh by 9 AM ET | Alert if not refreshed by 10 AM ET |
| **Latency** | <1 second query response | Alert if >3 seconds |
| **Completeness** | 100% of orders from prior month included | Alert if <95% |
| **Accuracy** | Sum of revenue matches SILVER.ORDERS ±$1 | Alert if variance >$10 |
| **Availability** | 99.5% uptime | Alert if 2+ hours downtime in month |

### Ownership & Governance

| Role | Responsibility |
|---|---|
| **Owner** | Sales Operations Manager (John Doe, john@company.com) |
| **Backup** | Sales Operations Lead (Jane Smith, jane@company.com) |
| **SLA Escalation** | VP Sales (escalate if SLA missed) |
| **Maintenance Window** | No scheduled downtime (refresh during business hours only) |

### Data Quality Thresholds

**Pre-refresh validation checks:**
```sql
-- Check 1: SILVER.ORDERS must have > 0 rows for current month
SELECT COUNT(*) AS CURRENT_MONTH_ORDERS
FROM RAW_SALES.SILVER.ORDERS
WHERE EXTRACT(YEAR FROM ORDER_DATE) = EXTRACT(YEAR FROM TODAY())
  AND EXTRACT(MONTH FROM ORDER_DATE) = EXTRACT(MONTH FROM TODAY());

-- Alert if result = 0; skip refresh

-- Check 2: SILVER.SALES_REPS must have all active reps
SELECT COUNT(*) AS ACTIVE_REPS
FROM RAW_SALES.SILVER.SALES_REPS
WHERE STATUS = 'Active';

-- Alert if result < prior month by >10%

-- Check 3: Total revenue must match SILVER within tolerance
SELECT ABS(
  (SELECT SUM(ORDER_AMOUNT) FROM RAW_SALES.SILVER.ORDERS WHERE EXTRACT(MONTH FROM ORDER_DATE) = EXTRACT(MONTH FROM CURRENT_DATE()))
  -
  (SELECT SUM(REVENUE_GENERATED) FROM RAW_SALES.GOLD.sales_rep_monthly_performance WHERE MONTH = EXTRACT(MONTH FROM CURRENT_DATE()))
) AS VARIANCE;

-- Alert if VARIANCE > $50
```

### Access Control

**Snowflake roles:**
```sql
-- Create role
CREATE ROLE IF NOT EXISTS SALES_ANALYSTS;

-- Grant read access
GRANT SELECT ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance TO ROLE SALES_ANALYSTS;

-- Assign to users
GRANT ROLE SALES_ANALYSTS TO USER 'john@company.com';
GRANT ROLE SALES_ANALYSTS TO USER 'jane@company.com';

-- Verify
SHOW GRANTS ON TABLE RAW_SALES.GOLD.sales_rep_monthly_performance;
```

---

## Product 2: Customer Revenue Forecast

### Product Overview
- **Name**: `customer_revenue_forecast`
- **Purpose**: Provide 12-month revenue projection for strategic planning and forecasting
- **Audience**: Finance team, CFO, strategic planning
- **Update Frequency**: Monthly (by end of month)

### Input Data Contract
| Source | Schema | Purpose |
|---|---|---|
| `gold.dim_customers` | RAW_SALES | Customer lifetime value, segment |
| `gold.customer_lifetime_value` | RAW_SALES | Historical spending patterns |
| `silver.orders` | RAW_SALES | Order history for trend analysis |

### Output Schema

```sql
CREATE TABLE IF NOT EXISTS RAW_SALES.GOLD.customer_revenue_forecast AS
WITH customer_trends AS (
  SELECT
    C.CUSTOMER_ID,
    C.NAME,
    C.SEGMENT,
    C.LIFETIME_VALUE,
    COUNT(DISTINCT O.ORDER_ID) AS TOTAL_ORDERS_HISTORY,
    AVG(O.ORDER_AMOUNT) AS AVG_ORDER_VALUE,
    DATEDIFF(MONTH, MIN(O.ORDER_DATE), MAX(O.ORDER_DATE)) + 1 AS MONTHS_ACTIVE,
    ROUND(SUM(O.ORDER_AMOUNT) / NULLIF(DATEDIFF(MONTH, MIN(O.ORDER_DATE), MAX(O.ORDER_DATE)) + 1, 0), 2) AS AVG_MONTHLY_REVENUE
  FROM RAW_SALES.GOLD.dim_customers C
  LEFT JOIN RAW_SALES.SILVER.orders O ON C.CUSTOMER_ID = O.CUSTOMER_ID
  GROUP BY 1, 2, 3, 4
)
SELECT
  CUSTOMER_ID,
  NAME,
  SEGMENT,
  LIFETIME_VALUE,
  MONTHS_ACTIVE,
  AVG_MONTHLY_REVENUE,
  ROUND(AVG_MONTHLY_REVENUE * 12, 2) AS PROJECTED_12MONTH_REVENUE,
  ROUND(AVG_MONTHLY_REVENUE * 12 * 0.95, 2) AS CONSERVATIVE_FORECAST,  -- 95% confidence
  ROUND(AVG_MONTHLY_REVENUE * 12 * 1.05, 2) AS OPTIMISTIC_FORECAST,   -- 105% confidence
  CASE
    WHEN AVG_MONTHLY_REVENUE IS NULL THEN 'Inactive'
    WHEN AVG_MONTHLY_REVENUE < 100 THEN 'Low'
    WHEN AVG_MONTHLY_REVENUE < 500 THEN 'Medium'
    ELSE 'High'
  END AS FORECAST_TIER,
  CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM customer_trends;
```

### SLAs

| SLA Dimension | Target |
|---|---|
| **Freshness** | Monthly refresh (last day of month +2 days) |
| **Latency** | <5 seconds |
| **Completeness** | ≥95% of active customers covered |
| **Accuracy** | Holdout test: MAPE < 10% on prior month forecast |

### Ownership & Governance
- **Owner**: Finance Controller (finance@company.com)
- **Escalation**: CFO
- **Refresh Day**: 2nd business day of month

### Quality Checks
```sql
-- Min records check
SELECT COUNT(*) FROM RAW_SALES.GOLD.customer_revenue_forecast
WHERE PROJECTED_12MONTH_REVENUE > 0;
-- Alert if < 100 records

-- Forecast reasonableness
SELECT
  COUNT(CASE WHEN PROJECTED_12MONTH_REVENUE > 1000000 THEN 1 END) OUTLIERS,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY PROJECTED_12MONTH_REVENUE) AS P99
FROM RAW_SALES.GOLD.customer_revenue_forecast;
-- Alert if P99 > $500K (likely data quality issue)
```

---

## Product 3: Customer Acquisition Cohort Analysis

### Product Overview
- **Name**: `customer_acquisition_cohort`
- **Purpose**: Track cohort-level retention, spend, and ROI by acquisition month for marketing effectiveness
- **Audience**: Marketing, product, growth team
- **Update Frequency**: Weekly

### Input Data Contract
| Source | Purpose |
|---|---|
| `gold.dim_customers` | Acquisition date, LTV |
| `silver.orders` | Order timing for cohort analysis |

### Output Schema

```sql
CREATE TABLE IF NOT EXISTS RAW_SALES.GOLD.customer_acquisition_cohort AS
WITH cohorts AS (
  SELECT
    DATE_TRUNC('MONTH', C.FIRST_ORDER_DATE) AS COHORT_MONTH,
    EXTRACT(MONTH FROM O.ORDER_DATE) AS ORDER_MONTH_RELATIVE,
    DATEDIFF(MONTH, C.FIRST_ORDER_DATE, O.ORDER_DATE) + 1 AS MONTHS_SINCE_ACQUISITION,
    COUNT(DISTINCT C.CUSTOMER_ID) AS COHORT_SIZE,
    COUNT(DISTINCT CASE WHEN O.ORDER_DATE IS NOT NULL THEN C.CUSTOMER_ID END) AS ACTIVE_CUSTOMERS,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN O.ORDER_DATE IS NOT NULL THEN C.CUSTOMER_ID END) / COUNT(DISTINCT C.CUSTOMER_ID), 1) AS RETENTION_PCT,
    SUM(O.ORDER_AMOUNT) AS COHORT_REVENUE,
    ROUND(SUM(O.ORDER_AMOUNT) / COUNT(DISTINCT C.CUSTOMER_ID), 2) AS LTV_PER_ACQUIREE
  FROM RAW_SALES.GOLD.dim_customers C
  LEFT JOIN RAW_SALES.SILVER.orders O ON C.CUSTOMER_ID = O.CUSTOMER_ID
  WHERE C.FIRST_ORDER_DATE IS NOT NULL
  GROUP BY 1, 2, 3
)
SELECT
  COHORT_MONTH,
  MONTHS_SINCE_ACQUISITION,
  COHORT_SIZE,
  ACTIVE_CUSTOMERS,
  RETENTION_PCT,
  COHORT_REVENUE,
  LTV_PER_ACQUIREE,
  CURRENT_TIMESTAMP() AS _REFRESHED_AT
FROM cohorts
ORDER BY COHORT_MONTH DESC, MONTHS_SINCE_ACQUISITION;
```

### SLAs

| SLA Dimension | Target |
|---|---|
| **Freshness** | Weekly (every Monday 8 AM) |
| **Latency** | <100ms |
| **Completeness** | 100% of customers with acquisition date |
| **Availability** | 99.9% uptime |

### Ownership & Governance
- **Owner**: VP Marketing (vp.marketing@company.com)
- **Escalation**: Chief Marketing Officer
- **Refresh Day**: Monday 8 AM

---

## Snowflake Data Marketplace Listing

### Listing Title
**"Sales Analytics Data Bundle"**

### Listing Description

```
Curated data products for sales forecasting, performance management, and marketing analytics.

## What's Included

1. **Sales Rep Monthly Performance** - Daily-refreshing KPIs for sales management
   - Rep quota attainment, revenue generated, customer count
   - Updated daily, <1 second latency
   
2. **Customer Revenue Forecast** - 12-month revenue projections by customer
   - Includes conservative and optimistic scenarios
   - 95% historical accuracy (holdout tested)
   
3. **Customer Acquisition Cohort** - Retention and LTV by acquisition month
   - Track marketing campaign effectiveness
   - Weekly refresh

## Data Freshness
- Sales Rep Performance: Updated daily by 9 AM
- Revenue Forecast: Updated monthly
- Acquisition Cohort: Updated weekly

## Sample Schema

### sales_rep_monthly_performance
- rep_id: Sales rep identifier
- rep_name: Rep name
- territory: Territory assigned
- revenue_generated: Monthly revenue
- quota_attainment_pct: % of quota achieved
- performance_status: Exceeds | On Track | Below Target | At Risk

### customer_revenue_forecast
- customer_id: Customer identifier
- projected_12month_revenue: Revenue projection
- conservative_forecast: 95% confidence
- optimistic_forecast: 105% confidence
- forecast_tier: Low | Medium | High

### customer_acquisition_cohort
- cohort_month: Acquisition month
- months_since_acquisition: Months from first order
- retention_pct: % of cohort still active
- ltv_per_acquiree: Lifetime value per customer

## Use Cases
- Monitor sales team performance in real-time
- Forecast revenue for financial planning
- Measure marketing campaign ROI by cohort
- Identify at-risk customers for retention marketing
- Benchmark territory performance

## Support
Questions? Contact: data-products@company.com
```

### Sample Data (for preview in Marketplace)

```sql
-- Sample rows visible in marketplace preview (10 rows each)

-- Sales Rep Performance Sample
SELECT TOP 10
  YEAR, MONTH, MONTH_NAME, REP_ID, REP_NAME, TERRITORY,
  REVENUE_GENERATED, QUOTA_ATTAINMENT_PCT, PERFORMANCE_STATUS
FROM RAW_SALES.GOLD.sales_rep_monthly_performance
ORDER BY YEAR DESC, MONTH DESC;

-- Customer Forecast Sample
SELECT TOP 10
  CUSTOMER_ID, NAME, SEGMENT,
  PROJECTED_12MONTH_REVENUE, CONSERVATIVE_FORECAST, OPTIMISTIC_FORECAST
FROM RAW_SALES.GOLD.customer_revenue_forecast
ORDER BY PROJECTED_12MONTH_REVENUE DESC;

-- Cohort Analysis Sample
SELECT TOP 10
  COHORT_MONTH, MONTHS_SINCE_ACQUISITION,
  COHORT_SIZE, RETENTION_PCT, LTV_PER_ACQUIREE
FROM RAW_SALES.GOLD.customer_acquisition_cohort
ORDER BY COHORT_MONTH DESC, MONTHS_SINCE_ACQUISITION;
```

### Publishing Steps (in Snowflake UI)

1. **Go to Data > Listings**
2. Click **+ New Listing**
3. Fill in details:
   - **Title**: Sales Analytics Data Bundle
   - **Description**: (use above)
   - **Category**: Sales & CRM
   - **Pricing**: Free
4. **Add data products**:
   - Add table: `RAW_SALES.GOLD.sales_rep_monthly_performance`
   - Add table: `RAW_SALES.GOLD.customer_revenue_forecast`
   - Add table: `RAW_SALES.GOLD.customer_acquisition_cohort`
5. **Configure sharing**:
   - Listing visibility: **Public**
   - Require approval: No
6. **Contact info**:
   - Email: data-products@company.com
   - Support link: (optional)
7. Click **Publish**

---

## Testing Consumer Flow

### Step 1: Create Second Trial Account (as Consumer)
1. Use different email address
2. Sign up for Snowflake trial at https://signup.snowflake.com
3. Complete setup (warehouse, etc.)

### Step 2: Subscribe to Listing (in Consumer Account)
1. Go to **Data > Browse Listings**
2. Search for "Sales Analytics Data Bundle"
3. Click listing
4. Click **Request Access** or **Subscribe**
5. Wait for approval (if required)
6. Click **Install** once approved

### Step 3: Verify Data Access (in Consumer Account)
```sql
-- Browse shared databases
SHOW DATABASES;

-- Should see: <PROVIDER>_RAW_SALES or similar

-- Query the data
SELECT * FROM <SHARED_DB>.GOLD.sales_rep_monthly_performance LIMIT 10;
SELECT * FROM <SHARED_DB>.GOLD.customer_revenue_forecast LIMIT 10;
SELECT * FROM <SHARED_DB>.GOLD.customer_acquisition_cohort LIMIT 10;

-- Both accounts should see same data ✓
```

---

## Governance Documentation

### Data Dictionary Template

| Field Name | Data Type | Description | Source | Sensitivity |
|---|---|---|---|---|
| `rep_id` | STRING | Unique rep identifier | SILVER.SALES_REPS.REP_ID | Internal |
| `rep_name` | STRING | Rep full name | SILVER.SALES_REPS.NAME | Internal |
| `revenue_generated` | DECIMAL | Monthly revenue sum | SILVER.ORDERS.ORDER_AMOUNT | Internal |
| `quota_attainment_pct` | DECIMAL | % of quota achieved | Calculated | Internal |

### Refresh Procedure

```sql
-- Execute this daily for sales_rep_monthly_performance

USE DATABASE RAW_SALES;
USE SCHEMA GOLD;

-- Pre-validation
BEGIN TRANSACTION;

-- Check: Orders exist for current month
IF (SELECT COUNT(*) FROM SILVER.ORDERS 
    WHERE MONTH(ORDER_DATE) = MONTH(TODAY())) < 1
THEN
  RAISE EXCEPTION 'No orders found for current month - aborting refresh';
END IF;

-- Refresh
TRUNCATE TABLE sales_rep_monthly_performance;

INSERT INTO sales_rep_monthly_performance
SELECT ... FROM ... WHERE ...;

-- Post-validation
IF (SELECT COUNT(*) FROM sales_rep_monthly_performance) < 
   (SELECT COUNT(*) FROM sales_rep_monthly_performance WHERE DATE(_REFRESHED_AT) = TODAY() - 1)
THEN
  RAISE EXCEPTION 'Row count decreased - possible data loss';
END IF;

COMMIT;

-- Log completion
INSERT INTO MONITORING.data_quality_log
VALUES (NULL, CURRENT_TIMESTAMP(), 'sales_rep_monthly_performance', 'Freshness', 'Daily refresh', 
        (SELECT COUNT(*) FROM sales_rep_monthly_performance), 0, 100.0, 'PASS');
```

### SLA Monitoring Dashboard (Optional)

```sql
-- Monitor all product SLAs
CREATE VIEW RAW_SALES.MONITORING.product_sla_status AS
SELECT
  'sales_rep_monthly_performance' PRODUCT_NAME,
  MAX(_REFRESHED_AT) LAST_REFRESH,
  DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) HOURS_SINCE_REFRESH,
  CASE WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 24 THEN 'PASS' ELSE 'FAIL' END SLA_STATUS
FROM RAW_SALES.GOLD.sales_rep_monthly_performance
UNION ALL
SELECT 'customer_revenue_forecast', MAX(_REFRESHED_AT), DATEDIFF(DAY, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()),
  CASE WHEN DATEDIFF(DAY, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 35 THEN 'PASS' ELSE 'FAIL' END
FROM RAW_SALES.GOLD.customer_revenue_forecast
UNION ALL
SELECT 'customer_acquisition_cohort', MAX(_REFRESHED_AT), DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()),
  CASE WHEN DATEDIFF(HOUR, MAX(_REFRESHED_AT), CURRENT_TIMESTAMP()) <= 168 THEN 'PASS' ELSE 'FAIL' END
FROM RAW_SALES.GOLD.customer_acquisition_cohort;
```

---

## Verification Checklist (Phase 3 Complete)

- [ ] 3 data products created (GOLD layer tables)
- [ ] SLAs defined and documented for each product
- [ ] Data dictionary created
- [ ] Ownership assigned (owner, backup, escalation)
- [ ] Access control configured (roles created, users assigned)
- [ ] Refresh procedures documented
- [ ] Snowflake Marketplace listing published
- [ ] Consumer test successful (second account can query)
- [ ] Sample queries run successfully
- [ ] Data quality thresholds set
