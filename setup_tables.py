import snowflake.connector

pw = 'ChangeMe' + chr(33) + 'Str0ng' + chr(35) + '2026'

conn = snowflake.connector.connect(
    account='hhtxheq-ba04062',
    user='app_service_user',
    password=pw,
    role='SYSADMIN',
    warehouse='COMPUTE_WH',
)
cur = conn.cursor()

stmts = [
    # --- Grant SELECT on all existing MONITORING tables to APP_ROLE ---
    "GRANT SELECT ON ALL TABLES IN SCHEMA RAW_SALES.MONITORING TO ROLE APP_ROLE",
    "GRANT INSERT, UPDATE ON TABLE RAW_SALES.MONITORING.MANUAL_RELEASE_CHECKS TO ROLE APP_ROLE",

    # --- Create missing GOLD schema if needed ---
    "CREATE SCHEMA IF NOT EXISTS RAW_SALES.GOLD",
    "GRANT USAGE ON SCHEMA RAW_SALES.GOLD TO ROLE APP_ROLE",

    # --- Create FEATURE_STORE schema ---
    "CREATE SCHEMA IF NOT EXISTS RAW_SALES.FEATURE_STORE",
    "GRANT USAGE ON SCHEMA RAW_SALES.FEATURE_STORE TO ROLE APP_ROLE",

    # --- Create product_sla_status table ---
    """CREATE TABLE IF NOT EXISTS RAW_SALES.MONITORING.PRODUCT_SLA_STATUS (
        PRODUCT_NAME VARCHAR,
        OWNER VARCHAR,
        REFRESH_FREQUENCY VARCHAR,
        LAST_REFRESHED_AT TIMESTAMP_NTZ,
        HOURS_SINCE_REFRESH NUMBER,
        SLA_STATUS VARCHAR,
        CURRENT_ROW_COUNT NUMBER
    )""",

    # --- Seed product_sla_status with sample data ---
    """INSERT INTO RAW_SALES.MONITORING.PRODUCT_SLA_STATUS
    SELECT * FROM VALUES
        ('DAILY_REVENUE_SUMMARY','data-eng','daily',DATEADD(hour,-2,CURRENT_TIMESTAMP()),2,'green',145200),
        ('CUSTOMER_SEGMENTS','analytics','weekly',DATEADD(hour,-26,CURRENT_TIMESTAMP()),26,'yellow',89340),
        ('ORDER_ITEMS_CLEAN','data-eng','daily',DATEADD(hour,-1,CURRENT_TIMESTAMP()),1,'green',2340100),
        ('PRODUCT_PERFORMANCE','analytics','daily',DATEADD(hour,-50,CURRENT_TIMESTAMP()),50,'red',67800),
        ('MONTHLY_SALES_SUMMARY','finance','monthly',DATEADD(hour,-6,CURRENT_TIMESTAMP()),6,'green',12400),
        ('MARKETING_ATTRIBUTION','marketing','daily',DATEADD(hour,-4,CURRENT_TIMESTAMP()),4,'green',234500),
        ('INVENTORY_FORECAST','supply-chain','daily',DATEADD(hour,-30,CURRENT_TIMESTAMP()),30,'yellow',45600),
        ('CHURN_PREDICTIONS','ml-team','weekly',DATEADD(hour,-72,CURRENT_TIMESTAMP()),72,'red',18900)
    """,

    # --- Create ai_semantic_metadata table ---
    """CREATE TABLE IF NOT EXISTS RAW_SALES.GOLD.AI_SEMANTIC_METADATA (
        ENTITY_NAME VARCHAR,
        COLUMN_NAME VARCHAR,
        BUSINESS_DEFINITION VARCHAR,
        EXAMPLE_VALUE VARCHAR,
        EMBEDDING_READY_TEXT VARCHAR,
        _REFRESHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )""",

    """INSERT INTO RAW_SALES.GOLD.AI_SEMANTIC_METADATA
    SELECT * FROM VALUES
        ('DAILY_REVENUE_SUMMARY','TOTAL_REVENUE','Total revenue for the day in USD','45230.50','daily revenue summary total revenue usd',CURRENT_TIMESTAMP()),
        ('DAILY_REVENUE_SUMMARY','ORDER_COUNT','Number of orders placed','1523','daily revenue summary order count',CURRENT_TIMESTAMP()),
        ('CUSTOMER_SEGMENTS','SEGMENT_NAME','Customer segment classification','High Value','customer segments segment name classification',CURRENT_TIMESTAMP()),
        ('CUSTOMER_SEGMENTS','CLV_SCORE','Customer lifetime value score 0-100','87.3','customer segments clv lifetime value score',CURRENT_TIMESTAMP()),
        ('ORDER_ITEMS_CLEAN','ITEM_TOTAL','Line item total after discounts','29.99','order items clean item total after discounts',CURRENT_TIMESTAMP()),
        ('ORDER_ITEMS_CLEAN','REJECTION_FLAG','Whether item was rejected in QA','false','order items rejection flag quality assurance',CURRENT_TIMESTAMP()),
        ('PRODUCT_PERFORMANCE','UNITS_SOLD','Total units sold to date','4521','product performance units sold',CURRENT_TIMESTAMP()),
        ('PRODUCT_PERFORMANCE','RETURN_RATE','Percentage of items returned','3.2','product performance return rate percentage',CURRENT_TIMESTAMP())
    """,

    # --- Create monthly_sales_summary table ---
    """CREATE TABLE IF NOT EXISTS RAW_SALES.GOLD.MONTHLY_SALES_SUMMARY (
        YEAR_MONTH VARCHAR,
        TOTAL_REVENUE NUMBER(18,2),
        ORDER_COUNT NUMBER,
        AVG_ORDER_VALUE NUMBER(18,2)
    )""",

    """INSERT INTO RAW_SALES.GOLD.MONTHLY_SALES_SUMMARY
    SELECT * FROM VALUES
        ('2025-10',1245300.50,15230,81.77),
        ('2025-11',1389420.75,16890,82.27),
        ('2025-12',1567800.25,19450,80.61),
        ('2026-01',1423100.00,17320,82.16),
        ('2026-02',1298760.50,15870,81.84),
        ('2026-03',1534200.75,18960,80.92)
    """,

    # --- Create feature_registry table ---
    """CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.FEATURE_REGISTRY (
        FEATURE_ID NUMBER AUTOINCREMENT,
        FEATURE_NAME VARCHAR,
        ENTITY_TYPE VARCHAR,
        DATA_TYPE VARCHAR,
        DESCRIPTION VARCHAR,
        IS_POINT_IN_TIME BOOLEAN,
        OFFLINE_ENABLED BOOLEAN,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        TAGS VARCHAR
    )""",

    """INSERT INTO RAW_SALES.FEATURE_STORE.FEATURE_REGISTRY
        (FEATURE_NAME, ENTITY_TYPE, DATA_TYPE, DESCRIPTION, IS_POINT_IN_TIME, OFFLINE_ENABLED, TAGS)
    SELECT * FROM VALUES
        ('customer_lifetime_value','customer','FLOAT','Predicted customer lifetime value',TRUE,TRUE,'ml,revenue'),
        ('days_since_last_order','customer','INTEGER','Days since customer last ordered',TRUE,TRUE,'engagement'),
        ('avg_order_value_30d','customer','FLOAT','Average order value in last 30 days',TRUE,TRUE,'revenue,ml'),
        ('product_return_rate','product','FLOAT','Historical return rate for product',FALSE,TRUE,'quality'),
        ('category_trend_score','product','FLOAT','Trending score for product category',TRUE,FALSE,'marketing'),
        ('cart_abandonment_rate','session','FLOAT','Rate of cart abandonment per session',TRUE,TRUE,'conversion,ml'),
        ('inventory_velocity','product','FLOAT','Rate of inventory turnover',FALSE,TRUE,'supply-chain'),
        ('churn_probability','customer','FLOAT','ML-predicted churn probability',TRUE,TRUE,'ml,retention')
    """,

    # --- Create feature_lineage table ---
    """CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.FEATURE_LINEAGE (
        FEATURE_ID NUMBER,
        SOURCE_TABLE VARCHAR,
        TRANSFORMATION_SQL VARCHAR,
        LAST_COMPUTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )""",

    """INSERT INTO RAW_SALES.FEATURE_STORE.FEATURE_LINEAGE (FEATURE_ID, SOURCE_TABLE, TRANSFORMATION_SQL)
    SELECT * FROM VALUES
        (1,'RAW_SALES.GOLD.DAILY_REVENUE_SUMMARY','SUM(revenue) OVER customer PARTITION'),
        (2,'RAW_SALES.GOLD.ORDER_ITEMS_CLEAN','DATEDIFF(day, MAX(order_date), CURRENT_DATE)'),
        (3,'RAW_SALES.GOLD.ORDER_ITEMS_CLEAN','AVG(item_total) OVER 30d WINDOW'),
        (4,'RAW_SALES.GOLD.ORDER_ITEMS_CLEAN','COUNT_IF(returned)/COUNT(*)'),
        (5,'RAW_SALES.GOLD.PRODUCT_PERFORMANCE','TREND_SCORE UDF'),
        (6,'RAW_SALES.GOLD.ORDER_ITEMS_CLEAN','abandoned_carts/total_sessions'),
        (7,'RAW_SALES.GOLD.MONTHLY_SALES_SUMMARY','units_sold/avg_inventory'),
        (8,'RAW_SALES.GOLD.CUSTOMER_SEGMENTS','ML_PREDICT(churn_model, features)')
    """,

    # --- Create feature_versions table ---
    """CREATE TABLE IF NOT EXISTS RAW_SALES.FEATURE_STORE.FEATURE_VERSIONS (
        FEATURE_ID NUMBER,
        VERSION_NUMBER NUMBER,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        STATUS VARCHAR DEFAULT 'active'
    )""",

    """INSERT INTO RAW_SALES.FEATURE_STORE.FEATURE_VERSIONS (FEATURE_ID, VERSION_NUMBER, STATUS)
    SELECT * FROM VALUES
        (1,1,'active'),(1,2,'active'),
        (2,1,'active'),
        (3,1,'deprecated'),(3,2,'active'),
        (4,1,'active'),
        (5,1,'active'),(5,2,'active'),(5,3,'active'),
        (6,1,'active'),
        (7,1,'active'),
        (8,1,'deprecated'),(8,2,'active')
    """,

    # --- Grant SELECT on all new tables ---
    "GRANT SELECT ON ALL TABLES IN SCHEMA RAW_SALES.MONITORING TO ROLE APP_ROLE",
    "GRANT SELECT ON ALL TABLES IN SCHEMA RAW_SALES.GOLD TO ROLE APP_ROLE",
    "GRANT SELECT ON ALL TABLES IN SCHEMA RAW_SALES.FEATURE_STORE TO ROLE APP_ROLE",

    # --- Future grants so new tables are auto-accessible ---
    "GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_SALES.MONITORING TO ROLE APP_ROLE",
    "GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_SALES.GOLD TO ROLE APP_ROLE",
    "GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_SALES.FEATURE_STORE TO ROLE APP_ROLE",
]

for s in stmts:
    try:
        cur.execute(s.strip())
        print(f"OK: {s.strip()[:80]}")
    except Exception as e:
        print(f"ERR: {s.strip()[:80]} -> {e}")

cur.close()
conn.close()
print("\nDONE")
