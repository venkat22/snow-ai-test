import os
import pathlib
import tomllib
from contextlib import contextmanager
from typing import Any

import snowflake.connector
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = pathlib.Path(__file__).resolve().parent
app = FastAPI(title="Snow Data Marketplace Portal", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class SnowConfigError(RuntimeError):
    pass


def load_connection_profile() -> dict[str, Any]:
    # Try env vars first (container-friendly), then fall back to connections.toml
    if os.environ.get("SNOWFLAKE_ACCOUNT") and os.environ.get("SNOWFLAKE_USER"):
        return {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "role": os.environ.get("SNOWFLAKE_ROLE"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "RAW_SALES"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", "MONITORING"),
        }

    conn_path = pathlib.Path.home() / ".snowflake" / "connections.toml"
    if not conn_path.exists():
        raise SnowConfigError(
            "Missing ~/.snowflake/connections.toml — "
            "or set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER env vars"
        )

    with open(conn_path, "rb") as fh:
        profiles = tomllib.load(fh)

    if not profiles:
        raise SnowConfigError("No profiles found in connections.toml")

    # If there's a default_connection_name key, use that profile
    default_name = profiles.get("default_connection_name")
    if default_name and isinstance(profiles.get(default_name), dict):
        return profiles[default_name]

    # Otherwise, grab the first dict-typed profile
    for _, val in profiles.items():
        if isinstance(val, dict):
            return val

    raise SnowConfigError("No valid connection profile found in connections.toml")


@contextmanager
def get_connection():
    cfg = load_connection_profile()
    token = os.environ.get("SNOWFLAKE_TOKEN")
    password = os.environ.get("SNOWFLAKE_PASSWORD")

    kwargs: dict[str, Any] = {
        "account": cfg["account"],
        "user": cfg["user"],
        "role": cfg.get("role") or None,
        "warehouse": cfg.get("warehouse", "ANALYTICS_WH") or "ANALYTICS_WH",
        "database": cfg.get("database", "RAW_SALES") or "RAW_SALES",
        "schema": cfg.get("schema", "MONITORING") or "MONITORING",
    }

    if token:
        kwargs["authenticator"] = "programmatic_access_token"
        kwargs["token"] = token
    elif password:
        kwargs["authenticator"] = cfg.get("authenticator", "snowflake")
        kwargs["password"] = password
    elif cfg.get("password"):
        kwargs["authenticator"] = cfg.get("authenticator", "snowflake")
        kwargs["password"] = cfg["password"]
    elif cfg.get("token"):
        kwargs["authenticator"] = "programmatic_access_token"
        kwargs["token"] = cfg["token"]
    else:
        raise SnowConfigError("Set SNOWFLAKE_TOKEN or SNOWFLAKE_PASSWORD before starting UI")

    con = snowflake.connector.connect(**kwargs)
    try:
        yield con
    finally:
        con.close()


def execute_query(sql: str, binds: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with get_connection() as con:
        cur = con.cursor()
        try:
            cur.execute(sql, binds) if binds else cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        finally:
            cur.close()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/api/health")
def health():
    try:
        row = execute_query("SELECT CURRENT_TIMESTAMP() AS TS, CURRENT_ACCOUNT() AS ACCOUNT_NAME")[0]
        return {"status": "ok", "snowflake": row}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/products")
def products():
    sql = """
    SELECT
      p.PRODUCT_NAME,
      p.OWNER,
      p.REFRESH_FREQUENCY,
      p.LAST_REFRESHED_AT,
      p.HOURS_SINCE_REFRESH,
      p.SLA_STATUS,
      p.CURRENT_ROW_COUNT,
      t.COMMENT AS TABLE_COMMENT
    FROM RAW_SALES.MONITORING.product_sla_status p
    LEFT JOIN RAW_SALES.INFORMATION_SCHEMA.TABLES t
      ON t.TABLE_SCHEMA = 'GOLD'
     AND UPPER(t.TABLE_NAME) = UPPER(p.PRODUCT_NAME)
    ORDER BY p.PRODUCT_NAME
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/search")
def search(q: str = Query(min_length=2, max_length=120)):
    like = f"%{q}%"
    sql = """
    SELECT
      ENTITY_NAME,
      COLUMN_NAME,
      BUSINESS_DEFINITION,
      EXAMPLE_VALUE,
      EMBEDDING_READY_TEXT,
      _REFRESHED_AT,
      CASE
        WHEN UPPER(ENTITY_NAME) LIKE UPPER(%s) THEN 3
        WHEN UPPER(COLUMN_NAME) LIKE UPPER(%s) THEN 2
        WHEN UPPER(BUSINESS_DEFINITION) LIKE UPPER(%s) THEN 1
        ELSE 0
      END AS RELEVANCE_SCORE
    FROM RAW_SALES.GOLD.ai_semantic_metadata
    WHERE UPPER(ENTITY_NAME) LIKE UPPER(%s)
       OR UPPER(COLUMN_NAME) LIKE UPPER(%s)
       OR UPPER(BUSINESS_DEFINITION) LIKE UPPER(%s)
       OR UPPER(EMBEDDING_READY_TEXT) LIKE UPPER(%s)
    ORDER BY RELEVANCE_SCORE DESC, ENTITY_NAME, COLUMN_NAME
    LIMIT 50
    """
    try:
        return execute_query(sql, (like, like, like, like, like, like, like))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/manual-gate/marketplace-pass")
def mark_marketplace_pass(note: str = "Validated from consumer account"):
    sql = """
    MERGE INTO RAW_SALES.MONITORING.manual_release_checks t
    USING (
        SELECT
          'marketplace_consumer_test_passed' AS CHECK_NAME,
          TRUE AS CHECK_VALUE,
          CURRENT_TIMESTAMP() AS LAST_UPDATED_AT,
          CURRENT_USER() AS UPDATED_BY,
          %s AS NOTES
    ) s
    ON t.CHECK_NAME = s.CHECK_NAME
    WHEN MATCHED THEN UPDATE SET
      CHECK_VALUE = s.CHECK_VALUE,
      LAST_UPDATED_AT = s.LAST_UPDATED_AT,
      UPDATED_BY = s.UPDATED_BY,
      NOTES = s.NOTES
    WHEN NOT MATCHED THEN INSERT (CHECK_NAME, CHECK_VALUE, LAST_UPDATED_AT, UPDATED_BY, NOTES)
    VALUES (s.CHECK_NAME, s.CHECK_VALUE, s.LAST_UPDATED_AT, s.UPDATED_BY, s.NOTES)
    """
    check_sql = """
    SELECT CHECK_NAME, CHECK_VALUE, LAST_UPDATED_AT, UPDATED_BY, NOTES
    FROM RAW_SALES.MONITORING.manual_release_checks
    WHERE CHECK_NAME = 'marketplace_consumer_test_passed'
    """
    try:
        execute_query(sql, (note,))
        return execute_query(check_sql)[0]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Analytics endpoints
# ---------------------------------------------------------------------------

@app.get("/api/analytics/sla-summary")
def analytics_sla_summary():
    sql = """
    SELECT SLA_STATUS, COUNT(*) AS CNT
    FROM RAW_SALES.MONITORING.product_sla_status
    GROUP BY SLA_STATUS
    ORDER BY SLA_STATUS
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/analytics/row-counts")
def analytics_row_counts():
    sql = """
    SELECT PRODUCT_NAME, CURRENT_ROW_COUNT, HOURS_SINCE_REFRESH
    FROM RAW_SALES.MONITORING.product_sla_status
    ORDER BY CURRENT_ROW_COUNT DESC
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/analytics/monthly-sales")
def analytics_monthly_sales():
    sql = """
    SELECT YEAR_MONTH,
           SUM(TOTAL_ORDERS) AS TOTAL_ORDERS,
           SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
           SUM(UNIQUE_CUSTOMERS) AS UNIQUE_CUSTOMERS
    FROM RAW_SALES.GOLD.monthly_sales_summary
    GROUP BY YEAR_MONTH
    ORDER BY YEAR_MONTH
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Quality & Governance endpoints
# ---------------------------------------------------------------------------

@app.get("/api/quality/dq-log")
def quality_dq_log():
    sql = """
    SELECT TARGET_TABLE, DQ_DIMENSION, CHECK_DESCRIPTION,
           RECORDS_PASSED, RECORDS_FAILED, PASS_RATE_PCT, STATUS, LOG_TIMESTAMP
    FROM RAW_SALES.MONITORING.data_quality_log
    ORDER BY LOG_TIMESTAMP DESC
    LIMIT 100
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/quality/rejected")
def quality_rejected():
    sql = """
    SELECT REJECTION_REASON, COUNT(*) AS REJECTED_ROWS
    FROM RAW_SALES.MONITORING.order_items_rejected
    GROUP BY REJECTION_REASON
    ORDER BY REJECTED_ROWS DESC
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/quality/release-gates")
def quality_release_gates():
    sql = """
    SELECT GATE_ID, GATE_NAME, STATUS, ACTUAL_VALUE, EXPECTED_VALUE, DETAILS, EVALUATED_AT
    FROM RAW_SALES.MONITORING.release_gate_results
    WHERE RUN_ID = (SELECT MAX(RUN_ID) FROM RAW_SALES.MONITORING.release_gate_results)
    ORDER BY GATE_ID
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/quality/snowpark-jobs")
def quality_snowpark_jobs():
    sql = """
    SELECT JOB_NAME, STATUS, MESSAGE, RUN_AT
    FROM RAW_SALES.MONITORING.snowpark_job_runs
    ORDER BY RUN_AT DESC
    LIMIT 50
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Feature Store endpoints
# ---------------------------------------------------------------------------

@app.get("/api/features/registry")
def features_registry(q: str = Query(default="", max_length=120)):
    if q and len(q) >= 2:
        like = f"%{q}%"
        sql = """
        SELECT FEATURE_ID, FEATURE_NAME, ENTITY_TYPE, DATA_TYPE,
               OWNER_TEAM, IS_POINT_IN_TIME, OFFLINE_ENABLED, ONLINE_ENABLED,
               TAGS, DESCRIPTION, LINEAGE_SOURCE_TABLE, VERSION, CREATED_AT
        FROM RAW_SALES.FEATURE_STORE.feature_registry
        WHERE UPPER(FEATURE_NAME) LIKE UPPER(%s)
           OR UPPER(TAGS) LIKE UPPER(%s)
           OR UPPER(DESCRIPTION) LIKE UPPER(%s)
           OR UPPER(ENTITY_TYPE) LIKE UPPER(%s)
        ORDER BY ENTITY_TYPE, FEATURE_NAME
        """
        try:
            return execute_query(sql, (like, like, like, like))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    else:
        sql = """
        SELECT FEATURE_ID, FEATURE_NAME, ENTITY_TYPE, DATA_TYPE,
               OWNER_TEAM, IS_POINT_IN_TIME, OFFLINE_ENABLED, ONLINE_ENABLED,
               TAGS, DESCRIPTION, LINEAGE_SOURCE_TABLE, VERSION, CREATED_AT
        FROM RAW_SALES.FEATURE_STORE.feature_registry
        ORDER BY ENTITY_TYPE, FEATURE_NAME
        """
        try:
            return execute_query(sql)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/features/lineage")
def features_lineage():
    sql = """
    SELECT
      FL.UPSTREAM_FEATURE_ID,
      FL.DOWNSTREAM_FEATURE_ID,
      FL.DEPENDENCY_TYPE,
      COALESCE(FL.UPSTREAM_TABLE_NAME, '') AS UPSTREAM_TABLE_NAME,
      COALESCE(FR_UP.FEATURE_NAME, FL.UPSTREAM_TABLE_NAME, 'SOURCE') AS UPSTREAM_NAME,
      COALESCE(FR_DOWN.FEATURE_NAME, FL.DOWNSTREAM_FEATURE_ID) AS DOWNSTREAM_NAME
    FROM RAW_SALES.FEATURE_STORE.feature_lineage FL
    LEFT JOIN RAW_SALES.FEATURE_STORE.feature_registry FR_UP
      ON FL.UPSTREAM_FEATURE_ID = FR_UP.FEATURE_ID
    LEFT JOIN RAW_SALES.FEATURE_STORE.feature_registry FR_DOWN
      ON FL.DOWNSTREAM_FEATURE_ID = FR_DOWN.FEATURE_ID
    ORDER BY FL.DOWNSTREAM_FEATURE_ID, FL.UPSTREAM_FEATURE_ID
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/features/summary")
def features_summary():
    sql = """
    SELECT 'Features Registered' AS METRIC, COUNT(*)::VARCHAR AS VALUE
    FROM RAW_SALES.FEATURE_STORE.feature_registry
    UNION ALL
    SELECT 'Entity Types', COUNT(DISTINCT ENTITY_TYPE)::VARCHAR
    FROM RAW_SALES.FEATURE_STORE.feature_registry
    UNION ALL
    SELECT 'PIT-Correct Features', COUNT_IF(IS_POINT_IN_TIME)::VARCHAR
    FROM RAW_SALES.FEATURE_STORE.feature_registry
    UNION ALL
    SELECT 'Offline-Enabled', COUNT_IF(OFFLINE_ENABLED)::VARCHAR
    FROM RAW_SALES.FEATURE_STORE.feature_registry
    """
    try:
        return execute_query(sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/features/versions")
def features_versions(feature_id: str = Query(default="", max_length=120)):
    if feature_id:
        sql = """
        SELECT FV.FEATURE_ID, FR.FEATURE_NAME, FV.VERSION_NUMBER,
               FV.IS_ACTIVE, FV.DEPLOYMENT_TIMESTAMP, FV.CREATED_BY, FV.CHANGE_REASON
        FROM RAW_SALES.FEATURE_STORE.feature_versions FV
        LEFT JOIN RAW_SALES.FEATURE_STORE.feature_registry FR
          ON FV.FEATURE_ID = FR.FEATURE_ID
        WHERE FV.FEATURE_ID = %s
        ORDER BY FV.VERSION_NUMBER DESC
        LIMIT 50
        """
        try:
            return execute_query(sql, (feature_id,))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    else:
        sql = """
        SELECT FV.FEATURE_ID, FR.FEATURE_NAME, FV.VERSION_NUMBER,
               FV.IS_ACTIVE, FV.DEPLOYMENT_TIMESTAMP, FV.CREATED_BY, FV.CHANGE_REASON
        FROM RAW_SALES.FEATURE_STORE.feature_versions FV
        LEFT JOIN RAW_SALES.FEATURE_STORE.feature_registry FR
          ON FV.FEATURE_ID = FR.FEATURE_ID
        ORDER BY FV.FEATURE_ID, FV.VERSION_NUMBER DESC
        LIMIT 50
        """
        try:
            return execute_query(sql)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
