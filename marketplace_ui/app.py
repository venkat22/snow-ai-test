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

    _, cfg = next(iter(profiles.items()))
    return cfg


@contextmanager
def get_connection():
    cfg = load_connection_profile()
    token = os.environ.get("SNOWFLAKE_TOKEN")
    password = os.environ.get("SNOWFLAKE_PASSWORD")

    kwargs: dict[str, Any] = {
        "account": cfg["account"],
        "user": cfg["user"],
        "role": cfg.get("role"),
        "warehouse": cfg.get("warehouse", "ANALYTICS_WH"),
        "database": cfg.get("database", "RAW_SALES"),
        "schema": cfg.get("schema", "MONITORING"),
    }

    if token:
        kwargs["authenticator"] = "programmatic_access_token"
        kwargs["token"] = token
    elif password:
        kwargs["authenticator"] = cfg.get("authenticator", "snowflake")
        kwargs["password"] = password
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
    return templates.TemplateResponse("index.html", {"request": request})


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
