import os
import pathlib
import tomllib

import snowflake.connector

sql_path = pathlib.Path("c:/tmp/snow/sql/phase4_feature_store/06_feature_store_ml.sql")
text = sql_path.read_text(encoding="utf-8")
start = text.index("CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.customer_rfm_features_offline AS")
end = text.index("COMMENT ON TABLE RAW_SALES.FEATURE_STORE.customer_rfm_features_offline IS")
stmt = text[start:end].strip().rstrip(";")

cfg_path = pathlib.Path.home() / ".snowflake" / "connections.toml"
cfg = next(iter(tomllib.load(open(cfg_path, "rb")).values()))
token = os.environ.get("SNOWFLAKE_TOKEN")
if not token:
    raise RuntimeError("SNOWFLAKE_TOKEN is not set")

con = snowflake.connector.connect(
    account=cfg["account"],
    user=cfg["user"],
    authenticator="programmatic_access_token",
    token=token,
)
cur = con.cursor()

try:
    cur.execute("USE DATABASE RAW_SALES")
    cur.execute("USE WAREHOUSE ANALYTICS_WH")
    cur.execute(stmt)
    print("SUCCESS: customer_rfm_features_offline created")
except Exception as e:
    print(f"ERROR_TYPE: {type(e).__name__}")
    print(f"ERROR_TEXT: {e}")
finally:
    cur.close()
    con.close()
