import getpass
import os
import pathlib
import tomllib

import snowflake.connector


ROLE_NAMES = [
    "SALES_ANALYSTS",
    "FINANCE_ANALYSTS",
    "MARKETING_ANALYSTS",
    "DATA_CONSUMERS",
]


def connect():
    p = pathlib.Path.home() / ".snowflake" / "connections.toml"
    cfg = next(iter(tomllib.load(open(p, "rb")).values()))
    token = os.environ.get("SNOWFLAKE_TOKEN")
    env_password = os.environ.get("SNOWFLAKE_PASSWORD")

    if token:
        return snowflake.connector.connect(
            account=cfg["account"],
            user=cfg["user"],
            authenticator="programmatic_access_token",
            token=token,
            role=cfg.get("role"),
            warehouse=cfg.get("warehouse"),
            database=cfg.get("database"),
            schema=cfg.get("schema"),
        )

    password = env_password or getpass.getpass(f"Password for {cfg['user']}@{cfg['account']}: ")
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=password,
        role=cfg.get("role"),
        warehouse=cfg.get("warehouse"),
        database=cfg.get("database"),
        schema=cfg.get("schema"),
    )


con = connect()
cur = con.cursor()

print("\n== Role Existence ==")
cur.execute("SHOW ROLES")
all_roles = {r[1] for r in cur.fetchall()}
for role in ROLE_NAMES:
    print(f"{role}: {'FOUND' if role in all_roles else 'MISSING'}")

print("\n== Grants by Role ==")
for role in ROLE_NAMES:
    if role not in all_roles:
        continue
    print(f"\n[{role}]")
    cur.execute(f"SHOW GRANTS TO ROLE {role}")
    rows = cur.fetchall()
    if not rows:
        print("  (no grants)")
        continue
    for r in rows:
        privilege = r[1]
        granted_on = r[2]
        name = r[3]
        print(f"  {privilege:12s} on {granted_on:10s} {name}")

print("\n== SLA View Presence ==")
cur.execute("SHOW VIEWS LIKE 'PRODUCT_SLA_STATUS' IN SCHEMA RAW_SALES.MONITORING")
views = cur.fetchall()
print(f"PRODUCT_SLA_STATUS view: {'FOUND' if views else 'MISSING'}")

if views:
    print("\n== SLA Status Rows ==")
    cur.execute(
        """
        SELECT PRODUCT_NAME, OWNER, REFRESH_FREQUENCY, LAST_REFRESHED_AT,
               HOURS_SINCE_REFRESH, SLA_STATUS, CURRENT_ROW_COUNT
        FROM RAW_SALES.MONITORING.product_sla_status
        ORDER BY PRODUCT_NAME
        """
    )
    for row in cur.fetchall():
        print("  " + " | ".join(str(v) for v in row))

print("\n== Platform Capability Presence ==")

try:
    cur.execute("SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE")
    streams = cur.fetchall()
    print(f"BRONZE streams: {len(streams)}")
    for r in streams[:10]:
        name = r[1] if len(r) > 1 else "unknown"
        stale = r[10] if len(r) > 10 else "unknown"
        print(f"  {name} | stale={stale}")
except Exception as e:
    print(f"BRONZE streams check failed: {e}")

try:
    cur.execute("SHOW DYNAMIC TABLES IN SCHEMA RAW_SALES.SILVER")
    dts = cur.fetchall()
    print(f"SILVER dynamic tables: {len(dts)}")
    for r in dts[:10]:
        name = r[1] if len(r) > 1 else "unknown"
        state = r[6] if len(r) > 6 else "unknown"
        print(f"  {name} | state={state}")
except Exception as e:
    print(f"SILVER dynamic table check failed: {e}")

try:
    cur.execute("SHOW TASKS IN SCHEMA RAW_SALES.MONITORING")
    tasks = cur.fetchall()
    print(f"MONITORING tasks: {len(tasks)}")
    for r in tasks[:10]:
        name = r[1] if len(r) > 1 else "unknown"
        state = r[7] if len(r) > 7 else "unknown"
        print(f"  {name} | state={state}")
except Exception as e:
    print(f"MONITORING task check failed: {e}")

print("\n== Snowpark and Task Audit Telemetry ==")
try:
    cur.execute("SELECT COUNT(*) FROM RAW_SALES.MONITORING.snowpark_job_runs")
    print(f"snowpark_job_runs rows: {cur.fetchone()[0]}")
except Exception as e:
    print(f"snowpark_job_runs check failed: {e}")

try:
    cur.execute("SELECT COUNT(*) FROM RAW_SALES.MONITORING.task_run_audit")
    print(f"task_run_audit rows: {cur.fetchone()[0]}")
except Exception as e:
    print(f"task_run_audit check failed: {e}")

cur.close()
con.close()
print("\nDone.")
