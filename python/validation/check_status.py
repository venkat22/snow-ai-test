"""Quick status check: what's already been created in Snowflake."""
import getpass
import os
import pathlib
import tomllib

import snowflake.connector

p = pathlib.Path.home() / ".snowflake" / "connections.toml"
cfg = next(iter(tomllib.load(open(p, "rb")).values()))
token = os.environ.get("SNOWFLAKE_TOKEN")

if token:
    con = snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        authenticator="programmatic_access_token",
        token=token,
    )
else:
    password = getpass.getpass(f"Password for {cfg['user']}@{cfg['account']}: ")
    con = snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=password,
    )
cur = con.cursor()

print("\n── Warehouses ──────────────────────────────────────────")
cur.execute("SHOW WAREHOUSES")
wh_rows = cur.fetchall()
if wh_rows:
    for r in wh_rows:
        print(f"  {r[0]}")
else:
    print("  (none)")

print("\n── Schemas in RAW_SALES ────────────────────────────────")
try:
    cur.execute("SHOW SCHEMAS IN DATABASE RAW_SALES")
    for r in cur.fetchall():
        print(f"  {r[1]}")
except Exception as e:
    print(f"  RAW_SALES DB not found: {e}")

print("\n── LANDING tables + row counts ─────────────────────────")
try:
    cur.execute("""
        SELECT TABLE_NAME, ROW_COUNT
        FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'LANDING'
        ORDER BY TABLE_NAME
    """)
    rows = cur.fetchall()
    if rows:
        for r in rows:
            print(f"  {r[0]:20s}  {r[1]:>12,}")
    else:
        print("  (no tables yet)")
except Exception as e:
    print(f"  Error: {e}")

print("\n── BRONZE tables ───────────────────────────────────────")
try:
    cur.execute("""
        SELECT TABLE_NAME, ROW_COUNT FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'BRONZE' ORDER BY TABLE_NAME
    """)
    rows = cur.fetchall()
    for r in rows: print(f"  {r[0]:25s}  {r[1]:>12,}")
    if not rows: print("  (none)")
except Exception as e:
    print(f"  Error: {e}")

print("\n── BRONZE Streams ──────────────────────────────────────")
try:
    cur.execute("SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE")
    rows = cur.fetchall()
    if rows:
        for r in rows:
            name = r[1] if len(r) > 1 else "unknown"
            stale = r[10] if len(r) > 10 else "unknown"
            print(f"  {name:30s}  stale={stale}")
    else:
        print("  (none)")
except Exception as e:
    print(f"  Error: {e}")

print("\n── SILVER Dynamic Tables ──────────────────────────────")
try:
    cur.execute("SHOW DYNAMIC TABLES IN SCHEMA RAW_SALES.SILVER")
    rows = cur.fetchall()
    if rows:
        for r in rows:
            name = r[1] if len(r) > 1 else "unknown"
            state = r[6] if len(r) > 6 else "unknown"
            print(f"  {name:30s}  state={state}")
    else:
        print("  (none)")
except Exception as e:
    print(f"  Error: {e}")

print("\n── GOLD tables ─────────────────────────────────────────")
try:
    cur.execute("""
        SELECT TABLE_NAME, ROW_COUNT FROM RAW_SALES.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'GOLD' ORDER BY TABLE_NAME
    """)
    rows = cur.fetchall()
    for r in rows: print(f"  {r[0]:35s}  {r[1]:>12,}")
    if not rows: print("  (none)")
except Exception as e:
    print(f"  Error: {e}")

print("\n── MONITORING Tasks ───────────────────────────────────")
try:
    cur.execute("SHOW TASKS IN SCHEMA RAW_SALES.MONITORING")
    rows = cur.fetchall()
    if rows:
        for r in rows:
            name = r[1] if len(r) > 1 else "unknown"
            state = r[7] if len(r) > 7 else "unknown"
            print(f"  {name:45s}  state={state}")
    else:
        print("  (none)")
except Exception as e:
    print(f"  Error: {e}")

con.close()
print("\nDone.")
