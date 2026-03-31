import getpass
import os
import pathlib
import tomllib

import snowflake.connector

p = pathlib.Path.home() / ".snowflake" / "connections.toml"
cfg = next(iter(tomllib.load(open(p,"rb")).values()))
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

print("Silver tables:")
cur.execute("SELECT TABLE_NAME, ROW_COUNT FROM RAW_SALES.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='SILVER' ORDER BY TABLE_NAME")
rows = cur.fetchall()
if rows:
    for r in rows: print(f"  {r[0]:<40} {str(r[1]):>12}")
else:
    print("  (none)")

print()
print("Monitoring tables:")
cur.execute("SELECT TABLE_NAME, ROW_COUNT FROM RAW_SALES.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MONITORING' ORDER BY TABLE_NAME")
rows = cur.fetchall()
if rows:
    for r in rows: print(f"  {r[0]:<40} {str(r[1]):>12}")
else:
    print("  (none)")

con.close()
