"""Snowpark silver validation job.

Runs a small Snowpark transformation/validation workflow and logs result to
RAW_SALES.MONITORING.snowpark_job_runs.
"""

import getpass
import importlib
import os
import pathlib
import tomllib


def _load_connection_config() -> dict:
    path = pathlib.Path.home() / ".snowflake" / "connections.toml"
    with open(path, "rb") as fh:
        _, cfg = next(iter(tomllib.load(fh).items()))

    token = os.environ.get("SNOWFLAKE_TOKEN")
    env_password = os.environ.get("SNOWFLAKE_PASSWORD")
    if token:
        return {
            "account": cfg["account"],
            "user": cfg["user"],
            "authenticator": "programmatic_access_token",
            "token": token,
            "warehouse": cfg.get("warehouse", "ANALYTICS_WH"),
            "database": cfg.get("database", "RAW_SALES"),
            "schema": cfg.get("schema", "MONITORING"),
            "role": cfg.get("role", "ACCOUNTADMIN"),
        }

    password = env_password or getpass.getpass(f"Password for {cfg['user']}@{cfg['account']}: ")
    return {
        "account": cfg["account"],
        "user": cfg["user"],
        "password": password,
        "warehouse": cfg.get("warehouse", "ANALYTICS_WH"),
        "database": cfg.get("database", "RAW_SALES"),
        "schema": cfg.get("schema", "MONITORING"),
        "role": cfg.get("role", "ACCOUNTADMIN"),
    }


def _log_run(session, status: str, message: str) -> None:
    session.sql(
        """
        INSERT INTO RAW_SALES.MONITORING.snowpark_job_runs (JOB_NAME, STATUS, MESSAGE)
        SELECT 'snowpark_silver_validation', ?, ?
        """,
        params=[status, message],
    ).collect()


def main() -> None:
    cfg = _load_connection_config()
    snowpark = importlib.import_module("snowflake.snowpark")
    session = snowpark.Session.builder.configs(cfg).create()

    try:
        orders = session.table("RAW_SALES.SILVER.orders")
        order_items = session.table("RAW_SALES.SILVER.order_items")

        metrics = (
            orders.join(order_items, orders["ORDER_ID"] == order_items["ORDER_ID"], "inner")
            .group_by(orders["STATUS"])
            .count()
            .collect()
        )

        if not metrics:
            _log_run(session, "WARN", "No rows produced by Snowpark status aggregation.")
            print("WARN: No rows produced by Snowpark status aggregation.")
            return

        status_counts = ", ".join(f"{row['STATUS']}={row['COUNT']}" for row in metrics)
        _log_run(session, "SUCCESS", f"Snowpark validation succeeded: {status_counts}")
        print(f"SUCCESS: Snowpark validation succeeded: {status_counts}")

    except Exception as exc:  # noqa: BLE001
        _log_run(session, "FAIL", f"Snowpark job failed: {exc}")
        print(f"FAIL: Snowpark job failed: {exc}")
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()
