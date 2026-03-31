"""Snowflake full-scope runner.

Runs phase SQL scripts in order, optionally activates validation tasks,
optionally runs the Snowpark capability proof, and evaluates acceptance gates.
"""

import os
import sys
import time
import argparse
import subprocess
import tomllib
import pathlib
import getpass
import snowflake.connector

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent.parent  # Go up to root (snow/)


def parse_args():
    parser = argparse.ArgumentParser(description="Run Snowflake phase scripts and validations.")
    parser.add_argument(
        "--skip-phases",
        action="store_true",
        help="Skip executing 01-05 SQL scripts and run only selected post-phase operations.",
    )
    parser.add_argument(
        "--include-foundation",
        action="store_true",
        help="Include 01_phase1_foundation.sql before Bronze/Silver/Gold/Product phases.",
    )
    parser.add_argument(
        "--resume-tasks",
        action="store_true",
        help="Resume monitoring validation tasks after phase execution.",
    )
    parser.add_argument(
        "--execute-validation-tasks",
        action="store_true",
        help="Execute each monitoring validation task once and wait briefly.",
    )
    parser.add_argument(
        "--run-snowpark-job",
        action="store_true",
        help="Run snowpark_silver_job.py after SQL phases.",
    )
    parser.add_argument(
        "--run-acceptance-gates",
        action="store_true",
        help="Run acceptance_gates.sql and print PASS/FAIL release decision.",
    )
    parser.add_argument(
        "--mark-marketplace-pass",
        action="store_true",
        help="Mark marketplace manual gate as passed before running acceptance gates.",
    )
    parser.add_argument(
        "--marketplace-note",
        default="Marketplace consumer validation completed manually.",
        help="Evidence note stored when --mark-marketplace-pass is set.",
    )
    return parser.parse_args()


# ── Load connection config ────────────────────────────────────────────────────
connections_path = pathlib.Path.home() / ".snowflake" / "connections.toml"
with open(connections_path, "rb") as f:
    all_conns = tomllib.load(f)

conn_name, conn_cfg = next(
    (
        name,
        cfg,
    )
    for name, cfg in all_conns.items()
    if isinstance(cfg, dict) and "account" in cfg and "user" in cfg
)
print(f"Using connection: [{conn_name}]  account={conn_cfg['account']}  user={conn_cfg['user']}")

token = os.environ.get("SNOWFLAKE_TOKEN")
password = os.environ.get("SNOWFLAKE_PASSWORD")
if not token and not password:
    password = getpass.getpass(f"Password for {conn_cfg['user']}@{conn_cfg['account']}: ")


# ── Connection factory ────────────────────────────────────────────────────────
def make_connection():
    if token:
        return snowflake.connector.connect(
            account=conn_cfg["account"],
            user=conn_cfg["user"],
            authenticator="programmatic_access_token",
            token=token,
            role=conn_cfg.get("role"),
            warehouse=conn_cfg.get("warehouse"),
            database=conn_cfg.get("database"),
            schema=conn_cfg.get("schema"),
            login_timeout=60,
        )
    return snowflake.connector.connect(
        account=conn_cfg["account"],
        user=conn_cfg["user"],
        authenticator=conn_cfg.get("authenticator", "snowflake"),
        password=password,
        role=conn_cfg.get("role"),
        warehouse=conn_cfg.get("warehouse"),
        database=conn_cfg.get("database"),
        schema=conn_cfg.get("schema"),
        login_timeout=60,
    )


# ── SQL statement splitter ────────────────────────────────────────────────────
def split_statements(sql: str) -> list:
    """
    Split SQL on real semicolons; ignore semicolons inside:
      - single-line comments  (-- ...)
      - single-quoted strings ('...')
            - Snowflake $$ ... $$ blocks
    Skip comment-only blocks.
    """
    stmts = []
    current = []
    in_single_quote = False
    in_dollar_block = False

    for line in sql.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("--"):
            current.append(line)
            continue

        j = 0
        while j < len(line):
            ch = line[j]
            nxt = line[j:j+2]

            if not in_single_quote and nxt == '$$':
                in_dollar_block = not in_dollar_block
                current.append('$$')
                j += 2
                continue

            if not in_single_quote and ch == '-' and j + 1 < len(line) and line[j + 1] == '-':
                current.append(line[j:])
                break
            elif ch == "'" and not in_single_quote:
                in_single_quote = True
                current.append(ch)
            elif ch == "'" and in_single_quote:
                in_single_quote = False
                current.append(ch)
            elif ch == ';' and not in_single_quote and not in_dollar_block:
                stmt = ''.join(current).strip()
                current = []
                if stmt:
                    non_blank = [l for l in stmt.splitlines() if l.strip()]
                    if non_blank and not all(l.strip().startswith("--") for l in non_blank):
                        stmts.append(stmt)
            else:
                current.append(ch)
            j += 1
        else:
            if not stripped.startswith("--"):
                current.append('\n')

    remainder = ''.join(current).strip()
    if remainder:
        non_blank = [l for l in remainder.splitlines() if l.strip()]
        if non_blank and not all(l.strip().startswith("--") for l in non_blank):
            stmts.append(remainder)

    return stmts


# ── File runner ───────────────────────────────────────────────────────────────
def run_file(path: str, con_ref: list):
    """Execute every statement in a SQL file.
    con_ref[0] holds the live connection so we can swap it on reconnect."""
    print(f"\n{'='*70}")
    print(f"  Running: {os.path.basename(path)}")
    print(f"{'='*70}")

    with open(path, encoding="utf-8") as fh:
        sql = fh.read()

    stmts = split_statements(sql)
    print(f"  {len(stmts)} statements found\n")

    cur = con_ref[0].cursor()

    for i, stmt in enumerate(stmts, 1):
        first_line = stmt.splitlines()[0].strip()[:80]
        rows, desc, success = [], None, False

        for attempt in range(3):
            try:
                cur.execute(stmt)
                rows = cur.fetchall()
                desc = cur.description
                success = True
                break
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"  [{i:03d}] WARN: {first_line}")
                print(f"        {str(e).splitlines()[0][:120]}")
                success = True  # non-fatal
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  [{i:03d}] Network hiccup (attempt {attempt+1}/3), reconnecting...")
                    try:
                        cur.close(); con_ref[0].close()
                    except Exception:
                        pass
                    time.sleep(3)
                    con_ref[0] = make_connection()
                    cur = con_ref[0].cursor()
                    cur.execute("USE DATABASE RAW_SALES")
                else:
                    print(f"  [{i:03d}] ERROR after 3 attempts: {first_line}")
                    print(f"        {e}")
                    raise

        if not success:
            continue

        if desc and rows:
            print(f"  [{i:03d}] {first_line}")
            for row in rows[:20]:
                print(f"        {'  |  '.join(str(v) for v in row)}")
            if len(rows) > 20:
                print(f"        ... ({len(rows)} total rows)")
        elif success and desc is not None and not rows:
            status = getattr(cur, 'sfqid', None) or "OK"
            print(f"  [{i:03d}] {first_line}  →  {status}")

    cur.close()
    print(f"\n  ✓ {os.path.basename(path)} complete")


def run_platform_checks(con):
    """Post-run health checks for mandatory platform capabilities."""
    print(f"\n{'='*70}")
    print("  Platform Capability Checks")
    print(f"{'='*70}")
    cur = con.cursor()
    ok = True

    try:
        cur.execute("SHOW STREAMS IN SCHEMA RAW_SALES.BRONZE")
        streams = cur.fetchall()
        print(f"  Streams found: {len(streams)}")
        if len(streams) < 6:
            print("  WARN: expected at least 6 Bronze streams")
            ok = False

        cur.execute("SHOW DYNAMIC TABLES IN SCHEMA RAW_SALES.SILVER")
        dts = cur.fetchall()
        print(f"  Dynamic tables found: {len(dts)}")
        if len(dts) < 3:
            print("  WARN: expected at least 3 Silver dynamic tables")
            ok = False

        cur.execute("SHOW TASKS IN SCHEMA RAW_SALES.MONITORING")
        tasks = cur.fetchall()
        print(f"  Tasks found: {len(tasks)}")
        if len(tasks) < 3:
            print("  WARN: expected at least 3 Monitoring tasks")
            ok = False

        cur.execute("SELECT COUNT(*) FROM RAW_SALES.MONITORING.snowpark_job_runs")
        sp_runs = cur.fetchone()[0]
        print(f"  Snowpark run log rows: {sp_runs}")
        if sp_runs < 1:
            print("  WARN: expected at least 1 Snowpark run log row")
            ok = False

        cur.execute("SELECT COUNT(*) FROM RAW_SALES.MONITORING.task_run_audit")
        task_audits = cur.fetchone()[0]
        print(f"  Task audit rows: {task_audits}")

        cur.execute("SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.feature_registry")
        features = cur.fetchone()[0]
        print(f"  Feature store registry rows: {features}")
        if features < 20:
            print("  WARN: expected at least 20 features in feature registry")
            ok = False

        cur.execute("SELECT COUNT(*) FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline")
        customer_features = cur.fetchone()[0]
        print(f"  Customer RFM feature rows: {customer_features}")
        if customer_features < 1000:
            print("  WARN: expected at least 1000 customer RFM feature rows")
            ok = False

    finally:
        cur.close()

    if not ok:
        raise RuntimeError("Platform capability checks failed. Review warnings above.")

    print("  ✓ Platform capability checks passed")


def set_manual_marketplace_gate(con, note: str):
    """Set manual marketplace evidence gate to TRUE with audit note."""
    cur = con.cursor()
    try:
        cur.execute("USE DATABASE RAW_SALES")
        cur.execute("USE SCHEMA MONITORING")
        cur.execute(
            """
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
            """,
            (note,),
        )
        print("  ✓ Manual marketplace gate marked PASS")
    finally:
        cur.close()


def resume_monitoring_tasks(con):
    """Resume known monitoring validation tasks if they exist."""
    task_names = [
        "RAW_SALES.MONITORING.task_validate_sales_rep_monthly",
        "RAW_SALES.MONITORING.task_validate_customer_revenue_forecast",
        "RAW_SALES.MONITORING.task_validate_customer_acquisition_cohort",
    ]
    cur = con.cursor()
    print("\nResuming monitoring tasks...")
    try:
        for task in task_names:
            try:
                cur.execute(f"ALTER TASK {task} RESUME")
                print(f"  ✓ Resumed {task}")
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"  WARN: Could not resume {task}: {str(e).splitlines()[0]}")
    finally:
        cur.close()


def execute_monitoring_tasks(con):
    """Execute monitoring validation tasks once."""
    task_names = [
        "RAW_SALES.MONITORING.task_validate_sales_rep_monthly",
        "RAW_SALES.MONITORING.task_validate_customer_revenue_forecast",
        "RAW_SALES.MONITORING.task_validate_customer_acquisition_cohort",
    ]
    cur = con.cursor()
    print("\nExecuting monitoring tasks once...")
    try:
        for task in task_names:
            try:
                cur.execute(f"EXECUTE TASK {task}")
                print(f"  ✓ Triggered {task}")
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"  WARN: Could not execute {task}: {str(e).splitlines()[0]}")
        time.sleep(5)
    finally:
        cur.close()


def run_snowpark_capability_job():
    """Run Snowpark capability proof script as a subprocess."""
    script = BASE_DIR / "python" / "data_engineering" / "snowpark_silver_job.py"
    print("\nRunning Snowpark capability job...")
    result = subprocess.run([sys.executable, str(script)], check=False)
    if result.returncode != 0:
        raise RuntimeError("Snowpark capability job failed.")
    print("  ✓ Snowpark capability job completed")


def run_acceptance_gates(con_ref):
    """Run release acceptance gates and print decision summary."""
    gate_file = str(BASE_DIR / "sql" / "governance" / "acceptance_gates.sql")
    run_file(gate_file, con_ref)

    cur = con_ref[0].cursor()
    try:
        cur.execute(
            """
            SELECT RUN_ID, RELEASE_STATUS, PASS_COUNT, FAIL_COUNT, EVALUATED_AT
            FROM (
                SELECT
                    RUN_ID,
                    IFF(COUNT_IF(STATUS = 'FAIL') = 0, 'PASS', 'FAIL') AS RELEASE_STATUS,
                    COUNT_IF(STATUS = 'PASS') AS PASS_COUNT,
                    COUNT_IF(STATUS = 'FAIL') AS FAIL_COUNT,
                    MIN(EVALUATED_AT) AS EVALUATED_AT
                FROM RAW_SALES.MONITORING.release_gate_results
                GROUP BY RUN_ID
            )
            ORDER BY RUN_ID DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            run_id, status, pass_count, fail_count, evaluated_at = row
            print("\n" + "=" * 70)
            print(f"  Release Gate Summary: RUN_ID={run_id} STATUS={status}")
            print(f"  Pass={pass_count}  Fail={fail_count}  EvaluatedAt={evaluated_at}")
            print("=" * 70)
        else:
            raise RuntimeError("No release gate results were found after gate execution.")
    finally:
        cur.close()


def main():
    args = parse_args()

    scripts = []
    if args.include_foundation:
        scripts.append(str(BASE_DIR / "sql" / "phase1_foundation" / "01_phase1_foundation.sql"))
    scripts.extend(
        [
            str(BASE_DIR / "sql" / "phase2_bronze" / "02_phase2_bronze.sql"),
            str(BASE_DIR / "sql" / "phase2_silver" / "03_phase2_silver.sql"),
            str(BASE_DIR / "sql" / "phase2_gold" / "04_phase2_gold.sql"),
            str(BASE_DIR / "sql" / "phase3_data_products" / "05_phase3_data_products.sql"),
            str(BASE_DIR / "sql" / "phase4_feature_store" / "06_feature_store_ml.sql"),
            str(BASE_DIR / "sql" / "phase4_feature_store" / "07_feature_store_explore.sql"),
        ]
    )

    print("\nConnecting to Snowflake...", end=" ", flush=True)
    con = make_connection()
    print("OK")

    con_ref = [con]
    try:
        if not args.skip_phases:
            for script in scripts:
                run_file(script, con_ref)
        else:
            print("\nSkipping phase SQL execution (--skip-phases enabled)")

        if args.resume_tasks:
            resume_monitoring_tasks(con_ref[0])

        if args.execute_validation_tasks:
            execute_monitoring_tasks(con_ref[0])

        if args.run_snowpark_job:
            run_snowpark_capability_job()

        run_platform_checks(con_ref[0])

        if args.mark_marketplace_pass:
            set_manual_marketplace_gate(con_ref[0], args.marketplace_note)

        if args.run_acceptance_gates:
            run_acceptance_gates(con_ref)

        print("\n" + "=" * 70)
        print("  RUN COMPLETE ✓")
        print("=" * 70)
    except Exception as e:
        print(f"\nFAILED: {e}")
        sys.exit(1)
    finally:
        try:
            con_ref[0].close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
