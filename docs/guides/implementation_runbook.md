# Implementation Runbook (Committed Scope)

This runbook executes the full committed challenge scope (Foundation + Medallion + Platform + Data Products + Gates), excluding stretch UI.

## 1. Prerequisites

1. Configure Snowflake connection profile at `%USERPROFILE%/.snowflake/connections.toml`.
2. Install Python dependencies in the workspace venv:
   - `snowflake-connector-python`
   - `snowflake-snowpark-python`
3. Provide one auth mode:
   - Preferred: `SNOWFLAKE_TOKEN`
   - Alternative: `SNOWFLAKE_PASSWORD`

PowerShell example:

```powershell
$env:SNOWFLAKE_PASSWORD = "<your-password>"
```

## 2. Execute Full Scope

From workspace root:

```powershell
c:/tmp/snow/.venv/Scripts/python.exe c:/tmp/snow/python/orchestration/run_all.py \
  --include-foundation \
  --resume-tasks \
  --execute-validation-tasks \
  --run-snowpark-job \
  --run-acceptance-gates
```

Notes:
1. Do not use `--mark-marketplace-pass` until you complete real consumer validation.
2. The run prints release gate summary (`RUN_ID`, `STATUS`, pass/fail counts).

## 3. Marketplace Manual Validation (Gate E1)

1. Publish listing using guidance in `05_phase3_data_products.sql` Part F and metadata from `marketplace_listing.txt`.
2. Validate from a separate consumer account:
   - Discover listing
   - Subscribe
   - Query all three products

After evidence is complete, either:

Option A: rerun pipeline with manual pass update:

```powershell
c:/tmp/snow/.venv/Scripts/python.exe c:/tmp/snow/python/orchestration/run_all.py \
  --run-acceptance-gates \
  --mark-marketplace-pass \
  --marketplace-note "Validated from consumer account on 2026-03-30"
```

Option B: update manually in Snowflake:

```sql
UPDATE RAW_SALES.MONITORING.manual_release_checks
SET CHECK_VALUE = TRUE,
    LAST_UPDATED_AT = CURRENT_TIMESTAMP(),
    UPDATED_BY = CURRENT_USER(),
    NOTES = 'Validated from consumer account on 2026-03-30'
WHERE CHECK_NAME = 'marketplace_consumer_test_passed';
```

Then rerun `acceptance_gates.sql`.

## 4. Validation Commands

```powershell
c:/tmp/snow/.venv/Scripts/python.exe c:/tmp/snow/python/validation/check_status.py
c:/tmp/snow/.venv/Scripts/python.exe c:/tmp/snow/python/validation/quick_check.py
c:/tmp/snow/.venv/Scripts/python.exe c:/tmp/snow/python/validation/verify_roles_sla.py
```

## 5. Troubleshooting

1. If auth prompt appears unexpectedly, verify `SNOWFLAKE_PASSWORD` or `SNOWFLAKE_TOKEN` in current terminal.
2. If gate `B5` fails, run validation tasks once and rerun gates.
3. If gate `E1` fails, ensure manual evidence flag is set to TRUE after real consumer test.
