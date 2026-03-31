param(
    [int]$Port = 8080,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
    throw "Venv not found at $venvPython. Run: python -m venv $repoRoot\.venv"
}

if (-not $env:SNOWFLAKE_TOKEN -and -not $env:SNOWFLAKE_PASSWORD) {
    throw "Set SNOWFLAKE_TOKEN or SNOWFLAKE_PASSWORD before running this script."
}

Write-Host "Starting marketplace UI (dev/uvicorn) on http://localhost:$Port ..." -ForegroundColor Green
Write-Host "Press Ctrl+C to stop." -ForegroundColor Yellow

if (-not $NoBrowser) {
    Start-Sleep -Seconds 1
    Start-Process "http://localhost:$Port"
}

Push-Location $repoRoot
try {
    & $venvPython -m uvicorn marketplace_ui.app:app --host 0.0.0.0 --port $Port --reload
} finally {
    Pop-Location
}
