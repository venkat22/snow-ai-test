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

# Default to password auth with app_service_user
if (-not $env:SNOWFLAKE_PASSWORD) {
    $env:SNOWFLAKE_PASSWORD = 'ChangeMe!Str0ng#2026'
}
if (-not $env:SNOWFLAKE_USER) {
    $env:SNOWFLAKE_USER = 'app_service_user'
}
if (-not $env:SNOWFLAKE_ACCOUNT) {
    $env:SNOWFLAKE_ACCOUNT = 'hhtxheq-ba04062'
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
