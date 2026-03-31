param(
    [string]$ImageName    = "marketplace-ui",
    [int]$Port            = 8080,
    [string]$Account      = "hhtxheq-ba04062",
    [string]$User         = "tarnaka",
    [string]$Role         = "ACCOUNTADMIN",
    [string]$Warehouse    = "ANALYTICS_WH",
    [string]$Database     = "RAW_SALES",
    [string]$Schema       = "MONITORING"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI is not installed. Install Docker Desktop first."
}

if (-not $env:SNOWFLAKE_TOKEN -and -not $env:SNOWFLAKE_PASSWORD) {
    throw "Set SNOWFLAKE_TOKEN or SNOWFLAKE_PASSWORD before running this script."
}

$repoRoot   = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dockerfile = "marketplace_ui/Dockerfile"

Write-Host "Building image '$ImageName'..." -ForegroundColor Cyan
docker build -t $ImageName -f $dockerfile $repoRoot

# Remove any existing container with this name
docker rm -f $ImageName 2>$null | Out-Null

# Build env var list
$envArgs = @(
    "--env", "SNOWFLAKE_ACCOUNT=$Account",
    "--env", "SNOWFLAKE_USER=$User",
    "--env", "SNOWFLAKE_ROLE=$Role",
    "--env", "SNOWFLAKE_WAREHOUSE=$Warehouse",
    "--env", "SNOWFLAKE_DATABASE=$Database",
    "--env", "SNOWFLAKE_SCHEMA=$Schema"
)
if ($env:SNOWFLAKE_TOKEN)    { $envArgs += "--env"; $envArgs += "SNOWFLAKE_TOKEN=$env:SNOWFLAKE_TOKEN" }
if ($env:SNOWFLAKE_PASSWORD) { $envArgs += "--env"; $envArgs += "SNOWFLAKE_PASSWORD=$env:SNOWFLAKE_PASSWORD" }

Write-Host "Starting container '$ImageName' on http://localhost:$Port ..." -ForegroundColor Green
Start-Process "http://localhost:$Port"
docker run --rm --name $ImageName -p "${Port}:8080" @envArgs $ImageName

# This line only reached after container exits
Write-Host "Container stopped." -ForegroundColor Yellow
