param(
    [string]$ImageName    = "marketplace-ui",
    [int]$Port            = 8081,
    [string]$Account      = "hhtxheq-ba04062",
    [string]$User         = "tarnaka",
    [string]$Role         = "ACCOUNTADMIN",
    [string]$Warehouse    = "ANALYTICS_WH",
    [string]$Database     = "RAW_SALES",
    [string]$Schema       = "MONITORING"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command podman -ErrorAction SilentlyContinue)) {
    throw "Podman CLI is not installed. See: https://podman.io/getting-started/installation"
}

if (-not $env:SNOWFLAKE_TOKEN -and -not $env:SNOWFLAKE_PASSWORD) {
    throw "Set SNOWFLAKE_TOKEN or SNOWFLAKE_PASSWORD before running this script."
}

# Ensure Podman machine is running
Write-Host "Ensuring Podman machine is running..." -ForegroundColor Cyan
$machineState = podman machine list --format "{{.Running}}" 2>$null | Select-Object -First 1
if ($machineState -ne "true") {
    podman machine start
}

$repoRoot  = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dockerfile = "marketplace_ui/Dockerfile"

Write-Host "Building image '$ImageName'..." -ForegroundColor Cyan
podman build -t $ImageName -f $dockerfile $repoRoot

# Remove any existing container with this name
podman rm -f $ImageName 2>$null | Out-Null

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

# Resolve Podman VM IP for browser link
$vmIp = (podman machine ssh "ip addr show eth0 | grep 'inet '" 2>$null) -replace '.*inet ([0-9.]+)/.*','$1'
$browserUrl = if ($vmIp) { "http://${vmIp}:${Port}" } else { "http://localhost:${Port}" }

Write-Host "Starting container '$ImageName' on $browserUrl ..." -ForegroundColor Green
podman run --rm --name $ImageName -p "${Port}:8080" @envArgs $ImageName

# This line only reached after container exits
Write-Host "Container stopped." -ForegroundColor Yellow
