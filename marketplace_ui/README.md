# Custom Data Marketplace UI (Stretch)

This is a custom portal for product discovery, SLA visibility, and AI metadata search on top of Snowflake.

## Features

1. Product catalog from `RAW_SALES.MONITORING.product_sla_status`
2. SLA status and row count visibility per product
3. Metadata search over `RAW_SALES.GOLD.ai_semantic_metadata`
4. Optional manual Marketplace gate update endpoint
5. Health endpoint validating Snowflake connectivity

## Stack

1. FastAPI backend
2. Jinja2 templating
3. Vanilla JS frontend
4. Snowflake Python connector

## Prerequisites

1. `~/.snowflake/connections.toml` exists
2. Either `SNOWFLAKE_TOKEN` or `SNOWFLAKE_PASSWORD` is set
3. Workspace venv available at `c:/tmp/snow/.venv`

## Install dependencies

```powershell
c:/tmp/snow/.venv/Scripts/pip.exe install -r c:/tmp/snow/marketplace_ui/requirements.txt
```

## Run locally

```powershell
$env:SNOWFLAKE_TOKEN = "<token>"
c:/tmp/snow/.venv/Scripts/python.exe -m uvicorn marketplace_ui.app:app --host 0.0.0.0 --port 8080 --reload
```

Open: `http://localhost:8080`

## Run with Docker

Build image from workspace root:

```powershell
docker build -t snow-marketplace-ui -f marketplace_ui/Dockerfile .
```

Run container (token auth):

```powershell
docker run --rm -p 8080:8080 \
	-e SNOWFLAKE_TOKEN=$env:SNOWFLAKE_TOKEN \
	-v "$HOME/.snowflake:/root/.snowflake:ro" \
	snow-marketplace-ui
```

Run container (password auth):

```powershell
docker run --rm -p 8080:8080 \
	-e SNOWFLAKE_PASSWORD=$env:SNOWFLAKE_PASSWORD \
	-v "$HOME/.snowflake:/root/.snowflake:ro" \
	snow-marketplace-ui
```

## Run with Podman

Build image from workspace root:

```powershell
podman machine init
podman machine start
podman build -t snow-marketplace-ui -f marketplace_ui/Dockerfile .
```

Run container (token auth):

```powershell
podman run --rm -p 8080:8080 \
	-e SNOWFLAKE_TOKEN=$env:SNOWFLAKE_TOKEN \
	-v "$HOME/.snowflake:/root/.snowflake:ro" \
	snow-marketplace-ui
```

You can also use the one-command launcher:

```powershell
./marketplace_ui/run_podman.ps1
```

## Suggested deployment path (AWS)

1. Deploy as containerized FastAPI app on AWS App Runner or ECS Fargate
2. Store Snowflake token in AWS Secrets Manager
3. Attach ALB or App Runner public URL for interview demo
4. Add auth layer (Cognito or basic SSO) if needed
