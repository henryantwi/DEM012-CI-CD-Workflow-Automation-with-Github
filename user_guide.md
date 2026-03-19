# DEM012 — User Guide

> **Mini Data Platform: E-commerce Clickstream Analysis**

This guide walks you through everything you need to set up, operate, and extend the DEM012 Mini Data Platform — a containerised, end-to-end data pipeline that ingests synthetic e-commerce clickstream events, processes them through a 6-stage ETL pipeline, and surfaces results in a live dashboard.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Environment Setup](#3-environment-setup)
4. [Starting & Stopping the Platform](#4-starting--stopping-the-platform)
5. [Generating Data](#5-generating-data)
6. [Running the Pipeline](#6-running-the-pipeline)
7. [Exploring Results](#7-exploring-results)
8. [Running Tests](#8-running-tests)
9. [Code Quality & Linting](#9-code-quality--linting)
10. [CI/CD Pipeline](#10-cicd-pipeline)
11. [Environment Variables Reference](#11-environment-variables-reference)
12. [Common rav Commands](#12-common-rav-commands)
13. [Project Structure](#13-project-structure)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Overview

The platform is built as a hands-on CI/CD automation exercise using GitHub Actions. It implements a **fully incremental pipeline** — the Airflow DAG detects and processes only new/unprocessed event batches, while always recomputing full funnel metrics from all accumulated historical data.

### Key capabilities

| Capability | Detail |
|---|---|
| **Data Ingestion** | Synthetic e-commerce events via Faker + Polars, uploadable to MinIO in `seed` or `batch` modes |
| **Orchestration** | Apache Airflow 2.9 `@daily` DAG with 6 sequential tasks |
| **Validation** | Great Expectations suites on both raw and enriched data |
| **AI Enrichment** | Async concurrent LLM calls (Groq `llama-3.3-70b-versatile`) with semaphore rate limiting |
| **Storage** | MinIO (object store) + PostgreSQL 15 (data warehouse) |
| **Dashboard** | Metabase connected to PostgreSQL |
| **CI/CD** | GitHub Actions: lint → test → Docker startup → E2E validation |

---

## 2. Prerequisites

Before you begin, ensure the following are installed and available on your system:

| Tool | Version | Purpose |
|---|---|---|
| [Docker](https://docs.docker.com/get-docker/) | ≥ 24 (with Compose V2) | Runs all platform services |
| [uv](https://docs.astral.sh/uv/getting-started/installation/) | ≥ 0.5 | Python package/environment manager |
| [rav](https://github.com/jmitchel3/rav) | Latest | Shortcut CLI runner (`pip install rav`) |
| Groq API Key | — | Required for the AI enrichment task |

> **Get a free Groq API key** at [console.groq.com](https://console.groq.com) — no credit card required.

---

## 3. Environment Setup

### 3.1 Clone the repository

```bash
git clone <repo-url>
cd DEM012-CI-CD-Workflow-Automation-with-Github
```

### 3.2 Configure environment variables

Copy the example env file and fill in your secrets:

```bash
cp .env.example .env
```

Open `.env` and set the following required values:

| Variable | How to set |
|---|---|
| `GROQ_API_KEY` | Paste your key from [console.groq.com](https://console.groq.com) |
| `AIRFLOW__CORE__FERNET_KEY` | Run `rav x fernet-key` and paste the output |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Any random string (e.g. use `openssl rand -hex 32`) |

All other variables already have sensible defaults in `.env.example`.

### 3.3 Install Python dependencies (for local dev/testing)

```bash
uv sync
# or via rav:
rav x install
```

---

## 4. Starting & Stopping the Platform

### Start all services

```bash
docker compose up -d --build
# or via rav:
rav x up
```

Wait approximately **60 seconds** for all services to become healthy:

```bash
docker compose ps
# or via rav:
rav x ps
```

All services should display `(healthy)` in the status column.

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **Metabase** | http://localhost:3000 | Set on first launch |
| **PostgreSQL** | `localhost:5432` | `airflow` / `airflow` |

### Stop the platform

```bash
# Stop containers (preserve data volumes)
docker compose down
# or via rav:
rav x down

# Stop and remove all volumes (full reset)
docker compose down -v
# or via rav:
rav x down-v
```

> **Warning:** **`down-v` is destructive.** It deletes all stored data including PostgreSQL tables and MinIO buckets.

### View logs

```bash
docker compose logs -f
# or via rav:
rav x logs
```

---

## 5. Generating Data

The `data_generator` module creates synthetic e-commerce clickstream events using Faker and Polars, and uploads them directly to MinIO.

### Seed mode (first-time setup)

Uploads dimension tables (`users.csv`, `products.csv`) and an initial events batch:

```bash
rav x seed
# or explicitly:
uv run python data_generator/generate_data.py --mode seed
```

This creates in the MinIO `clickstream-data` bucket:
- `raw/users.csv`
- `raw/products.csv`
- `raw/events/batch_YYYYMMDD_HHmmss.csv`

### Batch mode (simulate ongoing data)

Generates and uploads a single new timestamped events batch:

```bash
rav x batch
# or explicitly:
uv run python data_generator/generate_data.py --mode batch
```

Run this multiple times to accumulate several batches and observe the incremental pipeline behaviour.

### Verify in MinIO

Open the MinIO Console at **http://localhost:9001**, navigate to the `clickstream-data` bucket, and confirm files appear under `raw/`.

> **Tip:** The number of events per batch is controlled by the `BATCH_EVENT_COUNT` environment variable (default: `500`).

---

## 6. Running the Pipeline

### Trigger via Airflow UI

1. Open the Airflow UI at **http://localhost:8080**
2. Log in with `admin` / `admin`
3. Find the `clickstream_pipeline` DAG
4. Toggle it **on** (if not already enabled)
5. Click the **Trigger DAG** button to run immediately, or let it run on its `@daily` schedule

### Trigger via CLI

```bash
docker compose exec airflow-webserver airflow dags trigger clickstream_pipeline
# or via rav:
rav x trigger
```

### Pipeline stages

The DAG executes the following 6 tasks in order:

| # | Task | What it does |
|---|---|---|
| 1 | `extract` | Scans MinIO `raw/events/` for batches not yet in `processed_batches`. Short-circuits if nothing new. |
| 2 | `validate_raw` | Runs Great Expectations suite: non-null IDs, valid event types, uniqueness. |
| 3 | `transform` | Sessionises new events with Polars; recomputes full funnel metrics from all historical batches. |
| 4 | `enrich_ai` | Calls Groq LLM (async, 2 concurrent max) to categorise products via structured output. |
| 5 | `validate_enriched` | GE suite: `category` not-null, conversion rates within `[0, 1]`. |
| 6 | `load` | Appends to `fact_events`; full-refreshes `dim_products` and `fact_funnel_metrics`; marks batches processed. |

### Validate the data flow (E2E check)

After a successful pipeline run, you can verify data flowed end-to-end:

```bash
rav x validate
# or explicitly:
uv run python scripts/validate_data_flow.py
```

---

## 7. Exploring Results

### Airflow

- View task logs by clicking any coloured task box in the DAG graph view
- Task state colours: **green** = success, **red** = failed, **yellow** = skipped/no new data

### PostgreSQL tables

Connect to PostgreSQL at `localhost:5432` (database: `airflow`, user: `airflow`, password: `airflow`) and query:

| Table | Description |
|---|---|
| `fact_events` | All individual clickstream events (incremental appends) |
| `fact_funnel_metrics` | Funnel conversion rates by product/category (full-refresh each run) |
| `dim_products` | Product dimension with AI-assigned categories (full-refresh each run) |
| `processed_batches` | Audit log of which MinIO batch files have been processed |

### Metabase dashboard

1. Navigate to **http://localhost:3000**
2. Complete the first-time setup wizard
3. Add a new **PostgreSQL** database connection:
   - Host: `postgres`
   - Port: `5432`
   - Database: `airflow`
   - Username: `airflow`
   - Password: `airflow`
4. Browse or create questions/dashboards from the tables above

---

## 8. Running Tests

The project has 83+ tests across 6 test files covering transformations, enrichment logic, data generation I/O, pipeline hardening, DAG triggering, and data flow validation.

```bash
uv run pytest tests/ -v
# or via rav:
rav x test
```

### Test files

| File | What it covers |
|---|---|
| `test_transformations.py` | Polars sessionisation and funnel metric logic |
| `test_enrich_ai_logic.py` | Async LLM enrichment, retry, and rate-limiting |
| `test_generate_data_io.py` | Data generator modes and MinIO upload |
| `test_pipeline_hardening.py` | Edge cases and failure handling in the DAG |
| `test_trigger_dag.py` | DAG trigger script behaviour |
| `test_validate_data_flow.py` | End-to-end data flow validation script |

Run a specific test file:

```bash
uv run pytest tests/test_transformations.py -v
```

Run tests matching a keyword:

```bash
uv run pytest tests/ -k "enrich" -v
```

---

## 9. Code Quality & Linting

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.

```bash
# Check for lint issues
uv run ruff check .
# or via rav:
rav x lint

# Auto-format code
uv run ruff format .
# or via rav:
rav x format

# Run all pre-commit hooks
uv run pre-commit run --all-files
# or via rav:
rav x pre-commit
```

Pre-commit hooks are also defined in `.pre-commit-config.yaml` and run automatically on `git commit`.

---

## 10. CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/ci_cd_pipeline.yml`) runs on every **push** and **pull request**.

### Workflow stages

```
Push / PR
    │
    ├─► Lint (ruff check)
    │
    ├─► Test (pytest — 83+ tests)
    │
    ├─► Docker Platform Startup
    │       └─ All services healthy
    │
    └─► E2E Validation
            └─ validate_data_flow.py
```

All stages run in parallel where possible. A failed lint or test check blocks the Docker startup stage.

### GitHub Secrets required

For the CI/CD workflow to run AI enrichment in E2E tests, add the following secret to your GitHub repository:

| Secret Name | Value |
|---|---|
| `GROQ_API_KEY` | Your Groq API key |

Set via: **Repository → Settings → Secrets and variables → Actions → New repository secret**

---

## 11. Environment Variables Reference

| Variable | Default | Description |
|---|---|---|
| `GROQ_API_KEY` | *(required)* | Groq API key for AI enrichment |
| `GROQ_MAX_CONCURRENT` | `2` | Max simultaneous async LLM requests |
| `GROQ_REQUESTS_PER_SECOND` | `0.8` | Max LLM request throughput |
| `GROQ_MAX_ATTEMPTS` | `8` | Max retries on rate-limit errors |
| `GROQ_RETRY_BASE_SECONDS` | `2` | Base backoff delay for retries |
| `GROQ_RETRY_MAX_SECONDS` | `30` | Max backoff cap |
| `BATCH_EVENT_COUNT` | `500` | Events generated per batch run |
| `POSTGRES_USER` | `airflow` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `airflow` | PostgreSQL password |
| `POSTGRES_DB` | `airflow` | PostgreSQL database name |
| `POSTGRES_HOST` | `postgres` | PostgreSQL host (Docker service name) |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO secret key |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO endpoint (internal Docker URL) |
| `MINIO_BUCKET` | `clickstream-data` | Target MinIO bucket name |
| `AIRFLOW__CORE__FERNET_KEY` | *(required)* | Fernet key for Airflow encryption |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | *(required)* | Secret key for Airflow webserver sessions |

---

## 12. Common rav Commands

[rav](https://github.com/jmitchel3/rav) provides shortcut commands defined in `rav.yaml`. Run any command with `rav x <name>`:

| Command | Equivalent | Description |
|---|---|---|
| `rav x fernet-key` | *(inline Python)* | Generate a new Airflow Fernet key |
| `rav x install` | `uv sync` | Install Python dependencies |
| `rav x up` | `docker compose up -d --build` | Build and start all services |
| `rav x down` | `docker compose down` | Stop services (keep volumes) |
| `rav x down-v` | `docker compose down -v` | Stop services and delete volumes |
| `rav x ps` | `docker compose ps` | Show service status |
| `rav x logs` | `docker compose logs -f` | Stream all service logs |
| `rav x seed` | `uv run python data_generator/generate_data.py --mode seed` | Upload initial data to MinIO |
| `rav x batch` | `uv run python data_generator/generate_data.py --mode batch` | Upload a new event batch |
| `rav x trigger` | `python scripts/trigger_dag.py` | Trigger the Airflow DAG |
| `rav x validate` | `uv run python scripts/validate_data_flow.py` | Run E2E data flow validation |
| `rav x test` | `uv run pytest tests/ -v` | Run full test suite |
| `rav x lint` | `uv run ruff check .` | Lint the codebase |
| `rav x format` | `uv run ruff format .` | Format code with Ruff |
| `rav x pre-commit` | `uv run pre-commit run --all-files` | Run all pre-commit hooks |

---

## 13. Project Structure

```
DEM012-CI-CD-Workflow-Automation-with-Github/
├── .github/
│   └── workflows/
│       └── ci_cd_pipeline.yml     # GitHub Actions CI/CD workflow
├── .agent/                        # AI assistant skills and workflows
├── dags/
│   ├── clickstream_pipeline.py    # Main Airflow DAG (6-stage ETL)
│   └── enrichment_logic.py        # Async LLM product enrichment
├── data_generator/
│   └── generate_data.py           # Faker-based data generator (seed/batch)
├── docker/
│   └── airflow/Dockerfile         # Custom Airflow image with dependencies
├── docs/screenshots/              # Pipeline and dashboard screenshots
├── great_expectations/
│   └── expectations/              # GE validation suite JSON files
├── scripts/
│   ├── trigger_dag.py             # Programmatic DAG trigger script
│   └── validate_data_flow.py      # End-to-end validation script
├── tests/                         # pytest test suite (83+ tests)
├── .env.example                   # Environment variable template
├── .pre-commit-config.yaml        # Pre-commit hook configuration
├── .python-version                # Python version pin (3.11)
├── docker-compose.yml             # Full platform service definition
├── pyproject.toml                 # Python project config (Ruff, pytest)
├── rav.yaml                       # rav shortcut commands
├── requirements.txt               # Airflow container pip dependencies
└── user_guide.md                  # This file
```

---

## 14. Troubleshooting

### Services not reaching "healthy" state

```bash
# Check individual service logs
docker compose logs airflow-webserver
docker compose logs postgres
docker compose logs minio
```

Common causes:
- **Fernet key not set** — run `rav x fernet-key` and paste the output into `.env`
- **Port conflicts** — ensure ports `8080`, `9000`, `9001`, `3000`, and `5432` are not in use
- **Insufficient Docker memory** — allocate at least **4 GB RAM** to Docker Desktop

---

### Airflow DAG skips or does nothing

The `extract` task short-circuits (marks downstream tasks as skipped) if no new/unprocessed batches are found in MinIO. This is expected behaviour.

**Fix:** Run `rav x batch` to upload a new event batch, then re-trigger the DAG.

---

### Groq AI enrichment fails / rate-limit errors

- Verify `GROQ_API_KEY` is set correctly in `.env`
- Reduce concurrency: set `GROQ_MAX_CONCURRENT=1` and `GROQ_REQUESTS_PER_SECOND=0.5` in `.env`
- Check your Groq usage quota at [console.groq.com](https://console.groq.com)

---

### MinIO connection errors from Airflow

Airflow tasks use the internal Docker network hostname `minio:9000`. If running tasks **outside** Docker (e.g., local scripts), update `MINIO_ENDPOINT` in `.env` to `http://localhost:9000`.

---

### Tests failing locally

Ensure Python dependencies match the lock file:

```bash
uv sync
uv run pytest tests/ -v
```

If tests fail due to Groq calls, most enrichment tests mock the LLM client. Verify mock patches are not broken by dependency upgrades.

---

### Resetting to a clean state

```bash
# Remove all containers, networks, and volumes
rav x down-v

# Re-seed fresh data
rav x up
rav x seed
```

---

*Generated: 2026-03-19 | DEM012 Mini Data Platform*
