# DEM012 — Mini Data Platform: E-commerce Clickstream Analysis

A containerised data platform that ingests synthetic e-commerce clickstream events,
runs a multi-stage ETL pipeline (extraction → validation → transformation →
AI enrichment → load), and surfaces the results in a dashboard.

Supports **incremental ingestion**: the data generator produces timestamped batch files,
and the DAG processes only new/unprocessed batches while recalculating funnel metrics
from all accumulated data. AI enrichment uses **async concurrent LLM calls** with
semaphore-based rate limiting.

Built as a hands-on CI/CD automation exercise using GitHub Actions.

---

## Architecture

```mermaid
graph LR
    GEN["data_generator\nFaker + Polars\n--mode seed | batch"]

    subgraph Storage
        MINIO["MinIO\nS3-compatible\nobject store\nraw/events/batch_*.csv"]
    end

    subgraph Orchestration
        AIRFLOW["Airflow 2.9\nLocalExecutor"]
        subgraph DAG: clickstream_pipeline
            E["1 · extract\nscan for new batches"]
            VR["2 · validate_raw\nGreat Expectations"]
            T["3 · transform\nPolars sessionisation\n+ full funnel recalc"]
            AI["4 · enrich_ai\nasync LLM calls\ngroq:llama-3.3-70b"]
            VE["5 · validate_enriched\nGreat Expectations"]
            L["6 · load\nappend events +\nfull-refresh metrics"]
        end
        E --> VR --> T --> AI --> VE --> L
    end

    subgraph Database
        PG["PostgreSQL 15\ndim_products\nfact_funnel_metrics\nfact_events\nprocessed_batches"]
    end

    subgraph Dashboard
        MB["Metabase\n:3000"]
    end

    GEN -->|upload CSV batches| MINIO
    MINIO -->|download new batches| E
    L -->|write rows| PG
    PG -->|SQL| MB
```

### Pipeline Tasks

| # | Task | What it does |
|---|------|-------------|
| 1 | `extract` | Scan MinIO `raw/events/` for new batch files not in `processed_batches` table. Download new batches + dimension tables. Skip downstream if no new data. |
| 2 | `validate_raw` | GE suite per batch: non-null IDs, valid event types, uniqueness checks |
| 3 | `transform` | Sessionise new events; read ALL batches from MinIO to recompute full funnel metrics |
| 4 | `enrich_ai` | `asyncio.gather` with `Semaphore(2)` + rate-gate throttling for concurrent LLM calls. Structured output via `init_chat_model("groq:llama-3.3-70b-versatile").with_structured_output(ProductCategory)` |
| 5 | `validate_enriched` | GE suite: category not-null, conversion rates in [0, 1] |
| 6 | `load` | Append new events to `fact_events`, full-refresh `dim_products` and `fact_funnel_metrics`, record batch keys in `processed_batches` |

### Data Flow

- **Dimension tables** (`users.csv`, `products.csv`): uploaded once via `seed` mode, full-refresh in PostgreSQL
- **Events**: partitioned into timestamped batches (`raw/events/batch_YYYYMMDD_HHmmss.csv`), appended to `fact_events`
- **Funnel metrics**: recalculated from ALL events in MinIO on each run, full-refresh in `fact_funnel_metrics`

---

## Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Package manager | [uv](https://docs.astral.sh/uv/) |
| Data processing | [Polars](https://pola.rs/) |
| Data validation | [Great Expectations](https://greatexpectations.io/) ≥ 0.18 |
| AI enrichment | [LangChain](https://python.langchain.com/) + Groq `llama-3.3-70b-versatile` (async) |
| Orchestration | Apache Airflow 2.9 (TaskFlow API) |
| Object storage | MinIO |
| Database | PostgreSQL 15 |
| Dashboard | Metabase |
| CI/CD | GitHub Actions |
| Containers | Docker Compose |

---

## Project Structure

```
.
├── .github/workflows/       # CI/CD pipeline (GitHub Actions)
├── dags/
│   ├── clickstream_pipeline.py   # Main Airflow DAG (6-stage ETL)
│   └── enrichment_logic.py       # Async LLM enrichment with rate limiting
├── data_generator/
│   └── generate_data.py          # Faker-based data generator (seed/batch modes)
├── docker/
│   └── airflow/Dockerfile        # Custom Airflow image
├── docs/screenshots/             # Dashboard and pipeline screenshots
├── great_expectations/
│   └── expectations/             # GE validation suites (JSON)
├── scripts/
│   └── validate_data_flow.py     # E2E data flow validation script
├── tests/                        # pytest test suite (83 tests)
├── docker-compose.yml            # Full platform definition
├── pyproject.toml                # Python project config
└── requirements.txt              # Airflow container dependencies
```

---

## Screenshots

### Pipeline Execution (Airflow)

| DAG Task Graph | Successful DAG Run |
|:-:|:-:|
| ![Task Graph](docs/screenshots/airflow-task-graph.png) | ![DAG Run](docs/screenshots/airflow-dag-run-success.png) |

### Data Storage (MinIO)

![MinIO Raw Files](docs/screenshots/minio-raw-files.png)

### Dashboards (Metabase)

| Metabase Home | Funnel Metrics Table |
|:-:|:-:|
| ![Home](docs/screenshots/metabase-home.png) | ![Funnel Metrics](docs/screenshots/metabase-fact-funnel-metrics-table.png) |

| Conversion Rates Chart | Products by Category |
|:-:|:-:|
| ![Conversion Rates](docs/screenshots/metabase-conversion-rates-chart.png) | ![Products by Category](docs/screenshots/metabase-products-by-category-chart.png) |

---

## Prerequisites

- Docker ≥ 24 with Docker Compose V2
- [uv](https://docs.astral.sh/uv/getting-started/installation/) ≥ 0.5
- A Groq API key (for the AI enrichment task)

---

## Quick Start

### 1 — Clone and configure environment

```bash
git clone <repo-url>
cd DEM012-CI-CD-Workflow-Automation-with-Github

cp .env.example .env
# Then open .env and fill in:
#   GROQ_API_KEY=gsk-...
#   AIRFLOW__CORE__FERNET_KEY=<run: rav x fernet-key>
#   AIRFLOW__WEBSERVER__SECRET_KEY=<random string>
```

### 2 — Install Python dependencies (local dev / testing)

```bash
uv sync
```

### 3 — Start the platform

```bash
docker compose up -d --build
```

Wait ~60 s for all services to become healthy:

```bash
docker compose ps   # all services should show "(healthy)"
```

### 4 — Generate and upload sample data

```bash
# Seed mode: uploads users, products, and initial events batch
rav x seed
# or: uv run python data_generator/generate_data.py --mode seed
```

This creates `raw/users.csv`, `raw/products.csv`, and a timestamped events batch
(`raw/events/batch_YYYYMMDD_HHmmss.csv`) in the MinIO bucket.

To add more events later (simulating ongoing data):

```bash
# Batch mode: generates one new timestamped events batch
rav x batch
# or: uv run python data_generator/generate_data.py --mode batch
```

Verify via the MinIO Console at **http://localhost:9001**.

### 5 — Trigger the pipeline

Open the Airflow UI at **http://localhost:8080** (admin / admin).

Either wait for the `@daily` schedule or trigger manually:

```bash
docker compose exec airflow-webserver airflow dags trigger clickstream_pipeline
```

The pipeline will:
1. Detect new event batches not yet processed
2. Validate, transform, enrich, and load them
3. Skip gracefully if no new batches exist

### 6 — Explore results in Metabase

1. Navigate to **http://localhost:3000**
2. Add a PostgreSQL database connection
3. Browse tables: `dim_products`, `fact_funnel_metrics`, `fact_events`

---

## Running Tests

```bash
uv run pytest tests/ -v
```

---

## Linting

```bash
uv run ruff check .
uv run ruff format .
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GROQ_API_KEY` | Required for AI enrichment |
| `GROQ_MAX_CONCURRENT` | Max concurrent async LLM calls (default: `2`) |
| `GROQ_REQUESTS_PER_SECOND` | Max request pace (default: `0.8`) |
| `GROQ_MAX_ATTEMPTS` | Max retry attempts on rate-limit errors (default: `8`) |
| `BATCH_EVENT_COUNT` | Events per batch in batch mode (default: `500`) |
| `POSTGRES_USER/PASSWORD/DB` | PostgreSQL credentials |
| `MINIO_ROOT_USER/ROOT_PASSWORD` | MinIO access credentials |

---

## License

MIT
