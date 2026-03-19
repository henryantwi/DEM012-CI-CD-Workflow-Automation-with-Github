# DEM012 Full-Project Architecture

## 1. Developer / GitHub
- Developer
- GitHub Repository

Flow:
- Developer -> GitHub Repository: `commit / PR`

## 2. CI/CD Lane (GitHub Actions)
Trigger:
- `push` / `pull_request` to `main`

Parallel checks:
- Unit Tests
- Ruff Code Quality
- Docker Build Test
- Airflow DAG Validation
- Great Expectations Tests
- Transformations + AI Tests

Conditional integration flow:
- Docker Compose Platform Startup
- End-to-End Validation

Flows:
- GitHub Repository -> Workflow Trigger: `workflow trigger`
- Workflow Trigger -> Parallel checks
- Parallel checks -> Docker Compose Platform Startup: `main push only`
- Docker Compose Platform Startup -> End-to-End Validation

## 3. Runtime Platform Lane (Docker Compose)
Core services:
- Data Generator
  - `seed`
  - `batch`
  - Faker + Polars
- MinIO Object Storage
  - `raw/users.csv`
  - `raw/products.csv`
  - `raw/events/batch_*.csv`
- Airflow 2.9 Platform
  - Webserver
  - Scheduler
  - Init
- Groq via LangChain
  - structured output
- PostgreSQL 15
  - `dim_products`
  - `fact_funnel_metrics`
  - `fact_events`
  - `processed_batches`
- Metabase Dashboards

Airflow DAG:
- `extract`
- `validate_raw`
- `transform`
- `enrich_ai`
- `validate_enriched`
- `load`

Flows:
- Data Generator -> MinIO: `upload seed/batch CSVs`
- MinIO -> Airflow: `read raw batches + dimensions`
- `extract -> validate_raw -> transform -> enrich_ai -> validate_enriched -> load`
- `enrich_ai` -> Groq via LangChain: `async LLM categorization`
- `load` -> PostgreSQL: `append events + refresh marts`
- PostgreSQL -> Metabase: `dashboard queries`

## 4. CI/CD to Runtime Validation Links
- Docker Compose Platform Startup -> Runtime Platform: `build + validate platform`
- End-to-End Validation -> MinIO: `health + data checks`
- End-to-End Validation -> PostgreSQL: `health + data checks`
- End-to-End Validation -> Metabase: `health + data checks`


```mermaid
graph LR
  DEV[Developer] -->|commit / PR| REPO[GitHub Repository]
  REPO -->|workflow trigger| TRIGGER[Workflow Trigger<br/>push / pull_request to main]

  subgraph CI_CD[CI/CD Lane - GitHub Actions]
    TRIGGER --> UT[Unit Tests]
    TRIGGER --> RUFF[Ruff Code Quality]
    TRIGGER --> DBUILD[Docker Build Test]
    TRIGGER --> DAGV[Airflow DAG Validation]
    TRIGGER --> GET[Great Expectations Tests]
    TRIGGER --> TAI[Transformations + AI Tests]

    UT --> START[Docker Compose Platform Startup]
    RUFF --> START
    DBUILD --> START
    DAGV --> START
    GET --> START
    TAI --> START
    START --> E2E[End-to-End Validation]
  end

  subgraph RUNTIME[Runtime Platform Lane - Docker Compose]
    GEN[Data Generator<br/>seed | batch<br/>Faker + Polars]
    MINIO[MinIO Object Storage<br/>raw/users.csv<br/>raw/products.csv<br/>raw/events/batch_*.csv]

    subgraph AIRFLOW[Airflow 2.9 Platform]
      EX[extract] --> VR[validate_raw] --> TF[transform] --> AI[enrich_ai] --> VE[validate_enriched] --> LD[load]
    end

    GROQ[Groq via LangChain<br/>structured output]
    PG[PostgreSQL 15<br/>dim_products<br/>fact_funnel_metrics<br/>fact_events<br/>processed_batches]
    MB[Metabase Dashboards]

    GEN -->|upload seed/batch CSVs| MINIO
    MINIO -->|read raw batches + dimensions| EX
    AI -->|async LLM categorization| GROQ
    LD -->|append events + refresh marts| PG
    PG -->|dashboard queries| MB
  end

  START -->|build + validate platform| AIRFLOW
  E2E -.->|health + data checks| MINIO
  E2E -.->|health + data checks| PG
  E2E -.->|health + data checks| MB
```
