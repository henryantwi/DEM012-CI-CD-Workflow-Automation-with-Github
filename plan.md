# Plan: Real-World Clickstream Platform Upgrade

## TL;DR
Upgrade the mini data platform from a one-shot seed-and-process demo to a realistic incremental ingestion pipeline with async AI enrichment. The data generator produces timestamped partitioned files; the DAG incrementally appends new events but recalculates funnel metrics from all accumulated data; AI enrichment uses `asyncio.gather` with a semaphore for concurrent LLM calls. Full overhaul of generator, DAG, enrichment logic, tests, GE suites, validation, and README.

---

## Phase 1: Incremental Data Generator

**Goal**: Produce timestamped/partitioned CSV files that simulate real data arriving over time.

### Steps
1. **Add a streaming mode to `data_generator/generate_data.py`**
   - Keep existing `main()` as `seed_mode()` for initial reference data (users, products) — these are uploaded once as `raw/users.csv` and `raw/products.csv` (dimension tables change slowly)
   - Add `stream_batch()` function that generates a single batch of events with:
     - Timestamped key: `raw/events/batch_YYYYMMDD_HHmmss.csv` (partitioned by timestamp)
     - Configurable batch size via `BATCH_EVENT_COUNT` env var (default: 500)
     - Uses existing user_ids and product_ids (reads them from MinIO or generates deterministically)
   - Add CLI interface: `--mode seed|batch` flag
     - `seed`: uploads users.csv, products.csv, and one initial events batch
     - `batch`: generates and uploads only a new events batch (for scheduled/repeated calls)
   - Events in batch mode use `datetime.now(UTC)` as the base time window (last ~1 hour of events)

2. **Add `rav.yaml` commands**
   - `seed`: runs `--mode seed` (initial setup, same as before)
   - `batch`: runs `--mode batch` (generate one new batch)

### Files modified
- `data_generator/generate_data.py` — add `stream_batch()`, CLI `--mode`, partitioned uploads
- `rav.yaml` — add `batch` command

---

## Phase 2: Hybrid Incremental DAG

**Goal**: DAG scans MinIO for new/unprocessed event batches, appends them to PostgreSQL, then recalculates funnel metrics from all accumulated data.

### Steps
3. **Add tracking table in PostgreSQL for processed batches**
   - `processed_batches` table: `(batch_key TEXT PRIMARY KEY, processed_at TIMESTAMPTZ)`
   - Created in the `load` task's `CREATE TABLE IF NOT EXISTS` block

4. **Rewrite `extract()` task** *(depends on step 3)*
   - List all objects under `raw/events/` prefix in MinIO
   - Query `processed_batches` table for already-processed keys
   - Download only new/unprocessed batch files
   - Still download `raw/users.csv` and `raw/products.csv` (dimension data)
   - Return dict with `event_files: list[str]` (paths), `users: str`, `products: str`
   - If no new batches found, raise `AirflowSkipException` to skip downstream tasks gracefully

5. **Rewrite `validate_raw()` task** *(depends on step 4)*
   - Validate each new event batch file against GE suite
   - Validate users/products as before (dimension tables)

6. **Rewrite `transform()` task** *(depends on step 5)*
   - Concatenate all new event batch CSVs into one DataFrame
   - Sessionise the new events batch
   - **For funnel metrics**: query PostgreSQL for existing aggregated events + merge with new events, OR read ALL events from MinIO (all batches) and recompute
   - Recommended: read all events from MinIO (simpler, more correct, supports backfill)
   - Compute funnel metrics from the full dataset

7. **Rewrite `load()` task** *(depends on step 6)*
   - **dim_products**: keep TRUNCATE + INSERT (dimension table, full refresh is fine)
   - **fact_funnel_metrics**: TRUNCATE + INSERT with recalculated metrics (since we recalculate from all data)
   - **raw events**: INSERT new event rows into a new `fact_events` staging table (append only)
   - Record processed batch keys in `processed_batches` table
   - All in one atomic transaction

### Files modified
- `dags/clickstream_pipeline.py` — rewrite extract, validate_raw, transform, load tasks

---

## Phase 3: Async AI Enrichment

**Goal**: Use `asyncio` to make concurrent LLM calls with semaphore-based rate limiting instead of sequential blocking calls.

### Steps
8. **Rewrite `dags/enrichment_logic.py` to async**
   - Convert `invoke_with_rate_limit_and_retries()` → `async_invoke_with_retries()`
     - Uses `asyncio.Semaphore` for concurrency control (e.g., 3 concurrent calls)
     - Uses `asyncio.sleep()` instead of `time.sleep()` for non-blocking waits
     - Exponential backoff on 429 errors remains
   - Convert `enrich_products_with_llm()` → `async_enrich_products_with_llm()`
     - Collects unique product names
     - Launches all LLM calls via `asyncio.gather(*tasks)` with semaphore limiting
     - Still deduplicates by product name
   - Add sync wrapper `enrich_products_with_llm()` that calls `asyncio.run(async_enrich_products_with_llm(...))` for backward compatibility with Airflow's sync task execution
   - Add `EnrichmentConfig.max_concurrent_requests` field (default: 3)
   - New env var: `GROQ_MAX_CONCURRENT=3`

9. **Update `enrich_ai()` DAG task** *(depends on step 8)*
   - Use the new async-backed enrichment function (sync wrapper)
   - Only enrich products that don't already have a category in the DB (incremental enrichment)

### Files modified
- `dags/enrichment_logic.py` — async rewrite with asyncio.gather + semaphore
- `dags/clickstream_pipeline.py` — update enrich_ai task call

---

## Phase 4: Updated Tests

**Goal**: Full test coverage for all new patterns.

### Steps
10. **Update `tests/test_generate_data_io.py`**
    - Test `stream_batch()` produces correctly timestamped keys
    - Test `--mode seed` vs `--mode batch` CLI behavior
    - Test partitioned key format: `raw/events/batch_YYYYMMDD_HHmmss.csv`
    - Keep existing generator shape tests

11. **Update `tests/test_transformations.py`**
    - Test funnel metrics computed from multiple concatenated batch DataFrames
    - Test sessionisation across batch boundaries (events from different batches for same user)

12. **Update `tests/test_enrich_ai_logic.py`** *(depends on step 8)*
    - Test async enrichment with mocked async LLM
    - Test semaphore limits concurrent calls
    - Test async retry behavior on 429
    - Test sync wrapper calls asyncio.run correctly
    - Keep existing deduplication and fail-open tests (adapted for async)

13. **Update `tests/test_pipeline_hardening.py`**
    - Test extract task handles empty batch list (AirflowSkipException)
    - Test processed_batches tracking
    - Update GE suite structure tests if suites change

14. **Update `tests/test_validate_data_flow.py`**
    - Test validation checks for partitioned events (not single events.csv)
    - Test fact_events table existence check

### Files modified
- `tests/test_generate_data_io.py`
- `tests/test_transformations.py`
- `tests/test_enrich_ai_logic.py`
- `tests/test_pipeline_hardening.py`
- `tests/test_validate_data_flow.py`

---

## Phase 5: GE Suites & Validation Scripts

### Steps
15. **Update GE suites**
    - `raw_events_suite.json` — adjust min row count expectation for per-batch validation (smaller batches)
    - Other suites remain largely unchanged

16. **Update `scripts/validate_data_flow.py`**
    - Check for `raw/events/` prefix objects in MinIO (not single `raw/events.csv`)
    - Check `fact_events` table row count > 0
    - Check `processed_batches` table has entries
    - Keep Metabase health check

### Files modified
- `great_expectations/expectations/raw_events_suite.json`
- `scripts/validate_data_flow.py`

---

## Phase 6: Documentation

### Steps
17. **Update `README.md`**
    - Document new real-world data flow architecture
    - Document `rav x seed` vs `rav x batch` commands
    - Update architecture diagram
    - Document async enrichment behavior
    - Document incremental pipeline behavior

### Files modified
- `README.md`

---

## Relevant Files (Full List)

| File | What changes |
|------|-------------|
| `data_generator/generate_data.py` | Add `stream_batch()`, CLI `--mode seed\|batch`, partitioned event uploads |
| `dags/clickstream_pipeline.py` | Rewrite extract (scan for new batches), transform (full recompute), load (append events + track batches) |
| `dags/enrichment_logic.py` | Async rewrite: `asyncio.gather` + `Semaphore` for concurrent LLM calls |
| `rav.yaml` | Add `batch` command |
| `tests/test_generate_data_io.py` | Tests for batch mode, partitioned keys |
| `tests/test_transformations.py` | Tests for multi-batch concatenation, cross-batch sessions |
| `tests/test_enrich_ai_logic.py` | Tests for async enrichment, semaphore, async retry |
| `tests/test_pipeline_hardening.py` | Tests for skip-on-empty, batch tracking |
| `tests/test_validate_data_flow.py` | Tests for partitioned validation checks |
| `great_expectations/expectations/raw_events_suite.json` | Lower min row count for per-batch validation |
| `scripts/validate_data_flow.py` | Check partitioned events, fact_events table, processed_batches |
| `README.md` | Full documentation update |

---

## Verification

1. **Unit tests pass**: `uv run pytest tests/ -v` — all existing + new tests green
2. **Seed mode works**: `rav x seed` uploads users.csv, products.csv, and first events batch to `raw/events/batch_*.csv`
3. **Batch mode works**: `rav x batch` generates a new timestamped events CSV in MinIO
4. **DAG processes incrementally**: trigger DAG → it picks up only unprocessed batches, appends events, recalculates funnel metrics
5. **DAG skips when no new data**: trigger DAG with no new batches → tasks skip gracefully
6. **Async enrichment**: AI enrichment makes concurrent LLM calls (visible in logs: multiple calls overlapping)
7. **E2E validation**: `rav x validate` passes with partitioned data structure
8. **Lint clean**: `rav x lint` passes

---

## Decisions

- **Dimension tables (users, products)** are still full-refresh (TRUNCATE + INSERT) — they change slowly
- **Events** are append-only into `fact_events` table
- **Funnel metrics** are recalculated from ALL events in MinIO (not just new ones) for accuracy, then full-refreshed in `fact_funnel_metrics`
- **Batch tracking** via PostgreSQL `processed_batches` table (simple, no external state store needed)
- **Async enrichment** uses sync wrapper (`asyncio.run`) since Airflow tasks are synchronous — the concurrency benefit is within the LLM calls themselves
- **Backward compatibility**: `rav x seed` still works as a one-command setup for new users
