"""
Clickstream Pipeline DAG
========================
Orchestrates the full ETL + AI enrichment flow:

  extract → validate_raw → transform → enrich_ai → validate_enriched → load

Data flow:  MinIO (raw CSV batches) → Airflow (Polars + LangChain) → PostgreSQL

Incremental behaviour:
  - extract scans MinIO for new event batches not yet in `processed_batches`
  - transform recalculates funnel metrics from ALL events across all batches
  - load appends new events to `fact_events` and records processed batch keys
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Literal

import polars as pl
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

try:
    from enrichment_logic import build_enrichment_config_from_env, enrich_products_with_llm
except ModuleNotFoundError:
    from dags.enrichment_logic import build_enrichment_config_from_env, enrich_products_with_llm

# ── DAG default args ───────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda ctx: logger.error(
        "🔴 TASK FAILED: dag=%s task=%s run=%s try=%s exception=%s",
        ctx.get("dag").dag_id if ctx.get("dag") else "unknown",
        ctx.get("task_instance").task_id if ctx.get("task_instance") else "unknown",
        ctx.get("dag_run").run_id if ctx.get("dag_run") else "unknown",
        ctx.get("task_instance").try_number if ctx.get("task_instance") else "?",
        ctx.get("exception", "N/A"),
    ),
    # To add email/Slack alerts, configure Airflow SMTP or use:
    #   "email_on_failure": True, "email": ["team@example.com"]
}

# ── Environment ────────────────────────────────────────────────────────────────
# Deferred to task execution to avoid crashing the DAG parser (which blocks ALL DAGs).


def _get_minio_config() -> tuple[str, str, str, str]:
    return (
        os.environ["MINIO_ENDPOINT"],
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
        os.environ["MINIO_BUCKET"],
    )


def _get_pg_conn() -> str:
    return (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )


GE_SUITES_DIR = "/opt/airflow/great_expectations/expectations"

# Session gap for sessionisation: 30 minutes without activity = new session
SESSION_GAP_SECONDS = 30 * 60
logger = logging.getLogger(__name__)


# ── Pydantic schema for LangChain Structured Output ───────────────────────────
class ProductCategory(BaseModel):
    """AI-determined category for a product."""

    category: Literal[
        "Electronics",
        "Computers & Accessories",
        "Home & Kitchen",
        "Sports & Outdoors",
        "Clothing & Apparel",
        "Health & Beauty",
        "Books & Stationery",
        "Other",
    ] = Field(description="The most fitting product category")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score between 0 and 1")


def run_ge_suite(df: pl.DataFrame, suite_path: str, task_name: str) -> None:
    """Run a Great Expectations suite against an in-memory dataframe."""
    import great_expectations as gx

    with open(suite_path, encoding="utf-8") as f:
        suite_def = json.load(f)

    dataset = gx.from_pandas(df.to_pandas())
    failed_expectations: list[str] = []

    for expectation in suite_def.get("expectations", []):
        expectation_type = expectation["expectation_type"]
        kwargs = expectation.get("kwargs", {})
        expectation_fn = getattr(dataset, expectation_type, None)

        if expectation_fn is None:
            raise AttributeError(f"[{task_name}] Unsupported expectation: {expectation_type}")

        result = expectation_fn(**kwargs)
        success = (
            result.get("success", False)
            if isinstance(result, dict)
            else bool(getattr(result, "success", False))
        )

        if not success:
            failed_expectations.append(f"{expectation_type}({kwargs})")

    if failed_expectations:
        raise ValueError(
            f"[{task_name}] {len(failed_expectations)} expectation(s) failed: "
            + "; ".join(failed_expectations)
        )


# ── DAG definition ─────────────────────────────────────────────────────────────
@dag(
    dag_id="clickstream_pipeline",
    description="E-commerce clickstream ETL with AI enrichment",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["clickstream", "etl", "ai"],
)
def clickstream_pipeline():
    # ── Task 1: Extract ────────────────────────────────────────────────────────
    @task()
    def extract() -> dict:
        """Download new event batches and dimension files from MinIO."""
        import boto3
        from botocore.config import Config as BotoConfig

        minio_endpoint, minio_access_key, minio_secret_key, minio_bucket = _get_minio_config()

        client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=BotoConfig(connect_timeout=10, read_timeout=30),
        )

        # List all event batch keys under raw/events/ prefix
        response = client.list_objects_v2(Bucket=minio_bucket, Prefix="raw/events/")
        all_batch_keys = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".csv")
        ]

        # Query processed_batches to find already-processed keys
        engine = create_engine(
            _get_pg_conn(), pool_pre_ping=True, connect_args={"connect_timeout": 10}
        )
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS processed_batches "
                    "(batch_key TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT NOW())"
                )
            )
        with engine.connect() as conn:
            result = conn.execute(text("SELECT batch_key FROM processed_batches"))
            processed_keys = {row[0] for row in result}

        new_batch_keys = [k for k in all_batch_keys if k not in processed_keys]

        if not new_batch_keys:
            raise AirflowSkipException(
                "No new event batches found in MinIO — skipping downstream tasks."
            )

        # Download new batch files + dimension tables
        tmp_dir = tempfile.mkdtemp(prefix="clickstream_")
        event_files: list[str] = []

        try:
            for key in new_batch_keys:
                safe_name = key.replace("/", "_")
                local_path = os.path.join(tmp_dir, safe_name)
                client.download_file(minio_bucket, key, local_path)
                event_files.append(local_path)

            # Download dimension tables
            users_path = os.path.join(tmp_dir, "raw_users.csv")
            products_path = os.path.join(tmp_dir, "raw_products.csv")
            client.download_file(minio_bucket, "raw/users.csv", users_path)
            client.download_file(minio_bucket, "raw/products.csv", products_path)
        except Exception:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

        logger.info(
            "[extract] Found %s new batches (of %s total). Downloaded to %s",
            len(new_batch_keys),
            len(all_batch_keys),
            tmp_dir,
        )
        return {
            "event_files": event_files,
            "batch_keys": new_batch_keys,
            "users": users_path,
            "products": products_path,
            "_tmp_dir": tmp_dir,
            "_minio_bucket": minio_bucket,
        }

    # ── Task 2: Validate raw data ──────────────────────────────────────────────
    @task()
    def validate_raw(extract_data: dict) -> dict:
        """Run Great Expectations validation on new event batches and dimension tables."""
        # Validate each event batch file
        events_suite = os.path.join(GE_SUITES_DIR, "raw_events_suite.json")
        for event_file in extract_data["event_files"]:
            events_df = pl.read_csv(event_file)
            run_ge_suite(
                events_df, events_suite,
                task_name=f"validate_raw/events/{os.path.basename(event_file)}",
            )

        users_df = pl.read_csv(extract_data["users"])
        users_suite = os.path.join(GE_SUITES_DIR, "raw_users_suite.json")
        run_ge_suite(users_df, users_suite, task_name="validate_raw/users")

        products_df = pl.read_csv(extract_data["products"])
        products_suite = os.path.join(GE_SUITES_DIR, "raw_products_suite.json")
        run_ge_suite(products_df, products_suite, task_name="validate_raw/products")

        logger.info(
            "[validate_raw] All expectations passed — %s event batch(es), users, products",
            len(extract_data["event_files"]),
        )
        return extract_data

    # ── Task 3: Transform ──────────────────────────────────────────────────────
    @task()
    def transform(extract_data: dict) -> dict:
        """
        Polars transformations:
        1. Concatenate new event batch CSVs
        2. Sessionise new events
        3. Read ALL events from MinIO to recompute funnel metrics from scratch
        """
        import shutil

        import boto3
        from botocore.config import Config as BotoConfig

        # Concatenate new event batch files
        new_event_dfs = []
        for event_file in extract_data["event_files"]:
            df = pl.read_csv(event_file)
            new_event_dfs.append(df)

        new_events = pl.concat(new_event_dfs).with_columns(
            pl.col("timestamp").str.to_datetime(time_zone="UTC", strict=True)
        )
        products = pl.read_csv(extract_data["products"])

        if len(new_events) == 0:
            raise ValueError("[transform] No events in new batches — aborting")
        if len(products) == 0:
            raise ValueError("[transform] products.csv is empty — aborting to prevent data loss")

        # Clean up temp directory from extract
        tmp_dir = extract_data.get("_tmp_dir")
        if tmp_dir and os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.info("[transform] Cleaned up temp dir: %s", tmp_dir)

        # ── Sessionise new events ──────────────────────────────────────────────
        new_events = (
            new_events.sort(["user_id", "timestamp"])
            .with_columns(
                pl.col("timestamp")
                .diff()
                .over("user_id")
                .dt.total_seconds()
                .alias("time_since_prev")
            )
            .with_columns(
                (
                    pl.col("time_since_prev").is_null()
                    | (pl.col("time_since_prev") > SESSION_GAP_SECONDS)
                )
                .cast(pl.Int32)
                .cum_sum()
                .over("user_id")
                .alias("session_seq")
            )
            .with_columns(
                (pl.col("user_id") + "_" + pl.col("session_seq").cast(pl.Utf8)).alias(
                    "computed_session_id"
                )
            )
        )

        # ── Read ALL events from MinIO for full funnel recalculation ───────────
        minio_endpoint, minio_access_key, minio_secret_key, minio_bucket = _get_minio_config()
        client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=BotoConfig(connect_timeout=10, read_timeout=30),
        )

        response = client.list_objects_v2(Bucket=minio_bucket, Prefix="raw/events/")
        all_event_dfs = []
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                obj_data = client.get_object(Bucket=minio_bucket, Key=obj["Key"])
                df = pl.read_csv(obj_data["Body"].read())
                all_event_dfs.append(df)

        if not all_event_dfs:
            raise ValueError("[transform] No event batches found in MinIO at all")

        all_events = pl.concat(all_event_dfs).with_columns(
            pl.col("timestamp").str.to_datetime(time_zone="UTC", strict=True)
        )

        # ── Funnel metrics from ALL events per product ─────────────────────────
        funnel = (
            all_events.group_by("product_id")
            .agg(
                (pl.col("event_type") == "view").sum().alias("view_count"),
                (pl.col("event_type") == "add_to_cart").sum().alias("cart_count"),
                (pl.col("event_type") == "purchase").sum().alias("purchase_count"),
            )
            .with_columns(
                pl.when(pl.col("view_count") > 0)
                .then(pl.col("cart_count") / pl.col("view_count"))
                .otherwise(0.0)
                .alias("view_to_cart_rate"),
                pl.when(pl.col("cart_count") > 0)
                .then(pl.col("purchase_count") / pl.col("cart_count"))
                .otherwise(0.0)
                .alias("cart_to_purchase_rate"),
            )
            .join(
                products.select(["product_id", "name", "description"]),
                on="product_id",
                how="left",
            )
        )

        # Serialize to JSON for XCom
        funnel_records = funnel.to_dicts()
        products_records = products.to_dicts()
        new_events_records = new_events.select(
            ["event_id", "user_id", "product_id", "event_type", "timestamp", "session_id"]
        ).with_columns(pl.col("timestamp").cast(pl.Utf8)).to_dicts()

        import json as _json

        payload_size = (
            len(_json.dumps(funnel_records))
            + len(_json.dumps(products_records))
            + len(_json.dumps(new_events_records))
        )
        if payload_size > 40_000:
            logger.warning(
                "[transform] XCom payload is ~%s bytes — may exceed Airflow metadata DB limits.",
                payload_size,
            )

        logger.info(
            "[transform] Funnel metrics from %s total events across all batches for %s products",
            len(all_events),
            len(funnel),
        )
        return {
            "funnel": funnel_records,
            "products": products_records,
            "new_events": new_events_records,
            "batch_keys": extract_data["batch_keys"],
        }

    # ── Task 4: AI Enrichment ──────────────────────────────────────────────────
    @task()
    def enrich_ai(data: dict) -> dict:
        """
        Use LangChain Structured Output (init_chat_model.with_structured_output) to
        categorise unique products by name + description.
        Model: groq:llama-3.3-70b-versatile.
        """
        from langchain.chat_models import init_chat_model

        groq_api_key = os.getenv("GROQ_API_KEY", "")
        products: list[dict] = data["products"]
        config = build_enrichment_config_from_env()
        llm = None
        if groq_api_key:
            llm = init_chat_model(
                "groq:llama-3.3-70b-versatile",
                temperature=0,
                api_key=groq_api_key,
                max_retries=0,
                streaming=False,
            ).with_structured_output(ProductCategory)
        else:
            logger.warning(
                "[enrich_ai] GROQ_API_KEY is not set — AI enrichment disabled, "
                "using fallback category '%s'",
                config.fallback_category,
            )

        funnel_records, enriched_products, stats = enrich_products_with_llm(
            products=products,
            funnel_records=data["funnel"],
            llm=llm,
            config=config,
            log=logger,
        )
        logger.info(
            "[enrich_ai] Enriched %s unique product prompts; fallbacks=%s",
            stats["unique_prompts"],
            stats["fallbacks"],
        )
        return {
            "funnel": funnel_records,
            "products": enriched_products,
            "new_events": data.get("new_events", []),
            "batch_keys": data.get("batch_keys", []),
        }

    # ── Task 5: Validate enriched data ────────────────────────────────────────
    @task()
    def validate_enriched(data: dict) -> dict:
        """Run Great Expectations validation on the enriched funnel metrics."""
        funnel_df = pl.DataFrame(data["funnel"])
        suite_path = os.path.join(GE_SUITES_DIR, "enriched_funnel_suite.json")

        run_ge_suite(funnel_df, suite_path, task_name="validate_enriched")

        logger.info("[validate_enriched] All expectations passed (%s rows)", len(funnel_df))
        return data

    # ── Task 6: Load to PostgreSQL ─────────────────────────────────────────────
    @task()
    def load(data: dict) -> None:
        """Upsert enriched funnel metrics, product dimension, and new events into PostgreSQL."""
        engine = create_engine(
            _get_pg_conn(),
            pool_pre_ping=True,
            pool_timeout=10,
            connect_args={"connect_timeout": 10},
        )

        funnel_df = pl.DataFrame(data["funnel"])
        products_df = pl.DataFrame(data["products"])
        new_events_df = pl.DataFrame(data.get("new_events", []))
        batch_keys: list[str] = data.get("batch_keys", [])

        if len(funnel_df) == 0:
            raise ValueError(
                "[load] Refusing to load empty funnel data — would destroy existing data"
            )
        if len(products_df) == 0:
            raise ValueError(
                "[load] Refusing to load empty products data — would destroy existing data"
            )

        with engine.begin() as conn:
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id   TEXT PRIMARY KEY,
                    name         TEXT,
                    description  TEXT,
                    price        DOUBLE PRECISION,
                    stock        INTEGER,
                    category     TEXT
                )
                """)
            )
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS fact_funnel_metrics (
                    product_id             TEXT PRIMARY KEY,
                    name                   TEXT,
                    category               TEXT,
                    view_count             INTEGER,
                    cart_count             INTEGER,
                    purchase_count         INTEGER,
                    view_to_cart_rate      DOUBLE PRECISION,
                    cart_to_purchase_rate  DOUBLE PRECISION,
                    loaded_at              TIMESTAMPTZ DEFAULT NOW()
                )
                """)
            )
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS fact_events (
                    event_id    TEXT PRIMARY KEY,
                    user_id     TEXT,
                    product_id  TEXT,
                    event_type  TEXT,
                    timestamp   TEXT,
                    session_id  TEXT,
                    loaded_at   TIMESTAMPTZ DEFAULT NOW()
                )
                """)
            )
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS processed_batches "
                    "(batch_key TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT NOW())"
                )
            )

            # Dimension tables: full refresh
            conn.execute(text("TRUNCATE TABLE dim_products"))
            # Funnel metrics: full refresh (recalculated from all data)
            conn.execute(text("TRUNCATE TABLE fact_funnel_metrics"))

            products_df.to_pandas().to_sql("dim_products", conn, if_exists="append", index=False)
            funnel_df.select(
                [
                    "product_id",
                    "name",
                    "category",
                    "view_count",
                    "cart_count",
                    "purchase_count",
                    "view_to_cart_rate",
                    "cart_to_purchase_rate",
                ]
            ).to_pandas().to_sql("fact_funnel_metrics", conn, if_exists="append", index=False)

            # Append new events (ON CONFLICT DO NOTHING for idempotency)
            if len(new_events_df) > 0:
                for row in new_events_df.to_dicts():
                    conn.execute(
                        text(
                            "INSERT INTO fact_events "
                            "(event_id, user_id, product_id, event_type, "
                            "timestamp, session_id) "
                            "VALUES (:event_id, :user_id, :product_id, "
                            ":event_type, :timestamp, :session_id) "
                            "ON CONFLICT (event_id) DO NOTHING"
                        ),
                        row,
                    )

            # Record processed batch keys
            for key in batch_keys:
                conn.execute(
                    text(
                        "INSERT INTO processed_batches (batch_key) "
                        "VALUES (:key) ON CONFLICT DO NOTHING"
                    ),
                    {"key": key},
                )

        with engine.connect() as conn:
            row_count = conn.execute(text("SELECT COUNT(*) FROM fact_funnel_metrics")).scalar()
            events_count = conn.execute(text("SELECT COUNT(*) FROM fact_events")).scalar()

        logger.info(
            "[load] Loaded %s rows into fact_funnel_metrics, %s total events in fact_events, "
            "%s batch(es) recorded",
            row_count,
            events_count,
            len(batch_keys),
        )

    # ── Wire tasks ──────────────────────────────────────────────────────────────
    extract_data = extract()
    validated_data = validate_raw(extract_data)
    transformed = transform(validated_data)
    enriched = enrich_ai(transformed)
    validated_enriched = validate_enriched(enriched)
    load(validated_enriched)


clickstream_pipeline()
