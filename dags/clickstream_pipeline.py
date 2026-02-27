"""
Clickstream Pipeline DAG
========================
Orchestrates the full ETL + AI enrichment flow:

  extract → validate_raw → transform → enrich_ai → validate_enriched → load

Data flow:  MinIO (raw CSV) → Airflow (Polars + LangChain) → PostgreSQL
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
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

try:
    # Airflow adds DAGS_FOLDER to PYTHONPATH, so sibling imports should be plain.
    from enrichment_logic import build_enrichment_config_from_env, enrich_products_with_llm
except ModuleNotFoundError:
    # Fallback for local execution contexts where project root is on PYTHONPATH.
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
    def extract() -> dict[str, str]:
        """Download raw CSVs from MinIO to a temporary directory."""
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

        # Pre-flight: verify all expected files exist before downloading
        expected_keys = ["raw/events.csv", "raw/products.csv", "raw/users.csv"]
        missing = []
        for key in expected_keys:
            try:
                client.head_object(Bucket=minio_bucket, Key=key)
            except client.exceptions.NoSuchKey:
                missing.append(key)
            except Exception:
                missing.append(key)
        if missing:
            raise FileNotFoundError(
                f"MinIO bucket '{minio_bucket}' is missing required files: {missing}. "
                "Run the data generator first: uv run python data_generator/generate_data.py"
            )

        tmp_dir = tempfile.mkdtemp(prefix="clickstream_")
        paths: dict[str, str] = {}

        try:
            for key in expected_keys:
                local_path = os.path.join(tmp_dir, key.replace("/", "_"))
                client.download_file(minio_bucket, key, local_path)
                name = key.split("/")[-1].replace(".csv", "")
                paths[name] = local_path
        except Exception:
            import shutil

            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

        paths["_tmp_dir"] = tmp_dir
        logger.info("[extract] Downloaded files to %s: %s", tmp_dir, list(paths.keys()))
        return paths

    # ── Task 2: Validate raw data ──────────────────────────────────────────────
    @task()
    def validate_raw(paths: dict[str, str]) -> dict[str, str]:
        """Run Great Expectations validation on all raw CSVs."""
        events_df = pl.read_csv(paths["events"])
        suite_path = os.path.join(GE_SUITES_DIR, "raw_events_suite.json")
        run_ge_suite(events_df, suite_path, task_name="validate_raw/events")

        users_df = pl.read_csv(paths["users"])
        users_suite = os.path.join(GE_SUITES_DIR, "raw_users_suite.json")
        run_ge_suite(users_df, users_suite, task_name="validate_raw/users")

        products_df = pl.read_csv(paths["products"])
        products_suite = os.path.join(GE_SUITES_DIR, "raw_products_suite.json")
        run_ge_suite(products_df, products_suite, task_name="validate_raw/products")

        logger.info(
            "[validate_raw] All expectations passed — events=%s, users=%s, products=%s rows",
            len(events_df),
            len(users_df),
            len(products_df),
        )
        return paths

    # ── Task 3: Transform ──────────────────────────────────────────────────────
    @task()
    def transform(paths: dict[str, str]) -> dict:
        """
        Polars transformations:
        1. Sessionise events (30-min gap threshold)
        2. Compute per-product funnel metrics (view → cart → purchase rates)
        """
        import shutil

        events = pl.read_csv(paths["events"]).with_columns(
            pl.col("timestamp").str.to_datetime(time_zone="UTC", strict=True)
        )
        products = pl.read_csv(paths["products"])

        if len(events) == 0:
            raise ValueError("[transform] events.csv is empty — aborting to prevent data loss")
        if len(products) == 0:
            raise ValueError("[transform] products.csv is empty — aborting to prevent data loss")

        # Clean up temp directory from extract (T8)
        tmp_dir = paths.get("_tmp_dir")
        if tmp_dir and os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.info("[transform] Cleaned up temp dir: %s", tmp_dir)

        # ── Sessionisation ─────────────────────────────────────────────────────
        events = (
            events.sort(["user_id", "timestamp"])
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

        # ── Funnel metrics per product ─────────────────────────────────────────
        funnel = (
            events.group_by("product_id")
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

        # Serialize to JSON for XCom (Polars → dict)
        funnel_records = funnel.to_dicts()
        products_records = products.to_dicts()

        # XCom size warning (T9) — use JSON length, not str() which doubles memory
        import json as _json

        payload_size = len(_json.dumps(funnel_records)) + len(_json.dumps(products_records))
        if payload_size > 40_000:
            logger.warning(
                "[transform] XCom payload is ~%s bytes — may exceed Airflow metadata DB limits. "
                "Consider writing to MinIO and passing object keys instead.",
                payload_size,
            )

        logger.info("[transform] Funnel metrics computed for %s products", len(funnel))
        return {
            "funnel": funnel_records,
            "products": products_records,
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
        return {"funnel": funnel_records, "products": enriched_products}

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
        """Upsert enriched funnel metrics and product dimension into PostgreSQL."""
        engine = create_engine(
            _get_pg_conn(),
            pool_pre_ping=True,
            pool_timeout=10,
            connect_args={"connect_timeout": 10},
        )

        funnel_df = pl.DataFrame(data["funnel"])
        products_df = pl.DataFrame(data["products"])

        if len(funnel_df) == 0:
            raise ValueError(
                "[load] Refusing to load empty funnel data — would destroy existing data"
            )
        if len(products_df) == 0:
            raise ValueError(
                "[load] Refusing to load empty products data — would destroy existing data"
            )

        # Atomic: TRUNCATE + INSERT in a single transaction to prevent data loss
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
            conn.execute(text("TRUNCATE TABLE dim_products"))
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

        with engine.connect() as conn:
            row_count = conn.execute(text("SELECT COUNT(*) FROM fact_funnel_metrics")).scalar()

        logger.info("[load] Loaded %s rows into fact_funnel_metrics", row_count)

    # ── Wire tasks ──────────────────────────────────────────────────────────────
    raw_paths = extract()
    validated_paths = validate_raw(raw_paths)
    transformed = transform(validated_paths)
    enriched = enrich_ai(transformed)
    validated_enriched = validate_enriched(enriched)
    load(validated_enriched)


clickstream_pipeline()
