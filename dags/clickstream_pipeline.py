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
}

# ── Environment ────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

PG_CONN = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
    f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

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
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence score between 0 and 1"
    )


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
    tags=["clickstream", "etl", "ai"],
)
def clickstream_pipeline():
    # ── Task 1: Extract ────────────────────────────────────────────────────────
    @task()
    def extract() -> dict[str, str]:
        """Download raw CSVs from MinIO to a temporary directory."""
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        tmp_dir = tempfile.mkdtemp(prefix="clickstream_")
        paths: dict[str, str] = {}

        for key in ("raw/users.csv", "raw/products.csv", "raw/events.csv"):
            local_path = os.path.join(tmp_dir, key.replace("/", "_"))
            client.download_file(MINIO_BUCKET, key, local_path)
            name = key.split("/")[-1].replace(".csv", "")
            paths[name] = local_path

        logger.info("[extract] Downloaded files to %s: %s", tmp_dir, list(paths.keys()))
        return paths

    # ── Task 2: Validate raw data ──────────────────────────────────────────────
    @task()
    def validate_raw(paths: dict[str, str]) -> dict[str, str]:
        """Run Great Expectations validation on the raw events CSV."""
        events_df = pl.read_csv(paths["events"])
        suite_path = os.path.join(GE_SUITES_DIR, "raw_events_suite.json")

        run_ge_suite(events_df, suite_path, task_name="validate_raw")

        logger.info("[validate_raw] All expectations passed for events (%s rows)", len(events_df))
        return paths

    # ── Task 3: Transform ──────────────────────────────────────────────────────
    @task()
    def transform(paths: dict[str, str]) -> dict:
        """
        Polars transformations:
        1. Sessionise events (30-min gap threshold)
        2. Compute per-product funnel metrics (view → cart → purchase rates)
        """
        events = pl.read_csv(paths["events"]).with_columns(
            pl.col("timestamp").str.to_datetime(time_zone="UTC", strict=False)
        )
        products = pl.read_csv(paths["products"])

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

        products: list[dict] = data["products"]
        config = build_enrichment_config_from_env()
        llm = None
        if GROQ_API_KEY:
            llm = init_chat_model(
                "groq:llama-3.3-70b-versatile",
                temperature=0,
                api_key=GROQ_API_KEY,
                max_retries=0,
                streaming=False,
            ).with_structured_output(ProductCategory)

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
        engine = create_engine(PG_CONN)

        funnel_df = pl.DataFrame(data["funnel"])
        products_df = pl.DataFrame(data["products"])

        with engine.begin() as conn:
            # Create tables if not exists
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

        # Write via Polars + SQLAlchemy (upsert via truncate-and-replace for simplicity)
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE dim_products"))
            conn.execute(text("TRUNCATE TABLE fact_funnel_metrics"))

        products_df.to_pandas().to_sql(
            "dim_products", engine, if_exists="append", index=False
        )
        funnel_df.select(
            [
                "product_id", "name", "category",
                "view_count", "cart_count", "purchase_count",
                "view_to_cart_rate", "cart_to_purchase_rate",
            ]
        ).to_pandas().to_sql("fact_funnel_metrics", engine, if_exists="append", index=False)

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
