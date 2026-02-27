"""
End-to-End Data Flow Validation Script
=======================================
Validates that data has successfully moved through all pipeline components:

  MinIO (raw CSV files exist)
    → PostgreSQL (fact_funnel_metrics has rows)
      → Metabase (/api/health returns {"status": "ok"})

Used in the CD job of the GitHub Actions workflow.

Usage:
    uv run python scripts/validate_data_flow.py

Exit codes:
    0  — all checks passed
    1  — one or more checks failed
"""

from __future__ import annotations

import logging
import os
import sys
import time

# ── Config (from environment) ──────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "clickstream-data")

PG_CONN = (
    "postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'airflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'airflow')}"
)

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")

EXPECTED_KEYS = ["raw/users.csv", "raw/products.csv", "raw/events.csv"]
logger = logging.getLogger(__name__)


# ── Check functions ────────────────────────────────────────────────────────────


def check_minio() -> bool:
    """Verify that all three raw CSV files exist in the MinIO bucket."""
    import boto3
    from botocore.exceptions import ClientError

    logger.info("[1/3] Checking MinIO...")
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        for key in EXPECTED_KEYS:
            client.head_object(Bucket=MINIO_BUCKET, Key=key)
            logger.info("  [OK]  s3://%s/%s exists", MINIO_BUCKET, key)
        return True
    except ClientError as exc:
        logger.error("  [FAIL]  MinIO check failed: %s", exc)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.error("  [FAIL]  MinIO connection error: %s", exc)
        return False


def check_postgres() -> bool:
    """Verify that fact_funnel_metrics contains rows."""
    from sqlalchemy import create_engine, text

    logger.info("[2/3] Checking PostgreSQL...")
    try:
        engine = create_engine(PG_CONN)
        with engine.connect() as conn:
            row_count = conn.execute(text("SELECT COUNT(*) FROM fact_funnel_metrics")).scalar()
        if row_count and row_count > 0:
            logger.info("  [OK]  fact_funnel_metrics has %s rows", row_count)
            return True
        else:
            logger.error("  [FAIL]  fact_funnel_metrics is empty")
            return False
    except Exception as exc:  # noqa: BLE001
        logger.error("  [FAIL]  PostgreSQL check failed: %s", exc)
        return False


def check_metabase(max_retries: int = 5, delay: int = 10) -> bool:
    """Verify that the Metabase health endpoint returns {'status': 'ok'}."""
    import requests

    logger.info("[3/3] Checking Metabase...")
    url = f"{METABASE_URL}/api/health"

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                logger.info("  [OK]  Metabase healthy at %s", url)
                return True
            else:
                logger.warning(
                    "  [WARN]  Attempt %s: status=%s body=%s",
                    attempt,
                    resp.status_code,
                    resp.text[:80],
                )
        except requests.RequestException as exc:
            logger.warning("  [WARN]  Attempt %s: connection error - %s", attempt, exc)

        if attempt < max_retries:
            logger.info("     Retrying in %ss...", delay)
            time.sleep(delay)

    logger.error("  [FAIL]  Metabase not healthy after %s attempts", max_retries)
    return False


# ── Main ───────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("%s", "=" * 55)
    logger.info("  Clickstream Platform - End-to-End Validation")
    logger.info("%s", "=" * 55)

    results = {
        "MinIO": check_minio(),
        "PostgreSQL": check_postgres(),
        "Metabase": check_metabase(),
    }

    logger.info("%s", "=" * 55)
    logger.info("  Results Summary")
    logger.info("%s", "=" * 55)
    all_passed = True
    for name, passed in results.items():
        status = "PASS [OK]" if passed else "FAIL [X]"
        logger.info("  %-20s %s", name, status)
        if not passed:
            all_passed = False

    logger.info("%s", "=" * 55)
    if all_passed:
        logger.info("  All data flow checks passed!")
        sys.exit(0)
    else:
        logger.error("  One or more checks FAILED - see output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
