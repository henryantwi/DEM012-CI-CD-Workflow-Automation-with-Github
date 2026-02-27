"""
Data Generator — E-commerce Clickstream Platform
=================================================
Generates three interrelated CSV datasets and uploads them to MinIO:
  - users.csv      : synthetic user profiles
  - products.csv   : synthetic product catalogue (used for AI enrichment later)
  - events.csv     : clickstream events (view / add_to_cart / purchase)

Usage:
    uv run python data_generator/generate_data.py

Environment variables (copy .env.example → .env):
    MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_BUCKET
"""

from __future__ import annotations

import io
import logging
import os
import random
from datetime import datetime, timedelta, timezone

import polars as pl
from faker import Faker

# ── Config ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "clickstream-data")
logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        parsed = int(value)
        if parsed <= 0:
            raise ValueError
        return parsed
    except ValueError:
        logger.warning("Invalid %s=%r; using default %s", name, value, default)
        return default


NUM_USERS = _env_int("NUM_USERS", 200)
NUM_PRODUCTS = _env_int("NUM_PRODUCTS", 50)
NUM_EVENTS = _env_int("NUM_EVENTS", 2000)

PRODUCT_ADJECTIVES = [
    "Wireless",
    "Portable",
    "Smart",
    "Ergonomic",
    "Ultra",
    "Pro",
    "Compact",
    "Deluxe",
    "Premium",
    "Eco-Friendly",
]
PRODUCT_NOUNS = [
    "Headphones",
    "Keyboard",
    "Mouse",
    "Monitor",
    "Desk Lamp",
    "Chair",
    "Backpack",
    "Notebook",
    "Water Bottle",
    "Running Shoes",
    "T-Shirt",
    "Blender",
    "Coffee Maker",
    "Yoga Mat",
    "Sunglasses",
    "Watch",
]

fake = Faker()
random.seed(42)
Faker.seed(42)


# ── Generators ─────────────────────────────────────────────────────────────────


def generate_users(n: int = NUM_USERS) -> pl.DataFrame:
    """Generate synthetic user profiles."""
    return pl.DataFrame(
        {
            "user_id": [f"u_{i:04d}" for i in range(1, n + 1)],
            "name": [fake.name() for _ in range(n)],
            "email": [fake.email() for _ in range(n)],
            "country": [fake.country_code() for _ in range(n)],
            "signup_date": [
                fake.date_between(start_date="-2y", end_date="today").isoformat() for _ in range(n)
            ],
            "age": [random.randint(18, 65) for _ in range(n)],
        }
    )


def generate_products(n: int = NUM_PRODUCTS) -> pl.DataFrame:
    """Generate synthetic product catalogue with raw names for AI categorisation."""
    names = [
        f"{random.choice(PRODUCT_ADJECTIVES)} {random.choice(PRODUCT_NOUNS)}" for _ in range(n)
    ]
    descriptions = [f"{fake.sentence(nb_words=8)} {fake.sentence(nb_words=6)}" for _ in range(n)]
    return pl.DataFrame(
        {
            "product_id": [f"p_{i:03d}" for i in range(1, n + 1)],
            "name": names,
            "description": descriptions,
            "price": [round(random.uniform(5.0, 500.0), 2) for _ in range(n)],
            "stock": [random.randint(0, 500) for _ in range(n)],
            # category intentionally left empty — will be filled by AI enrichment
            "category": ["" for _ in range(n)],
        }
    )


def generate_events(
    user_ids: list[str],
    product_ids: list[str],
    n: int = NUM_EVENTS,
) -> pl.DataFrame:
    """
    Generate clickstream events with realistic funnel behaviour:
    view → add_to_cart (50% of views) → purchase (30% of cart additions).
    """
    base_time = datetime.now(tz=timezone.utc) - timedelta(days=30)
    rows: list[dict] = []

    for _ in range(n):
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        event_time = base_time + timedelta(seconds=random.randint(0, 30 * 24 * 3600))

        # Always record a view
        rows.append(
            {
                "event_id": fake.uuid4(),
                "user_id": user_id,
                "product_id": product_id,
                "event_type": "view",
                "timestamp": event_time.isoformat(),
                "session_id": fake.uuid4(),
            }
        )

        # 50% chance of add_to_cart following a view
        if random.random() < 0.5:
            cart_time = event_time + timedelta(seconds=random.randint(10, 300))
            rows.append(
                {
                    "event_id": fake.uuid4(),
                    "user_id": user_id,
                    "product_id": product_id,
                    "event_type": "add_to_cart",
                    "timestamp": cart_time.isoformat(),
                    "session_id": rows[-1]["session_id"],
                }
            )

            # 30% chance of purchase following add_to_cart
            if random.random() < 0.3:
                purchase_time = cart_time + timedelta(seconds=random.randint(30, 600))
                rows.append(
                    {
                        "event_id": fake.uuid4(),
                        "user_id": user_id,
                        "product_id": product_id,
                        "event_type": "purchase",
                        "timestamp": purchase_time.isoformat(),
                        "session_id": rows[-1]["session_id"],
                    }
                )

    return pl.DataFrame(rows).sort("timestamp")


# ── MinIO helpers ──────────────────────────────────────────────────────────────


def get_minio_client():
    import boto3  # lazy import — not needed in unit tests

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(client, bucket: str) -> None:
    from botocore.exceptions import ClientError  # lazy import

    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
        logger.info("[minio] Created bucket: %s", bucket)


def upload_dataframe(client, df: pl.DataFrame, bucket: str, key: str) -> None:
    buffer = io.BytesIO()
    df.write_csv(buffer)
    buffer.seek(0)
    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info("[minio] Uploaded  s3://%s/%s  (%s rows)", bucket, key, len(df))


# ── Main ───────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("=== Clickstream Data Generator ===")

    users_df = generate_users()
    products_df = generate_products()
    events_df = generate_events(
        user_ids=users_df["user_id"].to_list(),
        product_ids=products_df["product_id"].to_list(),
    )

    logger.info(
        "Generated: %s users, %s products, %s events",
        len(users_df),
        len(products_df),
        len(events_df),
    )

    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    upload_dataframe(client, users_df, MINIO_BUCKET, "raw/users.csv")
    upload_dataframe(client, products_df, MINIO_BUCKET, "raw/products.csv")
    upload_dataframe(client, events_df, MINIO_BUCKET, "raw/events.csv")

    logger.info("=== Upload complete ===")


if __name__ == "__main__":
    main()
