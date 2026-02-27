"""
Unit tests for Polars transformation logic in the clickstream pipeline.
These tests run without Docker — pure in-memory DataFrames.

Run with: uv run pytest
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

# ── Helpers mirroring DAG transform logic ─────────────────────────────────────
SESSION_GAP_SECONDS = 30 * 60  # 30 minutes


def compute_funnel_metrics(events: pl.DataFrame) -> pl.DataFrame:
    """Compute per-product funnel metrics from a clickstream events DataFrame."""
    return (
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
        .sort("product_id")
    )


def sessionise(events: pl.DataFrame) -> pl.DataFrame:
    """Assign session IDs based on a 30-minute inactivity gap per user."""
    return (
        events.sort(["user_id", "timestamp"])
        .with_columns(
            pl.col("timestamp").diff().over("user_id").dt.total_seconds().alias("time_since_prev")
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


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _ts(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


@pytest.fixture
def sample_events() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "event_id": ["e1", "e2", "e3", "e4", "e5", "e6"],
            "user_id": ["u1", "u1", "u1", "u2", "u2", "u2"],
            "product_id": ["p1", "p1", "p1", "p2", "p2", "p2"],
            "event_type": ["view", "add_to_cart", "purchase", "view", "add_to_cart", "view"],
            "timestamp": pl.Series(
                [
                    _ts("2024-01-01T10:00:00"),
                    _ts("2024-01-01T10:05:00"),
                    _ts("2024-01-01T10:10:00"),
                    _ts("2024-01-01T11:00:00"),
                    _ts("2024-01-01T11:05:00"),
                    _ts("2024-01-01T11:50:00"),
                ]
            ),
        }
    )


# ── Funnel metric tests ───────────────────────────────────────────────────────


class TestFunnelMetrics:
    def test_view_count_correct(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        p1 = result.filter(pl.col("product_id") == "p1").row(0, named=True)
        assert p1["view_count"] == 1

    def test_cart_count_correct(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        p1 = result.filter(pl.col("product_id") == "p1").row(0, named=True)
        assert p1["cart_count"] == 1

    def test_purchase_count_correct(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        p1 = result.filter(pl.col("product_id") == "p1").row(0, named=True)
        assert p1["purchase_count"] == 1

    def test_view_to_cart_rate_is_1(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        p1 = result.filter(pl.col("product_id") == "p1").row(0, named=True)
        assert p1["view_to_cart_rate"] == pytest.approx(1.0)

    def test_cart_to_purchase_rate_is_1(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        p1 = result.filter(pl.col("product_id") == "p1").row(0, named=True)
        assert p1["cart_to_purchase_rate"] == pytest.approx(1.0)

    def test_rates_bounded_between_0_and_1(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        assert result["view_to_cart_rate"].min() >= 0.0
        assert result["view_to_cart_rate"].max() <= 1.0
        assert result["cart_to_purchase_rate"].min() >= 0.0
        assert result["cart_to_purchase_rate"].max() <= 1.0

    def test_zero_rate_when_no_views(self):
        events = pl.DataFrame(
            {
                "event_id": ["e1"],
                "user_id": ["u1"],
                "product_id": ["p99"],
                "event_type": ["purchase"],  # purchase with no view — edge case
                "timestamp": pl.Series([_ts("2024-01-01T10:00:00")]),
            }
        )
        result = compute_funnel_metrics(events)
        p99 = result.filter(pl.col("product_id") == "p99").row(0, named=True)
        assert p99["view_to_cart_rate"] == pytest.approx(0.0)

    def test_two_products_independent(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        assert len(result) == 2  # p1 and p2

    def test_p2_two_views_one_cart(self, sample_events):
        result = compute_funnel_metrics(sample_events)
        p2 = result.filter(pl.col("product_id") == "p2").row(0, named=True)
        assert p2["view_count"] == 2
        assert p2["cart_count"] == 1
        assert p2["view_to_cart_rate"] == pytest.approx(0.5)


# ── Sessionisation tests ──────────────────────────────────────────────────────


class TestSessionisation:
    def test_session_column_created(self, sample_events):
        result = sessionise(sample_events)
        assert "computed_session_id" in result.columns

    def test_same_session_within_gap(self, sample_events):
        """Events < 30 min apart for same user should share a session."""
        result = sessionise(sample_events)
        u1_sessions = (
            result.filter(pl.col("user_id") == "u1").select("computed_session_id").unique()
        )
        # u1's three events are all within 10 minutes → one session
        assert len(u1_sessions) == 1

    def test_new_session_after_gap(self, sample_events):
        """u2 last event is 45 min after the previous one → new session."""
        result = sessionise(sample_events)
        u2_sessions = (
            result.filter(pl.col("user_id") == "u2").select("computed_session_id").unique()
        )
        assert len(u2_sessions) == 2

    def test_session_id_contains_user_id(self, sample_events):
        result = sessionise(sample_events)
        for row in result.to_dicts():
            assert row["user_id"] in row["computed_session_id"]


# ── Data generator shape tests ────────────────────────────────────────────────


class TestDataGeneratorShapes:
    def test_generate_users_shape(self):
        import sys

        sys.path.insert(0, ".")
        from data_generator.generate_data import generate_users

        df = generate_users(n=10)
        assert len(df) == 10
        assert "user_id" in df.columns
        assert "email" in df.columns

    def test_generate_products_shape(self):
        from data_generator.generate_data import generate_products

        df = generate_products(n=5)
        assert len(df) == 5
        assert "product_id" in df.columns
        assert "category" in df.columns

    def test_generate_events_has_valid_event_types(self):
        from data_generator.generate_data import generate_events

        users = ["u_001", "u_002"]
        products = ["p_001", "p_002"]
        df = generate_events(users, products, n=50)
        assert set(df["event_type"].unique().to_list()).issubset(
            {"view", "add_to_cart", "purchase"}
        )


# ── Multi-batch concatenation tests ──────────────────────────────────────────


class TestMultiBatchFunnel:
    """Test funnel metrics computed from multiple concatenated batch DataFrames."""

    def test_funnel_from_two_batches(self):
        batch1 = pl.DataFrame(
            {
                "event_id": ["e1", "e2"],
                "user_id": ["u1", "u1"],
                "product_id": ["p1", "p1"],
                "event_type": ["view", "add_to_cart"],
                "timestamp": pl.Series([_ts("2024-01-01T10:00:00"), _ts("2024-01-01T10:05:00")]),
            }
        )
        batch2 = pl.DataFrame(
            {
                "event_id": ["e3", "e4"],
                "user_id": ["u1", "u2"],
                "product_id": ["p1", "p2"],
                "event_type": ["purchase", "view"],
                "timestamp": pl.Series([_ts("2024-01-02T10:00:00"), _ts("2024-01-02T11:00:00")]),
            }
        )
        combined = pl.concat([batch1, batch2])
        result = compute_funnel_metrics(combined)
        p1 = result.filter(pl.col("product_id") == "p1").row(0, named=True)
        assert p1["view_count"] == 1
        assert p1["cart_count"] == 1
        assert p1["purchase_count"] == 1

    def test_cross_batch_sessions(self):
        """Events from different batches for same user should be sessionised correctly."""
        batch1 = pl.DataFrame(
            {
                "event_id": ["e1"],
                "user_id": ["u1"],
                "product_id": ["p1"],
                "event_type": ["view"],
                "timestamp": pl.Series([_ts("2024-01-01T10:00:00")]),
            }
        )
        batch2 = pl.DataFrame(
            {
                "event_id": ["e2"],
                "user_id": ["u1"],
                "product_id": ["p1"],
                "event_type": ["add_to_cart"],
                # 2 hours later => new session
                "timestamp": pl.Series([_ts("2024-01-01T12:00:00")]),
            }
        )
        combined = pl.concat([batch1, batch2])
        result = sessionise(combined)
        u1_sessions = (
            result.filter(pl.col("user_id") == "u1")
            .select("computed_session_id").unique()
        )
        assert len(u1_sessions) == 2  # 2-hour gap > 30-min threshold

    def test_cross_batch_same_session(self):
        """Events from different batches within 30-min window should share session."""
        batch1 = pl.DataFrame(
            {
                "event_id": ["e1"],
                "user_id": ["u1"],
                "product_id": ["p1"],
                "event_type": ["view"],
                "timestamp": pl.Series([_ts("2024-01-01T10:00:00")]),
            }
        )
        batch2 = pl.DataFrame(
            {
                "event_id": ["e2"],
                "user_id": ["u1"],
                "product_id": ["p1"],
                "event_type": ["add_to_cart"],
                # 10 minutes later => same session
                "timestamp": pl.Series([_ts("2024-01-01T10:10:00")]),
            }
        )
        combined = pl.concat([batch1, batch2])
        result = sessionise(combined)
        u1_sessions = (
            result.filter(pl.col("user_id") == "u1")
            .select("computed_session_id").unique()
        )
        assert len(u1_sessions) == 1
