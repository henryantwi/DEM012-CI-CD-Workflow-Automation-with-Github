"""
Tests for production-hardening changes to the clickstream pipeline.

Covers:
- Deferred environment variable access (T10)
- GE suite hardening: row count minimums and uniqueness checks (T2, T7)
- GE suite validation for users and products (T6)
- run_ge_suite rejects empty DataFrames
- run_ge_suite rejects duplicate event_ids / product_ids

Run with: uv run pytest tests/test_pipeline_hardening.py -v
"""

from __future__ import annotations

import json
import os

import pandas as pd
import pytest

# ── Helpers ────────────────────────────────────────────────────────────────────

GE_SUITES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "great_expectations",
    "expectations",
)


def _load_suite(name: str) -> dict:
    with open(os.path.join(GE_SUITES_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def _run_ge_suite_standalone(df: pd.DataFrame, suite_path: str, task_name: str) -> None:
    """Standalone version of run_ge_suite using pandas directly (no pyarrow needed)."""
    import great_expectations as gx

    with open(suite_path, encoding="utf-8") as f:
        suite_def = json.load(f)

    dataset = gx.from_pandas(df)
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


# ── T10: Deferred env var access ──────────────────────────────────────────────

class TestDeferredEnvVars:
    """Verify that the DAG module can be parsed without env vars set."""

    def test_dag_module_parses_without_env_vars(self):
        """The DAG file should parse (AST level) without any env vars."""
        import ast

        dag_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "dags", "clickstream_pipeline.py"
        )
        with open(dag_path) as f:
            source = f.read()
        # Should not raise — env vars are deferred to task execution
        ast.parse(source)

    def test_no_bare_os_environ_at_module_level(self):
        """Verify no os.environ[] calls at module level (only inside functions)."""
        import ast

        dag_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "dags", "clickstream_pipeline.py"
        )
        with open(dag_path) as f:
            source = f.read()
        tree = ast.parse(source)

        # Walk top-level statements only (not inside functions/classes)
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            # Check for os.environ subscripts at module level
            for child in ast.walk(node):
                if isinstance(child, ast.Subscript):
                    if isinstance(child.value, ast.Attribute):
                        attr = child.value
                        if (
                            isinstance(attr.value, ast.Name)
                            and attr.value.id == "os"
                            and attr.attr == "environ"
                        ):
                            pytest.fail(
                                f"Found os.environ[] at module level (line {child.lineno}). "
                                "This crashes the DAG parser if env vars are missing."
                            )


# ── T2 + T7: GE suite hardening ──────────────────────────────────────────────

class TestGESuiteHardening:
    """Verify that GE suites reject empty DataFrames and duplicate keys."""

    def test_raw_events_suite_rejects_empty_dataframe(self):
        empty_events = pd.DataFrame(
            {
                "event_id": pd.Series([], dtype=str),
                "user_id": pd.Series([], dtype=str),
                "product_id": pd.Series([], dtype=str),
                "event_type": pd.Series([], dtype=str),
                "timestamp": pd.Series([], dtype=str),
                "session_id": pd.Series([], dtype=str),
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_events_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(empty_events, suite_path, "test_empty_events")

    def test_raw_events_suite_rejects_duplicate_event_ids(self):
        events_with_dupes = pd.DataFrame(
            {
                "event_id": ["e1", "e1", "e2"],
                "user_id": ["u1", "u1", "u2"],
                "product_id": ["p1", "p1", "p2"],
                "event_type": ["view", "view", "view"],
                "timestamp": ["2024-01-01T10:00:00", "2024-01-01T10:01:00", "2024-01-01T10:02:00"],
                "session_id": ["s1", "s1", "s2"],
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_events_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(events_with_dupes, suite_path, "test_dupe_events")

    def test_raw_events_suite_passes_valid_data(self):
        valid_events = pd.DataFrame(
            {
                "event_id": ["e1", "e2", "e3"],
                "user_id": ["u1", "u1", "u2"],
                "product_id": ["p1", "p1", "p2"],
                "event_type": ["view", "add_to_cart", "purchase"],
                "timestamp": ["2024-01-01T10:00:00", "2024-01-01T10:01:00", "2024-01-01T10:02:00"],
                "session_id": ["s1", "s1", "s2"],
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_events_suite.json")
        _run_ge_suite_standalone(valid_events, suite_path, "test_valid_events")

    def test_enriched_funnel_suite_rejects_empty_dataframe(self):
        empty_funnel = pd.DataFrame(
            {
                "product_id": pd.Series([], dtype=str),
                "category": pd.Series([], dtype=str),
                "view_count": pd.Series([], dtype=int),
                "cart_count": pd.Series([], dtype=int),
                "purchase_count": pd.Series([], dtype=int),
                "view_to_cart_rate": pd.Series([], dtype=float),
                "cart_to_purchase_rate": pd.Series([], dtype=float),
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "enriched_funnel_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(empty_funnel, suite_path, "test_empty_funnel")

    def test_enriched_funnel_suite_rejects_duplicate_product_ids(self):
        funnel_with_dupes = pd.DataFrame(
            {
                "product_id": ["p1", "p1"],
                "category": ["Electronics", "Electronics"],
                "view_count": [10, 5],
                "cart_count": [5, 2],
                "purchase_count": [2, 1],
                "view_to_cart_rate": [0.5, 0.4],
                "cart_to_purchase_rate": [0.4, 0.5],
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "enriched_funnel_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(funnel_with_dupes, suite_path, "test_dupe_funnel")


# ── T6: Users and products GE suites ─────────────────────────────────────────

class TestNewGESuites:
    """Verify the new users and products validation suites."""

    def test_raw_users_suite_rejects_empty(self):
        empty_users = pd.DataFrame(
            {
                "user_id": pd.Series([], dtype=str),
                "name": pd.Series([], dtype=str),
                "email": pd.Series([], dtype=str),
                "country": pd.Series([], dtype=str),
                "signup_date": pd.Series([], dtype=str),
                "age": pd.Series([], dtype=int),
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_users_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(empty_users, suite_path, "test_empty_users")

    def test_raw_users_suite_passes_valid_data(self):
        valid_users = pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "name": ["Alice", "Bob"],
                "email": ["a@b.com", "b@c.com"],
                "country": ["US", "UK"],
                "signup_date": ["2024-01-01", "2024-02-01"],
                "age": [25, 30],
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_users_suite.json")
        _run_ge_suite_standalone(valid_users, suite_path, "test_valid_users")

    def test_raw_users_suite_rejects_duplicate_user_ids(self):
        dupe_users = pd.DataFrame(
            {
                "user_id": ["u1", "u1"],
                "name": ["Alice", "Alice Copy"],
                "email": ["a@b.com", "a2@b.com"],
                "country": ["US", "US"],
                "signup_date": ["2024-01-01", "2024-02-01"],
                "age": [25, 26],
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_users_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(dupe_users, suite_path, "test_dupe_users")

    def test_raw_products_suite_rejects_empty(self):
        empty_products = pd.DataFrame(
            {
                "product_id": pd.Series([], dtype=str),
                "name": pd.Series([], dtype=str),
                "description": pd.Series([], dtype=str),
                "price": pd.Series([], dtype=float),
                "stock": pd.Series([], dtype=int),
                "category": pd.Series([], dtype=str),
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_products_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(empty_products, suite_path, "test_empty_products")

    def test_raw_products_suite_passes_valid_data(self):
        valid_products = pd.DataFrame(
            {
                "product_id": ["p1", "p2"],
                "name": ["Widget", "Gadget"],
                "description": ["A widget", "A gadget"],
                "price": [9.99, 19.99],
                "stock": [100, 50],
                "category": ["", ""],
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_products_suite.json")
        _run_ge_suite_standalone(valid_products, suite_path, "test_valid_products")

    def test_raw_products_suite_rejects_duplicate_product_ids(self):
        dupe_products = pd.DataFrame(
            {
                "product_id": ["p1", "p1"],
                "name": ["Widget", "Widget Copy"],
                "description": ["A widget", "Another widget"],
                "price": [9.99, 9.99],
                "stock": [100, 100],
                "category": ["", ""],
            }
        )
        suite_path = os.path.join(GE_SUITES_DIR, "raw_products_suite.json")
        with pytest.raises(ValueError, match="expectation.*failed"):
            _run_ge_suite_standalone(dupe_products, suite_path, "test_dupe_products")


# ── T3: DAG concurrency limit ────────────────────────────────────────────────

class TestDAGConfig:
    """Verify DAG-level config for concurrency safety."""

    def test_dag_has_max_active_runs_one(self):
        """Parse the DAG file and verify max_active_runs=1 is set."""
        import ast

        dag_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "dags", "clickstream_pipeline.py"
        )
        with open(dag_path) as f:
            source = f.read()

        tree = ast.parse(source)
        found = False
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "max_active_runs":
                if isinstance(node.value, ast.Constant) and node.value.value == 1:
                    found = True
        assert found, "max_active_runs=1 not found in @dag() decorator"


# ── GE suite JSON structure tests ─────────────────────────────────────────────

class TestGESuiteStructure:
    """Verify that GE suite JSON files have required expectation types."""

    def test_raw_events_has_row_count_check(self):
        suite = _load_suite("raw_events_suite.json")
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_table_row_count_to_be_between" in types

    def test_raw_events_has_uniqueness_check(self):
        suite = _load_suite("raw_events_suite.json")
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_column_values_to_be_unique" in types

    def test_enriched_funnel_has_row_count_check(self):
        suite = _load_suite("enriched_funnel_suite.json")
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_table_row_count_to_be_between" in types

    def test_enriched_funnel_has_uniqueness_check(self):
        suite = _load_suite("enriched_funnel_suite.json")
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_column_values_to_be_unique" in types

    def test_raw_users_suite_exists_and_valid(self):
        suite = _load_suite("raw_users_suite.json")
        assert suite["expectation_suite_name"] == "raw_users_suite"
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_table_row_count_to_be_between" in types
        assert "expect_column_values_to_be_unique" in types

    def test_raw_products_suite_exists_and_valid(self):
        suite = _load_suite("raw_products_suite.json")
        assert suite["expectation_suite_name"] == "raw_products_suite"
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_table_row_count_to_be_between" in types
        assert "expect_column_values_to_be_unique" in types
