from __future__ import annotations

import sys
import types

import polars as pl

from data_generator import generate_data


def _install_botocore_client_error(monkeypatch, client_error_type):
    botocore_exc_mod = types.ModuleType("botocore.exceptions")
    botocore_exc_mod.ClientError = client_error_type
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exc_mod)


def test_env_int_returns_parsed_value(monkeypatch):
    monkeypatch.setenv("TEST_COUNT", "7")

    assert generate_data._env_int("TEST_COUNT", 3) == 7


def test_env_int_returns_default_when_missing(monkeypatch):
    monkeypatch.delenv("MISSING_COUNT", raising=False)

    assert generate_data._env_int("MISSING_COUNT", 11) == 11


def test_env_int_returns_default_for_invalid_or_non_positive(monkeypatch):
    monkeypatch.setenv("BAD_COUNT", "abc")
    assert generate_data._env_int("BAD_COUNT", 5) == 5

    monkeypatch.setenv("BAD_COUNT", "-1")
    assert generate_data._env_int("BAD_COUNT", 5) == 5


def test_ensure_bucket_noop_when_bucket_exists(monkeypatch):
    class FakeClientError(Exception):
        pass

    class FakeClient:
        def __init__(self):
            self.created = False
            self.head_checked = False

        def head_bucket(self, Bucket):
            self.head_checked = True

        def create_bucket(self, Bucket):
            self.created = True

    _install_botocore_client_error(monkeypatch, FakeClientError)
    client = FakeClient()

    generate_data.ensure_bucket(client, "test-bucket")

    assert client.head_checked is True
    assert client.created is False


def test_ensure_bucket_creates_bucket_on_client_error(monkeypatch):
    class FakeClientError(Exception):
        pass

    class FakeClient:
        def __init__(self):
            self.created_bucket: str | None = None

        def head_bucket(self, Bucket):
            raise FakeClientError("not found")

        def create_bucket(self, Bucket):
            self.created_bucket = Bucket

    _install_botocore_client_error(monkeypatch, FakeClientError)
    client = FakeClient()

    generate_data.ensure_bucket(client, "new-bucket")

    assert client.created_bucket == "new-bucket"


def test_upload_dataframe_puts_expected_payload():
    class FakeClient:
        def __init__(self):
            self.payload: dict | None = None

        def put_object(self, **kwargs):
            self.payload = kwargs

    client = FakeClient()
    df = pl.DataFrame({"product_id": ["p1", "p2"], "value": [1, 2]})

    generate_data.upload_dataframe(client, df, "bucket-a", "raw/items.csv")

    assert client.payload is not None
    assert client.payload["Bucket"] == "bucket-a"
    assert client.payload["Key"] == "raw/items.csv"
    assert isinstance(client.payload["Body"], bytes)
    assert b"product_id" in client.payload["Body"]


def test_generate_events_minimal_shape_and_event_types():
    events = generate_data.generate_events(
        user_ids=["u_001"],
        product_ids=["p_001"],
        n=1,
    )

    assert len(events) >= 1
    assert {"event_id", "user_id", "product_id", "event_type", "timestamp", "session_id"}.issubset(
        set(events.columns)
    )
    assert set(events["event_type"].unique().to_list()).issubset(
        {"view", "add_to_cart", "purchase"}
    )
