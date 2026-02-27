from __future__ import annotations

import sys
import types

import pytest

from scripts import validate_data_flow as vdf


class FakeResponse:
    def __init__(self, status_code: int, payload: dict, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> dict:
        return self._payload


def _install_fake_boto(monkeypatch, client, client_error_type):
    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda *args, **kwargs: client

    botocore_exc_mod = types.ModuleType("botocore.exceptions")
    botocore_exc_mod.ClientError = client_error_type

    monkeypatch.setitem(sys.modules, "boto3", boto3_mod)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exc_mod)


def _install_fake_sqlalchemy(monkeypatch, row_counts: dict[str, int] | int):
    """Accept either a single int (all tables return same count) or a dict of table->count."""
    sqlalchemy_mod = types.ModuleType("sqlalchemy")

    if isinstance(row_counts, int):
        counts = {
            "fact_funnel_metrics": row_counts,
            "fact_events": row_counts,
            "processed_batches": row_counts,
        }
    else:
        counts = row_counts

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query):
            for table_name, count in counts.items():
                if table_name in query:
                    return types.SimpleNamespace(scalar=lambda c=count: c)
            return types.SimpleNamespace(scalar=lambda: 0)

    class FakeEngine:
        def connect(self):
            return FakeConn()

    sqlalchemy_mod.create_engine = lambda *_args, **_kwargs: FakeEngine()
    sqlalchemy_mod.text = lambda query: query

    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_mod)


def _install_fake_requests(monkeypatch, responses: list[object]):
    requests_mod = types.ModuleType("requests")
    response_iter = iter(responses)

    class RequestException(Exception):
        pass

    requests_mod.RequestException = RequestException

    def fake_get(url, timeout):
        item = next(response_iter)
        if isinstance(item, Exception):
            raise item
        return item

    requests_mod.get = fake_get
    monkeypatch.setitem(sys.modules, "requests", requests_mod)
    return RequestException


def test_check_minio_success(monkeypatch):
    class FakeClientError(Exception):
        pass

    class FakeMinioClient:
        def __init__(self):
            self.seen_keys: list[str] = []

        def head_object(self, Bucket, Key):
            self.seen_keys.append(Key)

        def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000):
            return {"KeyCount": 1, "Contents": [{"Key": f"{Prefix}batch_001.csv"}]}

    client = FakeMinioClient()
    _install_fake_boto(monkeypatch, client, FakeClientError)

    assert vdf.check_minio() is True
    assert client.seen_keys == vdf.EXPECTED_KEYS


def test_check_minio_client_error_returns_false(monkeypatch):
    class FakeClientError(Exception):
        pass

    class FakeMinioClient:
        def head_object(self, Bucket, Key):
            raise FakeClientError("missing")

        def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000):
            return {"KeyCount": 0}

    _install_fake_boto(monkeypatch, FakeMinioClient(), FakeClientError)

    assert vdf.check_minio() is False


def test_check_minio_no_event_batches_returns_false(monkeypatch):
    class FakeClientError(Exception):
        pass

    class FakeMinioClient:
        def head_object(self, Bucket, Key):
            pass  # dimension files exist

        def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000):
            return {"KeyCount": 0}

    _install_fake_boto(monkeypatch, FakeMinioClient(), FakeClientError)

    assert vdf.check_minio() is False


def test_check_postgres_true_when_rows_exist(monkeypatch):
    _install_fake_sqlalchemy(monkeypatch, row_counts=42)

    assert vdf.check_postgres() is True


def test_check_postgres_false_when_empty(monkeypatch):
    _install_fake_sqlalchemy(monkeypatch, row_counts=0)

    assert vdf.check_postgres() is False


def test_check_postgres_false_when_fact_events_empty(monkeypatch):
    _install_fake_sqlalchemy(
        monkeypatch,
        row_counts={"fact_funnel_metrics": 10, "fact_events": 0, "processed_batches": 1},
    )

    assert vdf.check_postgres() is False


def test_check_metabase_success_first_attempt(monkeypatch):
    _install_fake_requests(monkeypatch, [FakeResponse(200, {"status": "ok"}, "ok")])

    assert vdf.check_metabase(max_retries=3, delay=0) is True


def test_check_metabase_retries_then_succeeds(monkeypatch):
    _install_fake_requests(
        monkeypatch,
        [
            FakeResponse(503, {"status": "starting"}, "starting"),
            FakeResponse(200, {"status": "ok"}, "ok"),
        ],
    )
    sleeps: list[int] = []
    monkeypatch.setattr(vdf.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert vdf.check_metabase(max_retries=3, delay=2) is True
    assert sleeps == [2]


def test_check_metabase_fails_after_max_retries(monkeypatch):
    _install_fake_requests(
        monkeypatch,
        [
            FakeResponse(503, {"status": "down"}, "down"),
            FakeResponse(503, {"status": "down"}, "down"),
            FakeResponse(503, {"status": "down"}, "down"),
        ],
    )
    sleeps: list[int] = []
    monkeypatch.setattr(vdf.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert vdf.check_metabase(max_retries=3, delay=5) is False
    assert sleeps == [5, 5]


def test_main_exits_zero_when_all_checks_pass(monkeypatch):
    monkeypatch.setattr(vdf, "check_minio", lambda: True)
    monkeypatch.setattr(vdf, "check_postgres", lambda: True)
    monkeypatch.setattr(vdf, "check_metabase", lambda: True)

    with pytest.raises(SystemExit) as exc:
        vdf.main()

    assert exc.value.code == 0


def test_main_exits_one_when_any_check_fails(monkeypatch):
    monkeypatch.setattr(vdf, "check_minio", lambda: True)
    monkeypatch.setattr(vdf, "check_postgres", lambda: False)
    monkeypatch.setattr(vdf, "check_metabase", lambda: True)

    with pytest.raises(SystemExit) as exc:
        vdf.main()

    assert exc.value.code == 1
