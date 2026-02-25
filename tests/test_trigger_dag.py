from __future__ import annotations

import sys

import requests

from scripts import trigger_dag


class DummyResponse:
    def __init__(self, payload: dict | None = None, error: Exception | None = None):
        self._payload = payload or {}
        self._error = error

    def raise_for_status(self) -> None:
        if self._error is not None:
            raise self._error

    def json(self) -> dict:
        return self._payload


def test_main_success(monkeypatch):
    calls: dict[str, tuple] = {}

    def fake_patch(url, auth, json, timeout):
        calls["patch"] = (url, auth, json, timeout)
        return DummyResponse()

    def fake_post(url, auth, json, timeout):
        calls["post"] = (url, auth, json, timeout)
        return DummyResponse(payload={"dag_run_id": "manual_123", "state": "queued"})

    monkeypatch.setattr(trigger_dag.requests, "patch", fake_patch)
    monkeypatch.setattr(trigger_dag.requests, "post", fake_post)
    monkeypatch.setattr(sys, "argv", ["trigger_dag.py"])

    rc = trigger_dag.main()

    assert rc == 0
    assert calls["patch"][0] == "http://localhost:8080/api/v1/dags/clickstream_pipeline"
    assert calls["patch"][1] == ("admin", "admin")
    assert calls["post"][0] == "http://localhost:8080/api/v1/dags/clickstream_pipeline/dagRuns"
    assert calls["post"][1] == ("admin", "admin")
    assert "dag_run_id" in calls["post"][2]


def test_main_returns_1_when_unpause_fails(monkeypatch):
    post_called = {"value": False}

    def fake_patch(url, auth, json, timeout):
        return DummyResponse(error=requests.HTTPError("boom"))

    def fake_post(url, auth, json, timeout):
        post_called["value"] = True
        return DummyResponse(payload={"dag_run_id": "unused", "state": "queued"})

    monkeypatch.setattr(trigger_dag.requests, "patch", fake_patch)
    monkeypatch.setattr(trigger_dag.requests, "post", fake_post)
    monkeypatch.setattr(sys, "argv", ["trigger_dag.py"])

    rc = trigger_dag.main()

    assert rc == 1
    assert post_called["value"] is False


def test_main_returns_1_when_trigger_fails(monkeypatch):
    def fake_patch(url, auth, json, timeout):
        return DummyResponse()

    def fake_post(url, auth, json, timeout):
        raise requests.Timeout("timed out")

    monkeypatch.setattr(trigger_dag.requests, "patch", fake_patch)
    monkeypatch.setattr(trigger_dag.requests, "post", fake_post)
    monkeypatch.setattr(sys, "argv", ["trigger_dag.py"])

    rc = trigger_dag.main()

    assert rc == 1


def test_main_uses_custom_cli_args(monkeypatch):
    calls: dict[str, tuple] = {}

    def fake_patch(url, auth, json, timeout):
        calls["patch"] = (url, auth, json, timeout)
        return DummyResponse()

    def fake_post(url, auth, json, timeout):
        calls["post"] = (url, auth, json, timeout)
        return DummyResponse(payload={"dag_run_id": "custom", "state": "queued"})

    monkeypatch.setattr(trigger_dag.requests, "patch", fake_patch)
    monkeypatch.setattr(trigger_dag.requests, "post", fake_post)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "trigger_dag.py",
            "--base-url",
            "http://airflow.internal:8080",
            "--dag-id",
            "my_dag",
            "--username",
            "user1",
            "--password",
            "pass1",
        ],
    )

    rc = trigger_dag.main()

    assert rc == 0
    assert calls["patch"][0] == "http://airflow.internal:8080/api/v1/dags/my_dag"
    assert calls["post"][0] == "http://airflow.internal:8080/api/v1/dags/my_dag/dagRuns"
    assert calls["patch"][1] == ("user1", "pass1")
    assert calls["post"][1] == ("user1", "pass1")
