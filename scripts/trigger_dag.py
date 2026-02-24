"""
Trigger the clickstream Airflow DAG in a shell-agnostic way.

Usage:
    uv run python scripts/trigger_dag.py
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unpause and trigger an Airflow DAG run.")
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--dag-id", default="clickstream_pipeline")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    auth = (args.username, args.password)
    run_id = f"manual_run_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    dag_url = f"{args.base_url}/api/v1/dags/{args.dag_id}"
    dag_runs_url = f"{dag_url}/dagRuns"

    try:
        pause_resp = requests.patch(
            dag_url,
            auth=auth,
            json={"is_paused": False},
            timeout=15,
        )
        pause_resp.raise_for_status()

        trigger_resp = requests.post(
            dag_runs_url,
            auth=auth,
            json={"dag_run_id": run_id},
            timeout=15,
        )
        trigger_resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[FAIL] Could not trigger DAG: {exc}")
        return 1

    payload = trigger_resp.json()
    print(f"[OK] Triggered DAG '{args.dag_id}'")
    print(f"     dag_run_id: {payload.get('dag_run_id', run_id)}")
    print(f"     state: {payload.get('state', 'unknown')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
