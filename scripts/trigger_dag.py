"""
Trigger the clickstream Airflow DAG in a shell-agnostic way.

Usage:
    uv run python scripts/trigger_dag.py
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unpause and trigger an Airflow DAG run.")
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--dag-id", default="clickstream_pipeline")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
        logger.error("[FAIL] Could not trigger DAG: %s", exc)
        return 1

    payload = trigger_resp.json()
    logger.info("[OK] Triggered DAG '%s'", args.dag_id)
    logger.info("dag_run_id: %s", payload.get("dag_run_id", run_id))
    logger.info("state: %s", payload.get("state", "unknown"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
