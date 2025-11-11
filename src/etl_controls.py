# src/etl_controls.py
import os
import json
import requests
from datetime import datetime

DEFAULT_DAG_ID = "eligibility_job_"


def trigger_airflow_dag(
    dag_id: str = DEFAULT_DAG_ID,
    base_url: str = None,
    token: str = None,
    run_conf: dict = None,
):
    """
    Triggers an Airflow DAG using the REST API (Airflow 2.x).
    Reads AIRFLOW_BASE_URL and AIRFLOW_TOKEN from env if not passed.
    Returns (ok: bool, message: str, dag_run_id: str|None)
    """
    base_url = base_url or os.getenv(
        "AIRFLOW_BASE_URL"
    )  # e.g. http://10.24.105.221:8080
    token = token or os.getenv("AIRFLOW_TOKEN")  # if using an auth token
    if not base_url:
        return False, "AIRFLOW_BASE_URL is not set", None

    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    payload = {
        "dag_run_id": f"manual__{datetime.now().strftime('%Y%m%dT%H%M%S')}",
        "conf": run_conf or {},
    }

    try:
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        if resp.status_code in (200, 201):
            jr = resp.json()
            return True, "DAG triggered", jr.get("dag_run_id")
        return False, f"Airflow error {resp.status_code}: {resp.text}", None
    except Exception as e:
        return False, f"Request failed: {e}", None
