"""
Deployment checks for the stream_to_minio DAG inside the running Airflow.

Require:  docker compose up -d postgres airflow-init airflow-scheduler
"""

import json

import pytest

pytestmark = pytest.mark.integration

DAG_ID = "stream_to_minio"


def test_dag_file_imports_without_errors(airflow):
    """Verify Airflow parses the stream_to_minio DAG file without import errors.

    Steps:
    1. Run airflow dags list-import-errors -o json in the scheduler container.
    Expected:
    - No import error refers to the stream_to_minio DAG file.
    """
    output = airflow("dags", "list-import-errors", "-o", "json")
    errors = json.loads(output) if output.strip() and output.strip() != "[]" else []
    print(f"import errors reported by Airflow: {len(errors)}")
    print(f"files with import errors: {[e.get('filepath') for e in errors] or 'none'}")
    assert not [e for e in errors if "stream_to_minio" in e.get("filepath", "")], output


def test_dag_is_registered(airflow):
    """Verify the stream_to_minio DAG is registered in the scheduler.

    Steps:
    1. Run airflow dags list -o json in the scheduler container.
    Expected:
    - stream_to_minio is among the listed dag_ids.
    """
    dags = json.loads(airflow("dags", "list", "-o", "json"))
    print(f"registered DAGs: {sorted(dag['dag_id'] for dag in dags)}")
    print(f"{DAG_ID} registered: {DAG_ID in {dag['dag_id'] for dag in dags}}")
    assert DAG_ID in {dag["dag_id"] for dag in dags}


def test_dag_runs_every_minute(scheduler_python):
    """Verify the DAG is scheduled as a one-minute micro-batch.

    Steps:
    1. Load the DAG with DagBag inside the scheduler container.
    2. Print schedule_interval.total_seconds().
    Expected:
    - The schedule interval is 60 seconds.
    """
    output = scheduler_python(
        "from airflow.models import DagBag; "
        "dag = DagBag('/opt/airflow/dags', include_examples=False)"
        ".get_dag('stream_to_minio'); "
        "print(dag.schedule_interval.total_seconds())"
    )
    interval = float(output.strip().splitlines()[-1])
    print(f"schedule_interval of {DAG_ID}: {interval} s (expected 60.0)")
    assert interval == 60.0
