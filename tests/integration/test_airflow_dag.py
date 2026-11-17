"""
Deployment checks for the stream_to_minio DAG inside the running Airflow.

Require:  docker compose up -d postgres airflow-init airflow-scheduler
"""

import json

import pytest

pytestmark = pytest.mark.integration

DAG_ID = "stream_to_minio"


def test_dag_file_imports_without_errors(airflow):
    """The DAG file is parsed by Airflow without import errors."""
    output = airflow("dags", "list-import-errors", "-o", "json")
    errors = json.loads(output) if output.strip() and output.strip() != "[]" else []
    assert not [e for e in errors if "stream_to_minio" in e.get("filepath", "")], output


def test_dag_is_registered(airflow):
    """The DAG is visible to the scheduler under its expected id."""
    dags = json.loads(airflow("dags", "list", "-o", "json"))
    assert DAG_ID in {dag["dag_id"] for dag in dags}


def test_dag_runs_every_minute(scheduler_python):
    """The DAG schedule is the one-minute micro-batch declared in the DAG file."""
    output = scheduler_python(
        "from airflow.models import DagBag; "
        "dag = DagBag('/opt/airflow/dags', include_examples=False)"
        ".get_dag('stream_to_minio'); "
        "print(dag.schedule_interval.total_seconds())"
    )
    assert float(output.strip().splitlines()[-1]) == 60.0
