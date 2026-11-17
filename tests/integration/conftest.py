"""
Fixtures for the integration suite: connections to the services of the compose
stack. Every fixture skips its tests when the service is not reachable, so the
suite is safe to run on a machine without Docker.

Run:  docker compose up -d && uv run pytest tests/integration -m integration -v
"""

import os
import subprocess

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError

from config import KAFKA_BROKER, MINIO_PASSWORD, MINIO_URL, MINIO_USER

AIRFLOW_CONTAINER = os.getenv("AIRFLOW_CONTAINER", "airflow-scheduler")


@pytest.fixture(scope="session")
def s3():
    """boto3 client for MinIO; skips when the service does not answer."""
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
    )
    try:
        client.list_buckets()
    except EndpointConnectionError:
        pytest.skip("MinIO is not reachable; start the minio service first")
    return client


@pytest.fixture(scope="session")
def kafka_admin():
    """confluent_kafka AdminClient for the broker; skips when it does not answer."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    try:
        admin.list_topics(timeout=5)
    except KafkaException:
        pytest.skip("Kafka is not reachable; start the kafka service first")
    return admin


@pytest.fixture(scope="session")
def airflow():
    """Runs `airflow <args>` inside the scheduler container and returns stdout."""

    def run(*args: str, timeout: int = 120) -> str:
        result = subprocess.run(
            ["docker", "exec", AIRFLOW_CONTAINER, "airflow", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        return result.stdout

    try:
        run("version", timeout=30)
    except (RuntimeError, FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("Airflow scheduler container is not running")
    return run


@pytest.fixture(scope="session")
def cassandra_session():
    """Session on the fraud_detection keyspace; skips without driver or service."""
    cassandra = pytest.importorskip("cassandra.cluster")

    cluster = cassandra.Cluster(
        [os.getenv("CASSANDRA_HOST", "localhost")],
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
    )
    try:
        session = cluster.connect("fraud_detection")
    except cassandra.NoHostAvailable:
        pytest.skip("Cassandra is not reachable; start the serving profile first")
    yield session
    cluster.shutdown()