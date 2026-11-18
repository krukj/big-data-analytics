"""
Deployment checks for the Kafka topics created by the kafka-init service.

Require:  docker compose up -d kafka kafka-init
"""

import pytest

pytestmark = pytest.mark.integration

EXPECTED_TOPICS = {"raw-transactions", "raw-logins", "preprocessed-transactions"}
EXPECTED_PARTITIONS = 3


@pytest.fixture(scope="module")
def topics(kafka_admin):
    return kafka_admin.list_topics(timeout=10).topics


def test_required_topics_exist(topics):
    """Verify kafka-init created the topics used by the pipeline.
    
    Steps:
    1. List the topics of the broker with the Kafka AdminClient.
    Expected:
    - raw-transactions, raw-logins and preprocessed-transactions exist.
    """
    assert EXPECTED_TOPICS <= set(topics)


def test_topics_have_three_partitions(topics):
    """Verify each pipeline topic has the configured number of partitions.
    
    Steps:
    1. List the topics of the broker with the Kafka AdminClient.
    2. Count the partitions of the three pipeline topics.
    Expected:
    - Each topic has 3 partitions.
    """
    for name in EXPECTED_TOPICS:
        assert len(topics[name].partitions) == EXPECTED_PARTITIONS, name
