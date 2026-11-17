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
    """kafka-init created the two raw topics and the preprocessed one."""
    assert EXPECTED_TOPICS <= set(topics)


def test_topics_have_three_partitions(topics):
    """Each topic has the three partitions declared in docker-compose."""
    for name in EXPECTED_TOPICS:
        assert len(topics[name].partitions) == EXPECTED_PARTITIONS, name
