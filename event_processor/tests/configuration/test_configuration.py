import pytest

from event_processor.configuration.configuration import EventProcessorConfiguration

DUMMY_BOOTSTRAP_SERVER = "dummy:9092"
DUMMY_SOURCE_TOPIC = "dummy.topic"
DUMMY_SCHEMA_REGISTRY_URL = "http://dummy:8081"
DUMMY_GROUP_ID = "dummy_group"
DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS = "25"


@pytest.fixture
def application_args():
    return [
        "-bootstrap_server",
        DUMMY_BOOTSTRAP_SERVER,
        "-source_topic",
        DUMMY_SOURCE_TOPIC,
        "-schema_registry_url",
        DUMMY_SCHEMA_REGISTRY_URL,
        "-group_id",
        DUMMY_GROUP_ID,
        "-batch_size_to_commit_offsets",
        DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS
    ]


def test_successfully_parse_args(application_args):
    config = EventProcessorConfiguration(application_args)
    assert config.kafka_source_topic == DUMMY_SOURCE_TOPIC
    assert config.kafka_bootstrap_server == DUMMY_BOOTSTRAP_SERVER
    assert config.schema_registry_url == DUMMY_SCHEMA_REGISTRY_URL
    assert config.group_id == DUMMY_GROUP_ID
    assert config.batch_size_to_commit_offsets == int(DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS)
