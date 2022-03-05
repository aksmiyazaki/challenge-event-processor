import pytest

from event_processor.configuration.configuration import EventProcessorConfiguration

DUMMY_BOOTSTRAP_SERVER = "dummy:9092"
DUMMY_SOURCE_TOPIC = "dummy.topic"
DUMMY_SCHEMA_REGISTRY_URL = "http://dummy:8081"
DUMMY_GROUP_ID = "dummy_group"
DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS = "25"
DUMMY_DESTINATION_TOPICS = (
    '{"finance": {"output_topic": "finance.topic", '
    '"output_subject":"finance.topic-value"},'
    '"marketing": {"output_topic": "marketing.topic", '
    '"output_subject":"marketing.topic-value"}}'
)
DUMMY_PROCESSOR_ID = "potato"
DUMMY_BROKEN_JSON_WITHOUT_OUTPUT_TOPIC = (
    '{"finance": {"output_subject":"finance.topic-value"},'
    '"marketing": {"output_topic": "marketing.topic", '
    '"output_subject":"marketing.topic-value"}}'
)

DUMMY_BROKEN_JSON_WITHOUT_OUTPUT_SUBJECT = (
    '{"finance": {"output_topic": "finance.topic"},'
    '"marketing": {"output_topic": "marketing.topic", '
    '"output_subject":"marketing.topic-value"}}'
)


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
        DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS,
        "-destination_configurations",
        DUMMY_DESTINATION_TOPICS,
        "-processor_id",
        DUMMY_PROCESSOR_ID,
    ]


@pytest.fixture
def application_args_with_broken_topic_config():
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
        DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS,
        "-destination_configurations",
        DUMMY_BROKEN_JSON_WITHOUT_OUTPUT_TOPIC,
        "-processor_id",
        DUMMY_PROCESSOR_ID,
    ]


@pytest.fixture
def application_args_with_broken_subject_config():
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
        DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS,
        "-destination_configurations",
        DUMMY_BROKEN_JSON_WITHOUT_OUTPUT_SUBJECT,
        "-processor_id",
        DUMMY_PROCESSOR_ID,
    ]


def test_successfully_parse_args(application_args):
    config = EventProcessorConfiguration(application_args)
    assert config.kafka_source_topic == DUMMY_SOURCE_TOPIC
    assert config.kafka_bootstrap_server == DUMMY_BOOTSTRAP_SERVER
    assert config.schema_registry_url == DUMMY_SCHEMA_REGISTRY_URL
    assert config.group_id == DUMMY_GROUP_ID
    assert config.batch_size_to_commit_offsets == int(DUMMY_BATCH_SIZE_TO_COMMIT_OFFSETS)
    assert len(config.service_destinations) == 2
    assert config.service_destinations["finance"]["output_topic"] == "finance.topic"
    assert config.service_destinations["finance"]["output_subject"] == "finance.topic-value"
    assert config.event_processor_id == DUMMY_PROCESSOR_ID


def test_fails_parsing_topic_on_json_arg(application_args_with_broken_topic_config):
    try:
        EventProcessorConfiguration(application_args_with_broken_topic_config)
    except ValueError as ex:
        assert "output_topic element." in str(ex)


def test_fails_parsing_subject_on_json_arg(application_args_with_broken_subject_config):
    try:
        EventProcessorConfiguration(application_args_with_broken_subject_config)
    except ValueError as ex:
        assert "output_subject element." in str(ex)
