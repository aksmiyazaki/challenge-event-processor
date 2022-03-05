import pytest

from dummy_producer.configuration.configuration import ProducerConfiguration

DUMMY_BOOTSTRAP_SERVER = "dummy:9092"
DUMMY_SCHEMA_REGISTRY_URL = "http://dummy:8081"
DUMMY_TARGET_TOPIC = "dummy.topic"
DUMMY_ORIGIN_SERVICE_TYPE = "AUDITING"
DUMMY_ORIGIN_SERVICE_ID = "123123"
DUMMY_AMOUNT_OF_MESSAGES = "10"


@pytest.fixture
def application_args():
    return [
        "-bootstrap_server",
        DUMMY_BOOTSTRAP_SERVER,
        "-schema_registry_url",
        DUMMY_SCHEMA_REGISTRY_URL,
        "-target_topic",
        DUMMY_TARGET_TOPIC,
        "-origin_service_type",
        DUMMY_ORIGIN_SERVICE_TYPE,
        "-origin_service_id",
        DUMMY_ORIGIN_SERVICE_ID,
        "-list_of_destinations",
        "FINANCE",
        "MaRkEtInG",
        "-amount_of_messages",
        DUMMY_AMOUNT_OF_MESSAGES,
    ]


def test_successfully_create_configuration(application_args):
    config = ProducerConfiguration(application_args)
    assert config.kafka_bootstrap_server == DUMMY_BOOTSTRAP_SERVER
    assert config.schema_registry_url == DUMMY_SCHEMA_REGISTRY_URL
    assert config.kafka_output_topic == DUMMY_TARGET_TOPIC
    assert config.origin_service_type == DUMMY_ORIGIN_SERVICE_TYPE
    assert config.origin_service_id == DUMMY_ORIGIN_SERVICE_ID
    assert config.amount_of_messages_to_produce == int(DUMMY_AMOUNT_OF_MESSAGES)

    assert len(config.list_of_destinations) == 2
    assert "FINANCE" in config.list_of_destinations
    assert "MARKETING" in config.list_of_destinations
