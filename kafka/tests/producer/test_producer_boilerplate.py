from unittest import mock
from unittest.mock import Mock

import pytest

from kafka.producer.producer_boilerplate import KafkaProducer, SupportedSerializers

DUMMY_SCHEMA_REGISTRY_URL = "http://dummy:8081"
DUMMY_BOOTSTRAP_SERVERS = "dummy_server:9092"


@pytest.fixture
def built_object():
    return KafkaProducer(
        DUMMY_SCHEMA_REGISTRY_URL,
        SupportedSerializers.STRING_SERIALIZER,
        None,
        SupportedSerializers.AVRO_SERIALIZER,
        "dummy-subject-value",
        DUMMY_BOOTSTRAP_SERVERS,
        10,
        Mock(),
        False,
    )


def test_successfully_initialize(built_object):
    with mock.patch(
        "kafka.producer.producer_boilerplate.SerializingProducer", autospec=True
    ) as ProducerMock, mock.patch(
        "kafka.producer.producer_boilerplate.SchemaRegistryClient", autospec=True
    ) as SchemaRegistryMock:
        built_object.fetch_serialization_schema = Mock()
        built_object.fetch_serialization_schema.side_effect = [Mock(), Mock()]
        built_object.build_serializer = Mock()
        key_serializer = Mock()
        value_serializer = Mock()
        built_object.build_serializer.side_effect = [key_serializer, value_serializer]

        built_object.initialize()

        SchemaRegistryMock.assert_called_once_with({"url": DUMMY_SCHEMA_REGISTRY_URL})
        producer_param = {
            "bootstrap.servers": DUMMY_BOOTSTRAP_SERVERS,
            "key.serializer": key_serializer,
            "value.serializer": value_serializer,
        }
        ProducerMock.assert_called_once_with(producer_param)


def test_fetch_avro_serialization_schema(built_object):
    expected_subject_name = "dummy-value"
    expected_return_value = "dummy-schema"

    with mock.patch.object(
        built_object, "_KafkaProducer__schema_registry_client"
    ) as mocked_schema_registry:
        mocked_schema_registry.get_latest_version.return_value = expected_return_value
        res = built_object.fetch_serialization_schema(
            SupportedSerializers.AVRO_SERIALIZER, expected_subject_name
        )
        mocked_schema_registry.get_latest_version.assert_called_once_with(
            expected_subject_name
        )
        assert res == expected_return_value


def test_fetch_string_serialization_schema(built_object):
    res = built_object.fetch_serialization_schema(
        SupportedSerializers.STRING_SERIALIZER, None
    )
    assert res is None


def test_build_string_serializer(built_object):
    with mock.patch(
        "kafka.producer.producer_boilerplate.StringSerializer", autospec=True
    ) as mock_string_serializer:
        built_object.build_serializer(SupportedSerializers.STRING_SERIALIZER, None)
        mock_string_serializer.assert_called_once()


def test_build_avro_serializer(built_object):
    with mock.patch(
        "kafka.producer.producer_boilerplate.AvroSerializer", autospec=True
    ) as mock_avro_serializer, mock.patch.object(
        built_object, "_KafkaProducer__schema_registry_client"
    ) as mocked_schema_registry:
        expected_schema = "dummy_schema"

        schema = Mock()
        schema.schema = Mock()
        schema.schema.schema_str = expected_schema

        built_object.build_serializer(SupportedSerializers.AVRO_SERIALIZER, schema)
        mock_avro_serializer.assert_called_once_with(
            mocked_schema_registry, expected_schema
        )


def test_fails_building_avro_serializer_with_none_schema(built_object):
    schema = None
    with pytest.raises(ValueError):
        built_object.build_serializer(SupportedSerializers.AVRO_SERIALIZER, schema)


def test_fails_building_avro_serializer_with_none_schema_str(built_object):
    schema = Mock()
    schema.schema = Mock()
    schema.schema.schema_str = None
    with pytest.raises(ValueError):
        built_object.build_serializer(SupportedSerializers.AVRO_SERIALIZER, schema)


def test_fails_building_avro_serializer_with_empty_schema_str(built_object):
    schema = Mock()
    schema.schema = Mock()
    schema.schema.schema_str = ""
    with pytest.raises(ValueError):
        built_object.build_serializer(SupportedSerializers.AVRO_SERIALIZER, schema)


def test_asynchronous_send(built_object):
    expected_key = "asdf"
    expected_value = "dummy"
    expected_topic = "dummy_topic"
    expected_callback = lambda x: x

    with mock.patch.object(built_object, "_KafkaProducer__producer") as mocked_producer:
        built_object.asynchronous_send(
            expected_key, expected_value, expected_topic, expected_callback
        )
        mocked_producer.produce.assert_called_once_with(
            topic=expected_topic,
            key=expected_key,
            value=expected_value,
            on_delivery=expected_callback,
        )


def test_poll_asynchronous_send(built_object):
    expected_key = "asdf"
    expected_value = "dummy"
    expected_topic = "dummy_topic"
    expected_callback = lambda x: x

    with mock.patch.object(built_object, "_KafkaProducer__producer") as mocked_producer:
        built_object.trigger_delivery_callbacks = Mock()
        built_object.messages_to_poll = 1
        built_object.asynchronous_send(
            expected_key, expected_value, expected_topic, expected_callback
        )
        built_object.trigger_delivery_callbacks.assert_called_once_with(1.0)
        mocked_producer.produce.assert_called_once_with(
            topic=expected_topic,
            key=expected_key,
            value=expected_value,
            on_delivery=expected_callback,
        )
