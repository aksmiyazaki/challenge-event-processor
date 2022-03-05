from unittest import mock
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaException, TopicPartition

from kafka.consumer.consumer_boilerplate import KafkaConsumer, SupportedDeserializers

DUMMY_SCHEMA_REGISTRY_URL = "http://dummy:8081"
DUMMY_BOOTSTRAP_SERVERS = "dummy_server:9092"
DUMMY_GROUP_ID = "123"


def DUMMY_CALLBACK():
    pass


@pytest.fixture
def built_object():
    return KafkaConsumer(
        DUMMY_SCHEMA_REGISTRY_URL,
        SupportedDeserializers.STRING_DESERIALIZER,
        None,
        SupportedDeserializers.AVRO_DESERIALIZER,
        "dummy-subject-value",
        DUMMY_GROUP_ID,
        DUMMY_BOOTSTRAP_SERVERS,
        10,
        DUMMY_CALLBACK,
        Mock(),
        False,
    )


def test_successfully_initialize(built_object):
    with mock.patch(
        "kafka.consumer.consumer_boilerplate.DeserializingConsumer", autospec=True
    ) as ConsumerMock, mock.patch(
        "kafka.consumer.consumer_boilerplate.SchemaRegistryClient", autospec=True
    ) as SchemaRegistryMock:
        built_object.fetch_deserialization_schema = Mock()
        built_object.fetch_deserialization_schema.side_effect = [Mock(), Mock()]
        built_object.build_deserializer = Mock()
        key_deserializer = Mock()
        value_deserializer = Mock()
        built_object.build_deserializer.side_effect = [
            key_deserializer,
            value_deserializer,
        ]

        built_object.initialize()

        SchemaRegistryMock.assert_called_once_with({"url": DUMMY_SCHEMA_REGISTRY_URL})
        producer_param = {
            "key.deserializer": key_deserializer,
            "value.deserializer": value_deserializer,
            "bootstrap.servers": DUMMY_BOOTSTRAP_SERVERS,
            "group.id": DUMMY_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "on_commit": DUMMY_CALLBACK,
        }
        ConsumerMock.assert_called_once_with(producer_param)


def test_fetch_avro_deserialization_schema(built_object):
    expected_subject_name = "dummy-value"
    expected_return_value = "dummy-schema"

    with mock.patch.object(built_object, "_KafkaConsumer__schema_registry_client") as mocked_schema_registry:
        mocked_schema_registry.get_latest_version.return_value = expected_return_value
        res = built_object.fetch_deserialization_schema(SupportedDeserializers.AVRO_DESERIALIZER, expected_subject_name)
        mocked_schema_registry.get_latest_version.assert_called_once_with(expected_subject_name)
        assert res == expected_return_value


def test_fetch_string_deserialization_schema(built_object):
    res = built_object.fetch_deserialization_schema(SupportedDeserializers.STRING_DESERIALIZER, None)
    assert res is None


def test_build_string_deserializer(built_object):
    with mock.patch(
        "kafka.consumer.consumer_boilerplate.StringDeserializer", autospec=True
    ) as mock_string_deserializer:
        built_object.build_deserializer(SupportedDeserializers.STRING_DESERIALIZER, None)
        mock_string_deserializer.assert_called_once()


def test_build_avro_serializer(built_object):
    with mock.patch(
        "kafka.consumer.consumer_boilerplate.AvroDeserializer", autospec=True
    ) as mock_avro_deserializer, mock.patch.object(
        built_object, "_KafkaConsumer__schema_registry_client"
    ) as mocked_schema_registry:
        expected_schema = "dummy_schema"

        schema = Mock()
        schema.schema = Mock()
        schema.schema.schema_str = expected_schema

        built_object.build_deserializer(SupportedDeserializers.AVRO_DESERIALIZER, schema)
        mock_avro_deserializer.assert_called_once_with(mocked_schema_registry, expected_schema)


def test_fails_building_avro_serializer_with_none_schema(built_object):
    schema = None
    with pytest.raises(ValueError):
        built_object.build_deserializer(SupportedDeserializers.AVRO_DESERIALIZER, schema)


def test_fails_building_avro_serializer_with_none_schema_str(built_object):
    schema = Mock()
    schema.schema = Mock()
    schema.schema.schema_str = None
    with pytest.raises(ValueError):
        built_object.build_deserializer(SupportedDeserializers.AVRO_DESERIALIZER, schema)


def test_fails_building_avro_serializer_with_empty_schema_str(built_object):
    schema = Mock()
    schema.schema = Mock()
    schema.schema.schema_str = ""
    with pytest.raises(ValueError):
        built_object.build_deserializer(SupportedDeserializers.AVRO_DESERIALIZER, schema)


def test_default_poll(built_object):
    with mock.patch.object(built_object, "_KafkaConsumer__consumer") as mocked_consumer, mock.patch.object(
        built_object, "_KafkaConsumer__on_flight_message_queue"
    ) as mocked_queue:
        mocked_msg = Mock()
        mocked_msg.error.return_value = None
        mocked_consumer.poll.return_value = mocked_msg

        built_object.poll()

        mocked_consumer.poll.assert_called_once()
        mocked_queue.put.assert_called_once_with(mocked_msg)


def test_default_poll(built_object):
    with mock.patch.object(built_object, "_KafkaConsumer__consumer") as mocked_consumer:
        mocked_msg = Mock()
        mocked_msg.error.return_value = "something"
        mocked_consumer.poll.return_value = mocked_msg

        with pytest.raises(KafkaException):
            built_object.poll()


def test_terminate(built_object):
    with mock.patch.object(built_object, "_KafkaConsumer__consumer") as mocked_consumer:
        mocked_handle_commits = Mock()
        built_object.handle_offset_commits = mocked_handle_commits
        built_object.terminate()
        mocked_handle_commits.assert_called_once_with(False)
        mocked_consumer.close.assert_called_once_with()


def test_signalize_message_processed_without_offset_commit(built_object):
    expected_topic_partition = TopicPartition("dummy.topic", 1, 42)
    with mock.patch.object(built_object, "_KafkaConsumer__on_flight_message_queue") as mocked_queue:
        mocked_msg = Mock()
        mocked_msg.topic.return_value = "dummy.topic"
        mocked_msg.partition.return_value = 1
        mocked_msg.offset.return_value = 42
        built_object.messages_processed = 1

        mocked_queue.get.return_value = mocked_msg
        built_object.signalize_message_processed()
        offsets = built_object.get_last_known_offsets()

        assert built_object.messages_processed == 2
        assert "dummy.topic" in offsets
        assert 1 in offsets["dummy.topic"]
        assert offsets["dummy.topic"][1].topic == expected_topic_partition.topic
        assert offsets["dummy.topic"][1].offset == expected_topic_partition.offset
        assert offsets["dummy.topic"][1].partition == expected_topic_partition.partition


def test_signalize_message_processed_with_offset_commit(built_object):
    expected_topic_partition = TopicPartition("dummy.topic", 1, 42)
    with mock.patch.object(built_object, "_KafkaConsumer__on_flight_message_queue") as mocked_queue:
        mocked_msg = Mock()
        mocked_msg.topic.return_value = "dummy.topic"
        mocked_msg.partition.return_value = 1
        mocked_msg.offset.return_value = 42

        built_object.messages_processed = 9
        built_object.handle_offset_commits = Mock()

        mocked_queue.get.return_value = mocked_msg
        built_object.signalize_message_processed()
        offsets = built_object.get_last_known_offsets()

        assert built_object.messages_processed == 10
        assert "dummy.topic" in offsets
        assert 1 in offsets["dummy.topic"]
        assert offsets["dummy.topic"][1].topic == expected_topic_partition.topic
        assert offsets["dummy.topic"][1].offset == expected_topic_partition.offset
        assert offsets["dummy.topic"][1].partition == expected_topic_partition.partition
        built_object.handle_offset_commits.assert_called_once()


def test_handle_offset_commits(built_object):
    with mock.patch.object(
        built_object, "_KafkaConsumer__last_known_offsets"
    ) as mocked_last_offsets, mock.patch.object(built_object, "_KafkaConsumer__consumer") as mocked_consumer:
        p1 = TopicPartition("dummy.topic", 1, 42)
        p2 = TopicPartition("dummy.topic", 2, 666)
        mocked_last_offsets.items.return_value = {"dummy.topic": {1: p1, 2: p2}}.items()

        built_object.handle_offset_commits()
        mocked_consumer.commit.assert_called_once()
        assert mocked_consumer.commit.call_args_list[0][1]["offsets"][0].offset == 43
        assert mocked_consumer.commit.call_args_list[0][1]["offsets"][1].offset == 667
