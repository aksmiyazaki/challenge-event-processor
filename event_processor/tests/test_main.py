from unittest import mock
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError
from confluent_kafka.avro import SerializerError

from event_processor.main import build_contextual_commit_offsets_callback, build_output_producers, main_loop, \
    process_message


def test_error_on_contextual_commit_callback():
    logger = Mock()

    callback = build_contextual_commit_offsets_callback(logger)
    err = Mock()

    err.code.return_value = 123123
    callback(err, Mock())
    logger.error.assert_called_once()


def test_end_of_partition_on_contextual_commit_callback():
    logger = Mock()

    callback = build_contextual_commit_offsets_callback(logger)
    err = Mock()

    err.code.return_value = KafkaError._NO_OFFSET
    callback(err, Mock())
    logger.info.assert_called_once_with("End of partition event.")


def test_success_on_contextual_commit_callback():
    logger = Mock()

    callback = build_contextual_commit_offsets_callback(logger)
    err = None

    callback(err, "dummy")
    logger.info.assert_called_once_with(f"Committed partition offsets: dummy")


def test_successfully_build_output_producers():
    configuration = Mock()
    configuration.service_destinations = {}
    configuration.service_destinations["t1"] = {}
    configuration.service_destinations["t1"]["output_subject"] = "dummy"
    configuration.service_destinations["t2"] = {}
    configuration.service_destinations["t2"]["output_subject"] = "dummy2"

    with mock.patch("event_processor.main.KafkaProducer", autospec=True):
        res = build_output_producers(configuration, Mock())

        assert len(res.keys()) == 2
        assert "t1" in res.keys()
        assert "t2" in res.keys()


def test_main_loop_on_keyboard_interrupt():
    with mock.patch("event_processor.main.process_message") as process_message_mock:
        process_message_mock.side_effect = KeyboardInterrupt("interrupted")
        configuration = Mock()
        message_consumer = Mock()
        output_producers = {"m1": Mock(), "m2": Mock()}
        logger = Mock()

        main_loop(configuration, message_consumer, output_producers, logger)

        logger.info.assert_called_once_with("User asked for termination.")
        output_producers["m1"].flush_producer.assert_called_once()
        output_producers["m2"].flush_producer.assert_called_once()
        message_consumer.terminate.assert_called_once()


def test_main_loop_on_serialization_error():
    with mock.patch("event_processor.main.process_message") as process_message_mock:
        process_message_mock.side_effect = SerializerError("interrupted")
        configuration = Mock()
        message_consumer = Mock()
        output_producers = {"m1": Mock(), "m2": Mock()}
        logger = Mock()

        main_loop(configuration, message_consumer, output_producers, logger)

        logger.error.assert_called_once()
        output_producers["m1"].flush_producer.assert_called_once()
        output_producers["m2"].flush_producer.assert_called_once()
        message_consumer.terminate.assert_called_once()


def test_process_message():
    configuration = Mock()
    configuration.service_destinations = {"FINANCE": {}}
    configuration.service_destinations["FINANCE"]["output_topic"] = "dummy.topic"
    message_consumer = Mock()
    message = Mock()
    message.key.return_value = "dummy_key"
    message.value.return_value = "dummy_value"
    message_consumer.poll.return_value = message
    output_producers = {"FINANCE": Mock()}
    logger = Mock()

    with mock.patch("event_processor.main.ProducerToProcessor", autospec=True) as mocked_python_msg_object, \
            mock.patch("event_processor.main.ProcessorToConsumer", autospec=True) as mocked_python_consumer_msg_object, \
            mock.patch("event_processor.main.get_now_as_millisseconds_from_epoch") as get_now:
        expected_service_type = "FINANCE"
        expected_origin_service_id = "123"
        expected_event_timestamp = 123123
        expected_now = 123123
        expected_payload = "9876"

        producer_to_processor_message = Mock()
        producer_to_processor_message.get_destination_service_type.return_value = expected_service_type
        producer_to_processor_message.get_origin_service_id.return_value = expected_origin_service_id
        producer_to_processor_message.get_event_timestamp.return_value = expected_event_timestamp
        producer_to_processor_message.get_payload.return_value = expected_payload
        mocked_python_msg_object.return_value = producer_to_processor_message
        get_now.return_value = expected_now
        processor_to_consumer_message = Mock()
        processor_to_consumer_message.dict.return_value = "dummy_msg"
        mocked_python_consumer_msg_object.return_value = processor_to_consumer_message
        cb = Mock()

        process_message(configuration, message_consumer, output_producers, logger, cb)

        message_consumer.poll.assert_called_once()
        mocked_python_msg_object.assert_called_once_with("dummy_value")
        mocked_python_consumer_msg_object.assert_called_once_with({
            "producer_service_id": expected_origin_service_id,
            "processor_service_id": configuration.event_processor_id,
            "destination_type": expected_service_type,
            "producer_event_timestamp": expected_event_timestamp,
            "processor_event_timestamp": expected_now,
            "payload": expected_payload,
        })

        output_producers["FINANCE"].asynchronous_send.assert_called_once_with(
            topic="dummy.topic",
            key="dummy_key",
            value="dummy_msg",
            callback_after_delivery=cb
        )