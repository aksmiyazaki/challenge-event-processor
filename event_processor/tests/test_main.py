from unittest import mock
from unittest.mock import Mock

from confluent_kafka import KafkaError
from confluent_kafka.avro import SerializerError

from event_processor.main import (
    build_contextual_commit_offsets_callback,
    build_output_producers,
    main_loop,
    fetch_message_from_kafka,
    transform_message_to_target_consumer_service,
    send_message_to_downstream_service_topic,
)


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
    with mock.patch("event_processor.main.fetch_message_from_kafka") as fetch_message_mock:
        fetch_message_mock.side_effect = KeyboardInterrupt("interrupted")
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
    with mock.patch("event_processor.main.fetch_message_from_kafka") as fetch_message_mock:
        fetch_message_mock.side_effect = SerializerError("interrupted")
        configuration = Mock()
        message_consumer = Mock()
        output_producers = {"m1": Mock(), "m2": Mock()}
        logger = Mock()

        main_loop(configuration, message_consumer, output_producers, logger)

        logger.error.assert_called_once()
        output_producers["m1"].flush_producer.assert_called_once()
        output_producers["m2"].flush_producer.assert_called_once()
        message_consumer.terminate.assert_called_once()


def test_fetch_message_from_kafka():
    message_consumer = Mock()
    message = Mock()
    message.key.return_value = "potato"
    message.value.return_value = "carrot"

    message_consumer.poll.return_value = message

    with mock.patch("event_processor.main.ProducerToProcessor", autospec=True) as mocked_parsed_message:
        mocked_parsed_message.return_value = "tomato"
        key, value = fetch_message_from_kafka(message_consumer)

        message_consumer.poll.assert_called_once()
        mocked_parsed_message.assert_called_once_with("carrot")
        assert key == "potato"
        assert value == "tomato"


def test_transform_message():
    event_processor_id = "12341234"
    origin_key = "potato"
    origin_message = Mock()
    origin_message.dict.return_vaue = "dummy_dict"
    origin_message.get_origin_service_id.return_value = "dummy_service_id"
    origin_message.get_destination_service_type.return_value = "FINANCE"
    origin_message.get_event_timestamp.return_value = 123123
    origin_message.get_payload.return_value = "dummy_payload"

    with mock.patch("event_processor.main.get_now_as_milliseconds_from_epoch") as get_now:
        get_now.return_value = 98765
        key_res, res = transform_message_to_target_consumer_service(
            event_processor_id, origin_key, origin_message, Mock()
        )
        assert key_res == origin_key
        assert res.producer_service_id == "dummy_service_id"
        assert res.processor_service_id == event_processor_id
        assert res.destination_type == "FINANCE"
        assert res.producer_event_timestamp == 123123
        assert res.payload == "dummy_payload"
        assert res.processor_event_timestamp == 98765


def test_send_message_to_downstream_service():
    dummy_key = "dummy_key"
    destination_message = Mock()
    destination_message.dict.return_value = "dummy_dict"
    destination_message.get_destination_type.return_value = "FINANCE"
    output_producers = {"FINANCE": Mock()}
    service_destinations = {"FINANCE": {"output_topic": "dummy.topic"}}
    callback = Mock()

    send_message_to_downstream_service_topic(
        dummy_key, destination_message, output_producers, service_destinations, Mock(), callback,
    )

    output_producers["FINANCE"].asynchronous_send.assert_called_once_with(
        topic="dummy.topic", key="dummy_key", value="dummy_dict", callback_after_delivery=callback,
    )


def test_failure_send_message_to_downstream_non_existent_service():
    dummy_key = "dummy_key"
    destination_message = Mock()
    destination_message.dict.return_value = "dummy_dict"
    destination_message.get_destination_type.return_value = "MARKETING"
    output_producers = {"FINANCE": Mock()}
    service_destinations = {"FINANCE": {"output_topic": "dummy.topic"}}
    callback = Mock()
    try:
        send_message_to_downstream_service_topic(
            dummy_key, destination_message, output_producers, service_destinations, Mock(), callback,
        )
    except ValueError as ex:
        assert "No destination for" in str(ex)


def test_contextual_commit_callback():
    logger = Mock()

    callback = build_contextual_commit_offsets_callback(logger)
