from unittest import mock
from unittest.mock import Mock

from dummy_producer.main import generate_message, send_async_message, main_loop


def test_default_main_loop():
    configuration = Mock()
    configuration.amount_of_messages_to_produce = 1
    configuration.sleep_between_messages_in_seconds = 0
    message_producer = Mock()
    logger = Mock()

    with mock.patch("dummy_producer.main.generate_message") as mock_generate_message, mock.patch(
        "dummy_producer.main.send_async_message"
    ) as mock_send_async_message:
        main_loop(configuration, message_producer, logger)

        mock_generate_message.assert_called_once()
        mock_send_async_message.assert_called_once()
        message_producer.flush_producer.assert_called_once()


def test_default_main_loop_with_sleep():
    configuration = Mock()
    configuration.amount_of_messages_to_produce = 1
    configuration.sleep_between_messages_in_seconds = 99
    message_producer = Mock()
    logger = Mock()

    with mock.patch("dummy_producer.main.generate_message") as mock_generate_message, mock.patch(
        "dummy_producer.main.send_async_message"
    ) as mock_send_async_message, mock.patch("dummy_producer.main.sleep") as mock_sleep:
        main_loop(configuration, message_producer, logger)

        mock_generate_message.assert_called_once()
        mock_send_async_message.assert_called_once()
        message_producer.flush_producer.assert_called_once()
        mock_sleep.assert_called_once_with(99)


def test_generate_message():
    configuration = Mock()
    configuration.origin_service_id = "123"
    configuration.origin_service_type = "AUDITING"
    configuration.list_of_destinations = ["MARKETING"]

    with mock.patch("dummy_producer.main.get_now_as_milliseconds_from_epoch") as get_now:
        get_now.return_value = 123123

        res = generate_message(configuration, 1, "dummy-message")

        assert res.get_origin_service_id() == "123"
        assert res.get_origin_service_type() == "AUDITING"
        assert res.get_destination_service_type() == "MARKETING"
        assert res.get_payload() == "dummy-message"
        assert res.get_event_timestamp() == 123123


def test_send_async_message():
    message = Mock()
    message.dict.return_value = "dummy_dict"
    callback = Mock()
    message_producer = Mock()

    send_async_message(message_producer, "dummy_topic", "dummy_key", message, callback)

    message_producer.asynchronous_send.assert_called_once_with(
        topic="dummy_topic", key="dummy_key", value="dummy_dict", callback_after_delivery=callback
    )
