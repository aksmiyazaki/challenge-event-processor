import uuid
from datetime import datetime, timezone
import sys
from time import sleep

from dummy_producer.configuration.configuration import ProducerConfiguration
from kafka.producer.producer_boilerplate import KafkaProducer, SupportedSerializers
from logger.boilerplate import get_logger
from schemas.avro_auto_generated_classes.service_messages.ProducerToProcessor import ProducerToProcessor


def main():
    logger = get_logger()
    configuration = ProducerConfiguration(sys.argv[1:])

    logger.info("Building message producer")
    message_producer = KafkaProducer(
        configuration.schema_registry_url,
        SupportedSerializers.STRING_SERIALIZER,
        None,
        SupportedSerializers.AVRO_SERIALIZER,
        f"{configuration.kafka_output_topic}-value",
        configuration.kafka_bootstrap_server,
        configuration.amount_of_messages_before_poll,
        logger,
    )

    main_loop(configuration, message_producer, logger)


def main_loop(configuration, message_producer, logger):
    try:
        logger.info(
            f"Starting producing messages, will produce {configuration.amount_of_messages_to_produce} "
            f"messages of {configuration.list_of_destinations} destination types"
        )
        for iterator in range(configuration.amount_of_messages_to_produce):
            message = generate_message(configuration, iterator, f"dummy-payload {uuid.uuid4()}")

            send_async_message(
                message_producer,
                configuration.kafka_output_topic,
                configuration.origin_service_id,
                message,
                build_produce_contextual_callback(logger),
            )

            if configuration.sleep_between_messages_in_seconds > 0:
                sleep(configuration.sleep_between_messages_in_seconds)
    finally:
        message_producer.flush_producer()


def generate_message(configuration, iterator, message_payload):
    return ProducerToProcessor(
        {
            "origin_service_id": configuration.origin_service_id,
            "origin_service_type": configuration.origin_service_type,
            "destination_service_type": configuration.list_of_destinations[
                iterator % len(configuration.list_of_destinations)
            ],
            "payload": message_payload,
            "event_timestamp": get_now_as_milliseconds_from_epoch(),
        }
    )


def send_async_message(message_producer, topic, key, message_as_obj, callback):
    message_producer.asynchronous_send(
        topic=topic, key=key, value=message_as_obj.dict(), callback_after_delivery=callback,
    )


def get_now_as_milliseconds_from_epoch():
    return int(datetime.timestamp(datetime.now(tz=timezone.utc)) * 1000)


def build_produce_contextual_callback(logger):
    def produce_callback(err, msg):
        if err:
            logger.error(f"Error producing message! {err}")
        else:
            logger.info(f"Message sent! Offset: {msg.offset()}, Key: {msg.key()}, Value: {msg.value()}")

    return produce_callback


if __name__ == "__main__":
    main()
