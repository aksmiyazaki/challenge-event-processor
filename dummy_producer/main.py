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
    try:
        logger.info(
            f"Starting producing messages, will produce {configuration.amount_of_messages_to_produce} "
            f"messages of {configuration.list_of_destinations} destination types"
        )

        for iterator in range(configuration.amount_of_messages_to_produce):
            message = ProducerToProcessor(
                {
                    "origin_service_id": configuration.origin_service_id,
                    "origin_service_type": configuration.origin_service_type,
                    "destination_service_type": configuration.list_of_destinations[
                        iterator % len(configuration.list_of_destinations)
                    ],
                    "payload": f"dummy-payload {uuid.uuid4()}",
                    "event_timestamp": int(datetime.timestamp(datetime.now(tz=timezone.utc)) * 1000),
                }
            )

            def produce_callback(err, msg):
                if err:
                    logger.error(f"Error producing message! {err}")
                else:
                    logger.info(f"Message sent! Offset: {msg.offset()}, Key: {msg.key()}, Value: {msg.value()}")

            message_producer.asynchronous_send(
                topic=configuration.kafka_output_topic,
                key=configuration.origin_service_id,
                value=message.dict(),
                callback_after_delivery=produce_callback,
            )

            if configuration.sleep_between_messages_in_seconds > 0:
                sleep(configuration.sleep_between_messages_in_seconds)
    finally:
        message_producer.flush_producer()


if __name__ == "__main__":
    main()
