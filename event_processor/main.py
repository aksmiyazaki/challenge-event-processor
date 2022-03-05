import sys

from confluent_kafka import KafkaError

from logger.boilerplate import get_logger

from confluent_kafka.avro import SerializerError

from event_processor.configuration.configuration import EventProcessorConfiguration
from kafka.consumer.consumer_boilerplate import KafkaConsumer, SupportedDeserializers
from kafka.producer.producer_boilerplate import KafkaProducer, SupportedSerializers
from schemas.avro_auto_generated_classes.service_messages.ProcessorToConsumer import ProcessorToConsumer
from schemas.avro_auto_generated_classes.service_messages.ProducerToProcessor import ProducerToProcessor

from datetime import datetime, timezone

message_counter = 0


def main():
    configuration = EventProcessorConfiguration(sys.argv[1:])

    logger = get_logger(configuration.event_processor_id)
    logger.info("Parsed Configuration")

    logger.info("Building Kafka Consumer")
    consumer = KafkaConsumer(
        configuration.schema_registry_url,
        SupportedDeserializers.STRING_DESERIALIZER,
        None,
        SupportedDeserializers.AVRO_DESERIALIZER,
        f"{configuration.kafka_source_topic}-value",
        configuration.group_id,
        configuration.kafka_bootstrap_server,
        build_contextual_commit_offsets_callback(logger),
        logger,
    )

    logger.info("Building Kafka Producers")
    output_producers = build_output_producers(configuration, logger)

    logger.info("Entering on Main Loop")
    main_loop(configuration, consumer, output_producers, logger)


def build_contextual_commit_offsets_callback(logger):
    def on_commit_offsets(err, partitions):
        if err:
            if err.code() == KafkaError._NO_OFFSET:
                logger.info("End of partition event.")
            else:
                logger.error(str(err))
        else:
            logger.info(f"Committed partition offsets: {str(partitions)}")

    return on_commit_offsets


def build_output_producers(configuration, logger):
    output_producers = {}
    for key, value in configuration.service_destinations.items():
        output_producers[key] = KafkaProducer(
            configuration.schema_registry_url,
            SupportedSerializers.STRING_SERIALIZER,
            None,
            SupportedSerializers.AVRO_SERIALIZER,
            value["output_subject"],
            configuration.kafka_bootstrap_server,
            configuration.no_messages_to_poll,
            logger,
        )
    return output_producers


def main_loop(configuration, message_consumer, output_producers, logger, is_running=True):
    message_consumer.subscribe_topic(configuration.kafka_source_topic)

    try:
        while is_running:
            try:
                origin_key, origin_service_message = fetch_message_from_kafka(message_consumer)

                (target_key, target_service_message,) = transform_message_to_target_consumer_service(
                    configuration.event_processor_id, origin_key, origin_service_message, logger,
                )

                send_message_to_downstream_service_topic(
                    target_key,
                    target_service_message,
                    output_producers,
                    configuration.service_destinations,
                    logger,
                    build_contextual_callback(configuration, message_consumer, logger),
                )
            except KeyboardInterrupt:
                logger.info("User asked for termination.")
                is_running = False
            except SerializerError as e:
                logger.error(f"Message Deserialization failed {e}. Ending this process.")
                is_running = False
    finally:
        for _, producer in output_producers.items():
            producer.flush_producer()
        message_consumer.terminate()


def fetch_message_from_kafka(message_consumer):
    msg = message_consumer.poll()
    key = msg.key()
    parsed_value = ProducerToProcessor(msg.value())

    return key, parsed_value


def transform_message_to_target_consumer_service(event_processor_id, origin_key, origin_message, logger):
    logger.debug(f"Transforming message: {origin_key}, {origin_message.dict()}")

    return ProcessorToConsumer(
        {
            "producer_service_id": origin_message.get_origin_service_id(),
            "processor_service_id": event_processor_id,
            "destination_type": origin_message.get_destination_service_type(),
            "producer_event_timestamp": origin_message.get_event_timestamp(),
            "processor_event_timestamp": get_now_as_milliseconds_from_epoch(),
            "payload": origin_message.get_payload(),
        }
    )


def get_now_as_milliseconds_from_epoch():
    return int(datetime.timestamp(datetime.now(tz=timezone.utc)) * 1000)


def send_message_to_downstream_service_topic(
    key, destination_message, output_producers, service_destinations, logger, callback
):
    target_service_type = destination_message.get_destination_type()
    if target_service_type not in output_producers.keys():
        raise ValueError(f"No destination for {target_service_type}.")

    logger.info(f"Sending message: {key}, {destination_message.dict()}")
    output_producers[target_service_type].asynchronous_send(
        topic=service_destinations[destination_message.get_destination_type()]["output_topic"],
        key=key,
        value=destination_message.dict(),
        callback_after_delivery=callback,
    )


def build_contextual_callback(configuration, consumer, logger):
    def check_and_commit_offsets(err, msg):
        global message_counter
        if err:
            logger.error(f"Couldn't deliver message: {msg.key()} {msg.value()}")
        else:
            message_counter += 1
            logger.debug(f"Send Message Successful, {message_counter}")
            if (message_counter % configuration.batch_size_to_commit_offsets) == 0:
                logger.info(f"Committing offsets on {message_counter} msgs")
                consumer.commit_offsets()

    return check_and_commit_offsets


if __name__ == "__main__":
    main()
