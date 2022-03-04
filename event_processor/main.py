import sys

from confluent_kafka import KafkaError

from logger.boilerplate import get_logger

from confluent_kafka.avro import SerializerError

from event_processor.configuration.configuration import EventProcessorConfiguration
from kafka.consumer.consumer_boilerplate import KafkaConsumer, SupportedDeserializers
from kafka.producer.producer_boilerplate import KafkaProducer, SupportedSerializers
from schemas.avro_auto_generated_classes.service_messages.ProcessorToConsumer import (
    ProcessorToConsumer,
)
from schemas.avro_auto_generated_classes.service_messages.ProducerToProcessor import (
    ProducerToProcessor,
)

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
                process_message(
                    configuration, message_consumer, output_producers, logger,
                    build_contextual_callback(configuration, message_consumer, logger)
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


def process_message(
        configuration, message_consumer, output_producers, logger, callback
):
    msg = message_consumer.poll()
    key = msg.key()
    parsed_message = ProducerToProcessor(msg.value())

    logger.debug(f"Processing message: {key}, {parsed_message.dict()}")
    target_service_type = parsed_message.get_destination_service_type()

    if target_service_type not in output_producers.keys():
        raise Exception(f"No destination for {target_service_type}.")

    processor_message = ProcessorToConsumer(
        {
            "producer_service_id": parsed_message.get_origin_service_id(),
            "processor_service_id": configuration.event_processor_id,
            "destination_type": parsed_message.get_destination_service_type(),
            "producer_event_timestamp": parsed_message.get_event_timestamp(),
            "processor_event_timestamp": get_now_as_millisseconds_from_epoch(),
            "payload": parsed_message.get_payload(),
        }
    )

    output_producers[target_service_type].asynchronous_send(
        topic=configuration.service_destinations[
            parsed_message.get_destination_service_type()
        ]["output_topic"],
        key=key,
        value=processor_message.dict(),
        callback_after_delivery=callback
    )


def get_now_as_millisseconds_from_epoch():
    return int(datetime.timestamp(datetime.now(tz=timezone.utc)) * 1000)


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
