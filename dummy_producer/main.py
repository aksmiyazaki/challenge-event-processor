import argparse
import uuid
from datetime import datetime, timezone
import sys
from time import sleep

from kafka.producer.producer_boilerplate import KafkaProducer, SupportedSerializers
from logger.boilerplate import get_logger
from schemas.avro_auto_generated_classes.service_messages.ProducerToProcessor import ProducerToProcessor


def main():
    logger = get_logger()
    cli_args = parse_cli_arguments(sys.argv[1:])
    logger.info("Building message producer")
    message_producer = KafkaProducer(
        cli_args.schema_registry_url,
        SupportedSerializers.STRING_SERIALIZER,
        None,
        SupportedSerializers.AVRO_SERIALIZER,
        f"{cli_args.target_topic}-value",
        cli_args.kafka_bootstrap_server,
        cli_args.no_messages_before_poll,
        logger,
    )
    try:
        logger.info(
            f"Starting producing messages, will produce {cli_args.amount_of_messages} "
            f"messages of {cli_args.list_of_destinations} destination types"
        )

        for iterator in range(cli_args.amount_of_messages):
            message = ProducerToProcessor(
                {
                    "origin_service_id": cli_args.origin_service_id,
                    "origin_service_type": cli_args.origin_service_type,
                    "destination_service_type": cli_args.list_of_destinations[
                        iterator % len(cli_args.list_of_destinations)
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
                topic=cli_args.target_topic,
                key=cli_args.origin_service_id,
                value=message.dict(),
                callback_after_delivery=produce_callback,
            )

            if cli_args.sleep_between_messages_in_seconds > 0:
                sleep(cli_args.sleep_between_messages_in_seconds)
    finally:
        message_producer.flush_producer()


def parse_cli_arguments(args_list):
    parser = argparse.ArgumentParser("Dummy producer of avro data to Kafka")
    parser.add_argument(
        "-bootstrap_server",
        dest="kafka_bootstrap_server",
        type=str,
        help="Kafka bootstrap server with port. ex: kafka_server:9092",
        required=True,
    )

    parser.add_argument(
        "-schema_registry_url",
        dest="schema_registry_url",
        type=str,
        help="Schema Registry URL. ex:http://localhost:8081",
        required=True,
    )

    parser.add_argument(
        "-target_topic",
        dest="target_topic",
        type=str,
        help="The topic where this producer will produce messages.",
        required=True,
    )

    parser.add_argument(
        "-origin_service_type",
        dest="origin_service_type",
        type=str,
        help="The type of this particular service.",
        required=True,
    )

    parser.add_argument(
        "-origin_service_id",
        dest="origin_service_id",
        type=str,
        help="The id of this instance of service",
        required=True,
    )

    parser.add_argument(
        "-list_of_destinations",
        dest="list_of_destinations",
        nargs="+",
        help="List of service types that this message is destinated separated by space",
        required=True,
    )

    parser.add_argument(
        "-amount_of_messages", dest="amount_of_messages", type=int, help="Amount of messages to send.", required=True,
    )

    parser.add_argument(
        "-sleep_between_messages_in_seconds",
        dest="sleep_between_messages_in_seconds",
        type=int,
        help="Sleep between messages.",
        default=0,
        required=False,
    )

    parser.add_argument(
        "-no_messages_before_poll",
        dest="no_messages_before_poll",
        type=int,
        help="Number of messages before trying to poll for callbacks.",
        default=10,
        required=False,
    )

    args = parser.parse_args(args_list)
    args.list_of_destinations = [str.upper(el) for el in args.list_of_destinations]
    return args


if __name__ == "__main__":
    main()
