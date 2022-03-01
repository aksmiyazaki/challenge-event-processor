import argparse
import uuid
from datetime import datetime, timezone
import sys

from dummy_producer.kafka_producer.kafka_producer import KafkaProducer, SupportedSerializers
from schemas.avro_auto_generated_classes.service_messages.ProducerToProcessor import ProducerToProcessor


def main():
    cli_args = parse_cli_arguments(sys.argv[1:])
    message_producer = KafkaProducer(cli_args.schema_registry_url,
                                     SupportedSerializers.STRING_SERIALIZER,
                                     None,
                                     SupportedSerializers.AVRO_SERIALIZER,
                                     f"{cli_args.target_topic}-value",
                                     cli_args.kafka_bootstrap_server)

    for iterator in range(cli_args.amount_of_messages):
        message = ProducerToProcessor({
            "origin_service_id": cli_args.origin_service_id,
            "origin_service_type": cli_args.origin_service_type,
            "destination_service_type": cli_args.list_of_destinations[iterator % len(cli_args.list_of_destinations)],
            "payload": f"dummy-payload {uuid.uuid4()}",
            "event_timestamp": int(datetime.timestamp(datetime.now(tz=timezone.utc)) * 1000)
        })

        def callback(err, msg):
            if err:
                print(f"ERROR! {err}")
            else:
                print(f"Message sent! {msg}")

        message_producer.asynchronous_send(topic=cli_args.target_topic,
                                           key=cli_args.origin_service_id,
                                           value=message.dict(),
                                           callback_after_delivery=callback)

    message_producer.terminate()


def parse_cli_arguments(args_list):
    parser = argparse.ArgumentParser("Dummy producer of avro data to Kafka")
    parser.add_argument("-bootstrap_server",
                        dest="kafka_bootstrap_server",
                        type=str,
                        help="Kafka bootstrap server with port. ex: kafka_server:9092",
                        required=True)

    parser.add_argument("-schema_registry_url",
                        dest="schema_registry_url",
                        type=str,
                        help="Schema Registry URL. ex:http://localhost:8081",
                        required=True)

    parser.add_argument("-target_topic",
                        dest="target_topic",
                        type=str,
                        help="The topic where this producer will produce messages.",
                        required=True)

    parser.add_argument("-origin_service_type",
                        dest="origin_service_type",
                        type=str,
                        help="The type of this particular service.",
                        required=True)

    parser.add_argument("-origin_service_id",
                        dest="origin_service_id",
                        type=str,
                        help="The id of this instance of service",
                        required=True)

    parser.add_argument("-list_of_destinations",
                        dest="list_of_destinations",
                        nargs='+',
                        help="List of service types that this message is destinated separated by space",
                        required=True)

    parser.add_argument("-amount_of_messages",
                        dest="amount_of_messages",
                        type=int,
                        help="Amount of messages to send.",
                        required=True)

    args = parser.parse_args(args_list)
    args.list_of_destinations = [str.upper(el) for el in args.list_of_destinations]
    print(args.list_of_destinations)
    return args


if __name__ == '__main__':
    main()
