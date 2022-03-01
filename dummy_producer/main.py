import argparse
from datetime import datetime, timezone
import sys

from dummy_producer.kafka_producer.kafka_producer import KafkaProducer, SupportedSerializers
from schemas.avro_auto_generated_classes.com.github.aksmiyazaki.ProducerToProcessor import ProducerToProcessor


def main():
    cli_args = parse_cli_arguments(sys.argv[1:])
    message_producer = KafkaProducer(cli_args.schema_registry_url,
                                     SupportedSerializers.STRING_SERIALIZER,
                                     None,
                                     SupportedSerializers.AVRO_SERIALIZER,
                                     f"{cli_args.target_topic}-value",
                                     cli_args.kafka_bootstrap_server)

    message = ProducerToProcessor(
        {
            "origin_service_id": "POTATO",
            "origin_service_type": "Tomato",
            "destination_service_type": "Financial",
            "payload": "bleeergh",
            "event_timestamp": int(datetime.timestamp(datetime.now(tz=timezone.utc)) * 1000)
        })

    def end(err, msg):
        print("Message done!")
        if err:
            print(f"ERROR! {err}")
        else:
            print(f"Msg {msg}")

    message_producer.asynchronous_send(topic=cli_args.target_topic,
                                       key="POTATO",
                                       value=message.dict(),
                                       callback_after_delivery=end)

    message_producer.producer.flush()


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
                        help="Schema Registry URL.",
                        required=True)

    parser.add_argument("-target_topic",
                        dest="target_topic",
                        type=str,
                        help="The topic where this producer will produce messages.",
                        required=True)

    args = parser.parse_args(args_list)
    return args


if __name__ == '__main__':
    main()
