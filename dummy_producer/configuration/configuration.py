import argparse


class ProducerConfiguration:
    def __init__(self, args_list):
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
            "-amount_of_messages",
            dest="amount_of_messages",
            type=int,
            help="Amount of messages to send.",
            required=True,
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
            "-amount_of_messages_before_poll",
            dest="amount_of_messages_before_poll",
            type=int,
            help="Number of messages before trying to poll for callbacks.",
            default=10,
            required=False,
        )

        parsed_args = parser.parse_args(args_list)
        parsed_args.list_of_destinations = [str.upper(el) for el in parsed_args.list_of_destinations]

        self.kafka_bootstrap_server = parsed_args.kafka_bootstrap_server
        self.kafka_output_topic = parsed_args.target_topic
        self.schema_registry_url = parsed_args.schema_registry_url
        self.origin_service_type = parsed_args.origin_service_type
        self.origin_service_id = parsed_args.origin_service_id
        self.list_of_destinations = parsed_args.list_of_destinations
        self.amount_of_messages_to_produce = parsed_args.amount_of_messages
        self.sleep_between_messages_in_seconds = parsed_args.sleep_between_messages_in_seconds
        self.amount_of_messages_before_poll = parsed_args.amount_of_messages_before_poll
