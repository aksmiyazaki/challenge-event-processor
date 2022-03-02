import argparse
import json


class EventProcessorConfiguration:
    def __init__(self, args_list):
        parser = argparse.ArgumentParser("""
        Processor of Kafka Events, making a fanout of a multitenant topic to service-specific topics
        """)

        parser.add_argument("-processor_id",
                            dest="processor_id",
                            type=str,
                            help="Identifier of this processor instance.",
                            required=True)

        parser.add_argument("-bootstrap_server",
                            dest="kafka_bootstrap_server",
                            type=str,
                            help="Kafka bootstrap server with port. ex: kafka_server:9092",
                            required=True)

        parser.add_argument("-schema_registry_url",
                            dest="schema_registry_url",
                            type=str,
                            help="Schema Registry URL. ex: http://localhost:8081",
                            required=True)

        parser.add_argument("-source_topic",
                            dest="source_topic",
                            type=str,
                            help="The topic from where this processor will read events.",
                            required=True)

        parser.add_argument("-group_id",
                            dest="group_id",
                            type=str,
                            help="Kafka Consumer group ID.",
                            required=True)

        parser.add_argument("-batch_size_to_commit_offsets",
                            dest="batch_size_to_commit_offsets",
                            type=int,
                            help="Amount of messages to process before commiting offset (will commit on multiples"
                                 " of that number. ex: 10 will commit after the 10, 20, 30, .... messages.",
                            required=True)

        parser.add_argument("-destination_configurations",
                            dest="destination_configurations",
                            type=str,
                            help="A dict of pairs written like a json object, representing "
                                 "Service/topic relationship. This parameter is a String."
                                 "ex: {\"Finance\": {\"output_topic\": \"finance.topic\", "
                                 "\"output_subject\": \"finance.topic-value\"}, "
                                 "\"Marketing\":{\"output_topic\": \"marketing.topic\"}, "
                                 "\"output_subject\": \"marketing.topic-value\"}",
                            required=True)

        parsed_args = parser.parse_args(args_list)
        self.event_processor_id = parsed_args.processor_id
        self.kafka_bootstrap_server = parsed_args.kafka_bootstrap_server
        self.kafka_source_topic = parsed_args.source_topic
        self.schema_registry_url = parsed_args.schema_registry_url
        self.group_id = parsed_args.group_id
        self.batch_size_to_commit_offsets = parsed_args.batch_size_to_commit_offsets
        self.service_destinations = json.loads(parsed_args.destination_configurations)
        for key, value in self.service_destinations.items():
            if "output_topic" not in value.keys():
                raise Exception("Each service configuration must have an output_topic element.")
            if "output_subject" not in value.keys():
                raise Exception("Each service configuration must have an output_subject element.")
