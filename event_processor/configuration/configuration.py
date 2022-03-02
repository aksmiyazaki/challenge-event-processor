import argparse


class EventProcessorConfiguration:
    def __init__(self, args_list):
        parser = argparse.ArgumentParser("""
        Processor of Kafka Events, making a fanout of a multitenant topic to service-specific topics
        """)

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

        parsed_args = parser.parse_args(args_list)
        self.kafka_bootstrap_server = parsed_args.kafka_bootstrap_server
        self.kafka_source_topic = parsed_args.source_topic
        self.schema_registry_url = parsed_args.schema_registry_url
        self.group_id = parsed_args.group_id
        self.batch_size_to_commit_offsets = parsed_args.batch_size_to_commit_offsets
