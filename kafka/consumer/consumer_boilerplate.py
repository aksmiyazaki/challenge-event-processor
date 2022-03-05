from enum import Enum
from queue import Queue

from confluent_kafka import DeserializingConsumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


class SupportedDeserializers(Enum):
    STRING_DESERIALIZER = 1
    AVRO_DESERIALIZER = 2


class KafkaConsumer:
    def __init__(
        self,
        schema_registry_url,
        key_deserializer_type,
        key_deserializer_subject,
        value_deserializer_type,
        value_deserializer_subject,
        kafka_consumer_group_id,
        kafka_bootstrap_servers,
        amount_of_messages_processes_to_commit_offsets,
        callback_after_committing_offsets,
        logger,
        must_initialize=True,
    ):
        self.__commit_offsets_callback = callback_after_committing_offsets
        self.__kafka_consumer_group_id = kafka_consumer_group_id
        self.__bootstrap_servers = kafka_bootstrap_servers
        self.__key_deserializer_type = key_deserializer_type
        self.__key_deserializer_subject = key_deserializer_subject
        self.__value_deserializer_type = value_deserializer_type
        self.__value_deserializer_subject = value_deserializer_subject
        self.__logger = logger
        self.__amount_of_messages_processed_to_commit_offsets = amount_of_messages_processes_to_commit_offsets
        self.__schema_registry_client = None
        self.__value_deserialization_schema = None
        self.__key_deserialization_schema = None
        self.__consumer = None
        self.__on_flight_message_queue = Queue()
        self.__last_known_offsets = {}

        self.schema_registry_conf = {"url": schema_registry_url}
        self.messages_processed = 0

        if must_initialize:
            logger.info("Initializing consumer...")
            self.initialize()

    def initialize(self):
        self.__schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        self.__key_deserialization_schema = self.fetch_deserialization_schema(
            self.__key_deserializer_type, self.__key_deserializer_subject
        )

        self.__value_deserialization_schema = self.fetch_deserialization_schema(
            self.__value_deserializer_type, self.__value_deserializer_subject
        )

        key_deserializer = self.build_deserializer(self.__key_deserializer_type, self.__key_deserialization_schema)
        value_deserializer = self.build_deserializer(
            self.__value_deserializer_type, self.__value_deserialization_schema
        )

        consumer_config = {
            "key.deserializer": key_deserializer,
            "value.deserializer": value_deserializer,
            "bootstrap.servers": self.__bootstrap_servers,
            "group.id": self.__kafka_consumer_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "on_commit": self.__commit_offsets_callback,
        }
        self.__consumer = DeserializingConsumer(consumer_config)

    def fetch_deserialization_schema(self, serializer_type, subject_name):
        if serializer_type == SupportedDeserializers.AVRO_DESERIALIZER:
            return self.__schema_registry_client.get_latest_version(subject_name)
        else:
            return None

    def build_deserializer(self, serializer_type, schema):
        if serializer_type == SupportedDeserializers.STRING_DESERIALIZER:
            return StringDeserializer()
        elif serializer_type == SupportedDeserializers.AVRO_DESERIALIZER:
            if schema is None or schema.schema.schema_str is None or schema.schema.schema_str == "":
                raise ValueError(f"Cannot encode {SupportedDeserializers.AVRO_DESERIALIZER} without a Schema")
            return AvroDeserializer(self.__schema_registry_client, schema.schema.schema_str)

    def poll(self):
        msg = None
        while msg is None:
            msg = self.__consumer.poll(1.0)
            if msg is None:
                self.__logger.debug("No message, polling again")

        if msg.error():
            raise KafkaException(msg.error())
        else:
            self.__on_flight_message_queue.put(msg)
            return msg

    def terminate(self):
        self.handle_offset_commits(False)
        self.__consumer.close()

    def signalize_message_processed(self):
        msg = self.__on_flight_message_queue.get()
        self.messages_processed += 1

        self.__handle_topic_on_offsets_control(msg.topic())
        self.__update_partition_of_message(msg)

        if (self.messages_processed % self.__amount_of_messages_processed_to_commit_offsets) == 0:
            self.handle_offset_commits()

    def __handle_topic_on_offsets_control(self, topic):
        if topic not in self.__last_known_offsets:
            self.__last_known_offsets[topic] = {}

    def __update_partition_of_message(self, msg):
        self.__last_known_offsets[msg.topic()][msg.partition()] = TopicPartition(
            msg.topic(), msg.partition(), msg.offset()
        )

    def get_last_known_offsets(self):
        return self.__last_known_offsets.copy()

    def handle_offset_commits(self, asynchronous=True):
        list_to_commit = self.__convert_control_offset_dictionary_to_list()
        self.__logger.info(
            f"After {self.messages_processed} messages this is the list of offsets being commited: {list_to_commit}"
        )
        self.__consumer.commit(offsets=list_to_commit, asynchronous=asynchronous)

    def __convert_control_offset_dictionary_to_list(self):
        list_of_partitions = []
        for _, partitions in self.__last_known_offsets.items():
            for _, partition_object in partitions.items():
                partition_object.offset += 1
                list_of_partitions.append(partition_object)
        return list_of_partitions

    def subscribe_topic(self, topic):
        self.__consumer.subscribe([topic])
