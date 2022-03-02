from enum import Enum

from confluent_kafka import DeserializingConsumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


class SupportedDeserializers(Enum):
    STRING_DESERIALIZER = 1
    AVRO_DESERIALIZER = 2


class KafkaConsumer:
    def __init__(self,
                 schema_registry_url,
                 key_deserializer_type,
                 key_deserializer_subject,
                 value_deserializer_type,
                 value_deserializer_subject,
                 group_id,
                 bootstrap_servers,
                 callback_commit_offsets,
                 logger):
        self.__commit_offsets_callback = callback_commit_offsets
        self.__group_id = group_id
        self.__bootstrap_servers = bootstrap_servers
        self.__schema_registry_url = schema_registry_url
        self.schema_registry_conf = {
            'url': self.__schema_registry_url
        }
        self.__schema_registry_client = None
        self.__key_deserializer_type = key_deserializer_type
        self.__key_deserializer_subject = key_deserializer_subject
        self.__value_deserializer_type = value_deserializer_type
        self.__value_deserializer_subject = value_deserializer_subject
        self.__key_deserialization_schema = None
        self.__value_deserialization_schema = None
        self.__consumer_config = {}
        self.__consumer = None
        self.__logger = logger
        logger.info("Initializing consumer...")
        self.initialize()

    def initialize(self):
        self.__schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        self.__key_deserialization_schema = self.fetch_deserialization_schema(self.__key_deserializer_type,
                                                                              self.__key_deserializer_subject)

        self.__value_deserialization_schema = self.fetch_deserialization_schema(self.__value_deserializer_type,
                                                                                self.__value_deserializer_subject)

        key_deserializer = self.build_deserializer(self.__key_deserializer_type,
                                                   self.__key_deserialization_schema)
        value_deserializer = self.build_deserializer(self.__value_deserializer_type,
                                                     self.__value_deserialization_schema)

        self.__consumer_config["key.deserializer"] = key_deserializer
        self.__consumer_config["value.deserializer"] = value_deserializer
        self.__consumer_config["bootstrap.servers"] = self.__bootstrap_servers
        self.__consumer_config["group.id"] = self.__group_id
        self.__consumer_config["auto.offset.reset"] = "earliest"
        self.__consumer_config["enable.auto.commit"] = False
        self.__consumer_config["on_commit"] = self.__commit_offsets_callback
        self.__consumer = DeserializingConsumer(self.__consumer_config)

    def fetch_deserialization_schema(self, serializer_type, subject_name):
        if serializer_type == SupportedDeserializers.AVRO_DESERIALIZER:
            return self.__schema_registry_client.get_latest_version(subject_name)
        else:
            return None

    def build_deserializer(self, serializer_type, schema):
        if serializer_type not in SupportedDeserializers:
            raise Exception(f"{serializer_type} is not supported. Supported types are: {SupportedDeserializers}.")

        if serializer_type == SupportedDeserializers.STRING_DESERIALIZER:
            return StringDeserializer()
        elif serializer_type == SupportedDeserializers.AVRO_DESERIALIZER:
            if schema is None or schema.schema.schema_str == "":
                raise Exception(f"Cannot encode {SupportedDeserializers.AVRO_DESERIALIZER} without a Schema")
            return AvroDeserializer(self.__schema_registry_client, schema.schema.schema_str)

    def subscribe_topic(self, topic):
        self.__consumer.subscribe([topic])

    def poll(self):
        msg = None
        while msg is None:
            msg = self.__consumer.poll(1.0)
            if msg is None:
                self.__logger.debug("No message, polling again")

        if msg.error():
            raise KafkaException(msg.error())
        else:
            return msg

    def commit_offsets(self):
        self.__consumer.commit(asynchronous=True)

    def terminate(self):
        try:
            self.__consumer.commit(asynchronous=False)
        except KafkaException as e:
            if e.args[0].code() == KafkaError._NO_OFFSET:
                self.__logger.info("No offsets to commit, moving on.")
        self.__consumer.close()
