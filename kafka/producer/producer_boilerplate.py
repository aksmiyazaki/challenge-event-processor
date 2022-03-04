from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from enum import Enum

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


class SupportedSerializers(Enum):
    STRING_SERIALIZER = 1
    AVRO_SERIALIZER = 2


class KafkaProducer:
    def __init__(
        self,
        schema_registry_url,
        key_serializer_type,
        key_serializer_subject,
        value_serializer_type,
        value_serializer_subject,
        bootstrap_servers,
        messages_to_poll,
        logger,
        must_initialize=True,
    ):
        self.schema_registry_conf = {"url": schema_registry_url}
        self.messages_to_poll = messages_to_poll
        self.__bootstrap_servers = bootstrap_servers
        self.__schema_registry_client = None
        self.__key_serializer_type = key_serializer_type
        self.__key_serializer_subject = key_serializer_subject
        self.__value_serializer_type = value_serializer_type
        self.__value_serializer_subject = value_serializer_subject
        self.__key_serialization_schema = None
        self.__value_serialization_schema = None
        self.__producer_config = {}
        self.__producer = None
        self.__logger = logger
        self.__amount_of_messages_sent = 0

        if must_initialize:
            logger.info("Initializing producer")
            self.initialize()

    def initialize(self):
        self.__schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        self.__key_serialization_schema = self.fetch_serialization_schema(
            self.__key_serializer_type, self.__key_serializer_subject
        )

        self.__value_serialization_schema = self.fetch_serialization_schema(
            self.__value_serializer_type, self.__value_serializer_subject
        )

        key_serializer = self.build_serializer(
            self.__key_serializer_type, self.__key_serialization_schema
        )
        value_serializer = self.build_serializer(
            self.__value_serializer_type, self.__value_serialization_schema
        )

        self.__producer_config["bootstrap.servers"] = self.__bootstrap_servers
        self.__producer_config["key.serializer"] = key_serializer
        self.__producer_config["value.serializer"] = value_serializer
        self.__producer = SerializingProducer(self.__producer_config)

    def fetch_serialization_schema(self, serializer_type, subject_name):
        if serializer_type == SupportedSerializers.AVRO_SERIALIZER:
            return self.__schema_registry_client.get_latest_version(subject_name)
        else:
            return None

    def build_serializer(self, serializer_type, schema):
        if serializer_type == SupportedSerializers.STRING_SERIALIZER:
            return StringSerializer()
        elif serializer_type == SupportedSerializers.AVRO_SERIALIZER:
            if (
                schema is None
                or schema.schema.schema_str is None
                or schema.schema.schema_str == ""
            ):
                raise ValueError(
                    f"Cannot encode {SupportedSerializers.AVRO_SERIALIZER} without a Schema"
                )
            return AvroSerializer(
                self.__schema_registry_client, schema.schema.schema_str
            )

    def asynchronous_send(self, key, value, topic, callback_after_delivery):
        self.__amount_of_messages_sent += 1
        if (self.__amount_of_messages_sent % self.messages_to_poll) == 0:
            self.__logger.info(
                f"Trying to poll for callbacks, produced {self.__amount_of_messages_sent} messages"
            )
            self.trigger_delivery_callbacks(1.0)

        return self.__producer.produce(
            topic=topic, key=key, value=value, on_delivery=callback_after_delivery
        )

    def trigger_delivery_callbacks(self, timeout_in_seconds=0.0):
        self.__producer.poll(timeout_in_seconds)

    def flush_producer(self):
        self.__logger.info("Triggered termination, flushing.")
        self.__producer.flush()
