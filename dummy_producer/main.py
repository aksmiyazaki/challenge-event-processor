import argparse
import sys

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def main():
    cli_args = parse_cli_arguments(sys.argv[1:])

    schema_registry_conf = {
        'url': cli_args.schema_registry_url
    }

    print(schema_registry_conf)
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    schema = schema_registry_client.get_latest_version(f"{cli_args.target_topic}")
    print(schema.schema.schema_str)

    value_serializer = AvroSerializer(schema.schema.schema_str, schema_registry_client)
    key_serializer = StringSerializer()

    producer_config = {}
    producer_config['key.serializer'] = key_serializer
    producer_config['value.serializer'] = value_serializer
    producer = SerializingProducer(producer_config)

    for n in range(10):
        name_object = ccloud_lib.Name()
        name_object.name = "alice"
        count_object = ccloud_lib.Count()
        count_object.count = n
        print("Producing Avro record: {}\t{}".format(name_object.name, count_object.count))
        producer.produce(topic=topic, key=name_object, value=count_object, on_delivery=acked)
        producer.poll(0)

    producer.flush()




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