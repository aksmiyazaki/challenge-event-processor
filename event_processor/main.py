import sys
from time import sleep

from confluent_kafka.avro import SerializerError

from event_processor.configuration.configuration import EventProcessorConfiguration
from kafka.consumer.boilerplate import KafkaConsumer, SupportedDeserializers
from kafka.producer.boilerplate import KafkaProducer, SupportedSerializers
from schemas.avro_auto_generated_classes.service_messages.ProducerToProcessor import ProducerToProcessor

from datetime import datetime, timezone


def main():
    configuration = EventProcessorConfiguration(sys.argv[1:])
    message_consumer = KafkaConsumer(configuration.schema_registry_url,
                                     SupportedDeserializers.STRING_DESERIALIZER,
                                     None,
                                     SupportedDeserializers.AVRO_DESERIALIZER,
                                     f"{configuration.kafka_source_topic}-value",
                                     configuration.group_id,
                                     configuration.kafka_bootstrap_server,
                                     on_commit_offsets)

    message_consumer.subscribe_topic(configuration.kafka_source_topic)
    output_producers = build_output_producers(configuration)

    counter = 0
    while True:
        try:
            msg = message_consumer.poll()
            print(msg.key())
            message_content = ProducerToProcessor(msg.value())
            print(
                f"{msg.partition()}:{msg.offset()}  "
                f"{message_content.payload} "
                f"{datetime.fromtimestamp(message_content.event_timestamp / 1000, tz=timezone.utc)}")
            counter += 1
            print(f"Consumed {counter} msgs")

            if (counter % configuration.batch_size_to_commit_offsets) == 0:
                print(f"Committing offsets on {counter} msgs")
                message_consumer.commit_offsets()
                sleep(1)

        except KeyboardInterrupt:
            print("User asked for termination.")
            break
        except SerializerError as e:
            print(f"Message Deserialization failed {e}")
            pass

    message_consumer.terminate()


def on_commit_offsets(err, partitions):
    if err:
        print(str(err))
    else:
        print(f"Committed partition offsets: {str(partitions)}")


def build_output_producers(configuration):
    output_producers = {}
    for key, value in configuration.service_destinations.items():
        output_producers[key] = KafkaProducer(configuration.schema_registry_url,
                                              SupportedSerializers.STRING_SERIALIZER,
                                              None,
                                              SupportedSerializers.AVRO_SERIALIZER,
                                              value["output_subject"],
                                              configuration.kafka_bootstrap_server)
    return output_producers


def process_message(key, value_as_object, output_producers, output_topic):
    print(f"Processing message: {key}, {value_as_object.dict()}")


if __name__ == '__main__':
    main()
