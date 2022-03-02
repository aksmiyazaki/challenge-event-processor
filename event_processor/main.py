import sys

from confluent_kafka.avro import SerializerError

from event_processor.configuration.configuration import EventProcessorConfiguration
from kafka.consumer.boilerplate import KafkaConsumer, SupportedDeserializers
from kafka.producer.boilerplate import KafkaProducer, SupportedSerializers
from schemas.avro_auto_generated_classes.service_messages.ProcessorToConsumer import ProcessorToConsumer
from schemas.avro_auto_generated_classes.service_messages.ProducerToProcessor import ProducerToProcessor

from datetime import datetime, timezone

message_counter = 0


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

    while True:
        try:
            msg = message_consumer.poll()
            message_content = ProducerToProcessor(msg.value())

            process_message(msg.key(),
                            message_content,
                            output_producers,
                            configuration.service_destinations[message_content.destination_service_type][
                                "output_topic"],
                            build_contextual_callback(configuration, message_consumer))
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


def process_message(key, value_as_object, output_producers, output_topic, producing_callback):
    print(f"Processing message: {key}, {value_as_object.dict()}")
    target_service_type = value_as_object.get_destination_service_type()

    if target_service_type not in output_producers.keys():
        raise Exception(f"No destination for {target_service_type}.")

    processor_message = ProcessorToConsumer({
        "producer_service_id": value_as_object.get_origin_service_id(),
        "processor_service_id": "MAKEME",
        "destination_type": value_as_object.get_destination_service_type(),
        "producer_event_timestamp": value_as_object.get_event_timestamp(),
        "processor_event_timestamp": int(datetime.timestamp(datetime.now(tz=timezone.utc)) * 1000),
        "payload": value_as_object.get_payload()
    })

    output_producers[target_service_type].asynchronous_send(topic=output_topic,
                                                            key=key,
                                                            value=processor_message.dict(),
                                                            callback_after_delivery=producing_callback)


def build_contextual_callback(configuration, consumer):
    def check_and_commit_offsets(err, msg):
        global message_counter
        if err:
            print(f"Couldn't deliver message.")
        else:
            message_counter += 1
            print(f"Send Message Successful, {message_counter}")
            if (message_counter % configuration.batch_size_to_commit_offsets) == 0:
                print(f"Committing offsets on {message_counter} msgs")
                consumer.commit_offsets()

    return check_and_commit_offsets


if __name__ == '__main__':
    main()
