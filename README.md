# challenge-event-processor
An implementation of a challenge that requires an event processor.

-> Python 3.9.2 because it was available in pyenv.
-> changed composed schema because of problems with confluent cloud version: https://github.com/confluentinc/schema-registry/issues/1439
-> avro-to-python
    -> namespaces are generated in java package formats.
    -> There's some adjustments to do after generating classes 
        - adjust import on service_messages to fully qualified
        - Remove import from init at root.
-> automate avro class generation and replace of stuff.
-> turn params case insensitive (destination topics)
-> hot partition using the service id as key?

-bootstrap_server localhost:9092 -schema_registry_url http://localhost:8081 -target_topic producer.to.processor -origin_service_type financial -origin_service_id 123456 -list_of_destinations FINANCIAL LOGISTICS marketing -amount_of_messages 10

