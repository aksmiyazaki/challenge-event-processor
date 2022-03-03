FROM python:3.9.2
ARG BOOTSTRAP_SERVERS
ARG SCHEMA_REGISTRY_URL
ARG TARGET_TOPIC
ARG ORIGIN_SERVICE_TYPE
ARG ORIGIN_SERVICE_ID
ARG LIST_OF_DESTINATIONS
ARG AMOUNT_OF_MESSAGES
COPY dummy_producer/ /usr/src/app/dummy_producer
COPY kafka/ /usr/src/app/kafka
COPY logger/ /usr/src/app/logger
COPY schemas/ /usr/src/app/schemas
COPY Makefile /usr/src/app/
COPY requirements.txt /usr/src/app
RUN pip install -r /usr/src/app/requirements.txt
RUN avro-to-python /usr/src/app/schemas/avro_schemas/processor-to-consumer.avsc /usr/src/app/schemas/avro_auto_generated_classes/
RUN	avro-to-python /usr/src/app/schemas/avro_schemas/producer-to-processor.avsc /usr/src/app/schemas/avro_auto_generated_classes/
RUN	sed -i 's/from helpers/from schemas.avro_auto_generated_classes.helpers/g' /usr/src/app/schemas/avro_auto_generated_classes/service_messages/ProcessorToConsumer.py
RUN	sed -i 's/from helpers/from schemas.avro_auto_generated_classes.helpers/g' /usr/src/app/schemas/avro_auto_generated_classes/service_messages/ProducerToProcessor.py
RUN	sed -i 's/from service_messages.ProducerToProcessor import ProducerToProcessor//g' /usr/src/app/schemas/avro_auto_generated_classes/service_messages/__init__.py
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app/"
WORKDIR /usr/src/app/dummy_producer
CMD ["sh", "-c", "sleep 10; python main.py -bootstrap_server $BOOTSTRAP_SERVERS -schema_registry_url $SCHEMA_REGISTRY_URL -target_topic $TARGET_TOPIC -origin_service_type $ORIGIN_SERVICE_TYPE -origin_service_id $ORIGIN_SERVICE_ID -list_of_destinations $LIST_OF_DESTINATIONS -amount_of_messages $AMOUNT_OF_MESSAGES"]