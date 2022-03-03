FROM python:3.9.2
COPY dummy_producer/ /usr/src/app/dummy_producer
COPY event_processor/ /usr/src/app/event_processor
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
WORKDIR /usr/src/app