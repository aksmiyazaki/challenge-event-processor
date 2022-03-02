.PHONY: local-docker-compose-spinup
local-docker-compose-spinup:
	docker-compose -f ./docker/docker-compose-local-dev.yml up -d

.PHONY: local-docker-compose-teardown
local-docker-compose-teardown:
	docker-compose -f ./docker/docker-compose-local-dev.yml down

.PHONY: local-setup
local-setup:
	pip install -r requirements.txt
	pip install -r requirements-development.txt

.PHONY: production-setup
production-setup:
	pip install -r requirements.txt

.PHONY: local-register-schemas
local-register-schemas:
	./schemas/avro_schemas/register-schema.sh ./schemas/avro_schemas/producer-to-processor.avsc http://localhost:8081/subjects/producer.to.processor-value/versions
	./schemas/avro_schemas/register-schema.sh ./schemas/avro_schemas/processor-to-consumer.avsc http://localhost:8081/subjects/processor.to.consumer-value/versions

.PHONY: local-generate-classes-from-avro-schemas
local-generate-classes-from-avro-schemas:
	avro-to-python schemas/avro_schemas/processor-to-consumer.avsc schemas/avro_auto_generated_classes/
	avro-to-python schemas/avro_schemas/producer-to-processor.avsc schemas/avro_auto_generated_classes/
	sed -i 's/from helpers/from schemas.avro_auto_generated_classes.helpers/g' schemas/avro_auto_generated_classes/service_messages/ProcessorToConsumer.py
	sed -i 's/from helpers/from schemas.avro_auto_generated_classes.helpers/g' schemas/avro_auto_generated_classes/service_messages/ProducerToProcessor.py
	sed -i 's/from service_messages.ProducerToProcessor import ProducerToProcessor//g' schemas/avro_auto_generated_classes/service_messages/__init__.py
