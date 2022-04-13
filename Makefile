.PHONY: help
help:
	$(info ************ Available Commands ************)
	$(info make test: Executes unit tests)
	$(info make lint: Reformats code according to a standard (runs black))
	$(info make local-docker-compose-spinup: spins a docker environment locally. Spins just kafka / schema registry)
	$(info make local-docker-compose-teardown: tears down the environment spinned in the command above)
	$(info make local-docker-compose-panda-spinup: spins a docker environment locally. Spins redpanda environment)
	$(info make local-docker-compose-panda-teardown: tears down the environment spinned in the command above)
	$(info make local-setup: installs python dependencies for development. Doing this in a virtual environment is recommended)
	$(info make local-generate-classes-from-avro-schemas: Generates classes from avro schemas.)
	$(info make local-register-schemas: register schemas in the local docker compose kafka.)
	$(info make production-setup: installs python dependencies for production.)
	$(info make dockerized-register-schemas: register schemas in the docker compose environment.)
	$(info make build-docker-custom-images: builds docker custom images.)
	$(info make run-dockerized-environment: THIS IS PROBABLY WHAT YOU WANT. Builds custom images and runs an environment with kafka, 2 producers and 1 event processor)
	$(info make run-dockerized-environment-redpanda: Builds custom images and runs an environment with redpanda, 2 producers and 1 event processor)


.PHONY: local-docker-compose-spinup
local-docker-compose-spinup:
	docker-compose -f ./docker/docker-compose-local-dev.yml up -d

.PHONY: local-docker-compose-teardown
local-docker-compose-teardown:
	docker-compose -f ./docker/docker-compose-local-dev.yml down

.PHONY: local-docker-compose-panda-spinup
local-docker-compose-panda-spinup:
	docker-compose -f ./docker/docker-compose-redpanda-local-dev.yml up -d

.PHONY: local-docker-compose-panda-teardown
local-docker-compose-panda-teardown:
	docker-compose -f ./docker/docker-compose-redpanda-local-dev.yml down

.PHONY: local-docker-compose-pulsar-spinup
local-docker-compose-pulsar-spinup:
	docker-compose -f ./docker/docker-compose-pulsar-local-dev.yml up -d

.PHONY: local-docker-compose-pulsar-teardown
local-docker-compose-pulsar-teardown:
	docker-compose -f ./docker/docker-compose-pulsar-local-dev.yml down

.PHONY: local-setup
local-setup:
	pip install -r requirements-development.txt
	pip install -r requirements.txt

.PHONY: production-setup
production-setup:
	pip install -r requirements.txt

.PHONY: local-register-schemas
local-register-schemas:
	./schemas/avro_schemas/register-schema.sh ./schemas/avro_schemas/producer-to-processor.avsc http://localhost:8081/subjects/producer.to.processor-value/versions
	./schemas/avro_schemas/register-schema.sh ./schemas/avro_schemas/processor-to-consumer.avsc http://localhost:8081/subjects/processor.to.consumer-value/versions

.PHONY: dockerized-register-schemas
dockerized-register-schemas:
	./schemas/avro_schemas/register-schema.sh ./schemas/avro_schemas/producer-to-processor.avsc ${SCHEMA_REGISTRY_URL}/subjects/producer.to.processor-value/versions
	./schemas/avro_schemas/register-schema.sh ./schemas/avro_schemas/processor-to-consumer.avsc ${SCHEMA_REGISTRY_URL}/subjects/processor.to.consumer-value/versions

.PHONY: build-docker-custom-images
build-docker-custom-images:
	docker build -t custom-producer -f docker/producer/Dockerfile .
	docker build -t custom-kafka-resource-creator -f docker/kafka_resource_creator/Dockerfile .

.PHONY: run-dockerized-environment
run-dockerized-environment: build-docker-custom-images
	docker-compose -f docker/docker-compose-environment.yml up

.PHONY: run-dockerized-environment-redpanda
run-dockerized-environment-redpanda: build-docker-custom-images
	docker-compose -f docker/docker-compose-environment-redpanda.yml up

.PHONY: local-generate-classes-from-avro-schemas
local-generate-classes-from-avro-schemas:
	avro-to-python schemas/avro_schemas/processor-to-consumer.avsc schemas/avro_auto_generated_classes/
	avro-to-python schemas/avro_schemas/producer-to-processor.avsc schemas/avro_auto_generated_classes/
	sed -i 's/from helpers/from schemas.avro_auto_generated_classes.helpers/g' schemas/avro_auto_generated_classes/service_messages/ProcessorToConsumer.py
	sed -i 's/from helpers/from schemas.avro_auto_generated_classes.helpers/g' schemas/avro_auto_generated_classes/service_messages/ProducerToProcessor.py
	sed -i 's/from service_messages.ProducerToProcessor import ProducerToProcessor//g' schemas/avro_auto_generated_classes/service_messages/__init__.py

.PHONY: test
test:
	coverage run -m pytest

.PHONY: lint
lint:
	black --line-length 120 .
