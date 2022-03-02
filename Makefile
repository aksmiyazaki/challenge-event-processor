.PHONY: local-docker-compose-spinup
local-docker-compose:
	docker-compose -f ./docker/docker-compose.yml up -d

.PHONY: local-docker-compose-teardown
local-docker-compose-teardown:
	docker-compose -f ./docker/docker-compose.yml down

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
	./schemas/avro_schemas/register-schema.sh ./schemas/avro_schemas/producer-to-consumer.avsc http://localhost:8081/subjects/producer.to.consumer-value/versions


