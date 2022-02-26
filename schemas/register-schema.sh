jq '. | {schema: tojson}' service.avsc | \
curl -X POST http://localhost:8081/subjects/service-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-

jq '. | {schema: tojson}' producer-to-processor.avsc | \
curl -X POST http://localhost:8081/subjects/producer.to.processor-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-
