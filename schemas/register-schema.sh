#!/bin/sh

jq '. | {schema: tojson}' $1 | \
curl -X POST http://localhost:8081/subjects/producer.to.processor-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-
