#!/bin/sh

jq '. | {schema: tojson}' $1 | \
curl -X POST $2 \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-
