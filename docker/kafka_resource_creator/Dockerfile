FROM python:3.9.2
ARG SCHEMA_REGISTRY_URL
RUN apt-get update && apt-get install jq -y
COPY schemas/ /usr/src/app/schemas
COPY Makefile /usr/src/app/
WORKDIR /usr/src/app/
CMD ["sh", "-c", "make dockerized-register-schemas"]
