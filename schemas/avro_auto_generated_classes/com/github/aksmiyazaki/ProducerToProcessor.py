# -*- coding: utf-8 -*-

""" avro python class for file: ProducerToProcessor """

import json
from schemas.avro_auto_generated_classes.helpers import *
from typing import Union


class ProducerToProcessor(object):

    schema = """
    {
        "name": "ProducerToProcessor",
        "namespace": "com.github.aksmiyazaki",
        "type": "record",
        "doc": "Simple schema that defines messages exchanged between an event producer and an event processor.",
        "fields": [
            {
                "name": "origin_service_id",
                "type": "string",
                "doc": "A unique identifier of the service."
            },
            {
                "name": "origin_service_type",
                "type": "string",
                "doc": "Type of the referred service."
            },
            {
                "name": "destination_service_type",
                "type": "string",
                "doc": "Service type that should receive this message."
            },
            {
                "name": "payload",
                "type": "string"
            }
        ]
    }
    """

    def __init__(self, obj: Union[str, dict, 'ProducerToProcessor']) -> None:
        if isinstance(obj, str):
            obj = json.loads(obj)

        elif isinstance(obj, type(self)):
            obj = obj.__dict__

        elif not isinstance(obj, dict):
            raise TypeError(
                f"{type(obj)} is not in ('str', 'dict', 'ProducerToProcessor')"
            )

        self.set_origin_service_id(obj.get('origin_service_id', None))

        self.set_origin_service_type(obj.get('origin_service_type', None))

        self.set_destination_service_type(obj.get('destination_service_type', None))

        self.set_payload(obj.get('payload', None))

    def dict(self):
        return todict(self)

    def set_origin_service_id(self, value: str) -> None:

        if isinstance(value, str):
            self.origin_service_id = value
        else:
            raise TypeError("field 'origin_service_id' should be type str")

    def get_origin_service_id(self) -> str:

        return self.origin_service_id

    def set_origin_service_type(self, value: str) -> None:

        if isinstance(value, str):
            self.origin_service_type = value
        else:
            raise TypeError("field 'origin_service_type' should be type str")

    def get_origin_service_type(self) -> str:

        return self.origin_service_type

    def set_destination_service_type(self, value: str) -> None:

        if isinstance(value, str):
            self.destination_service_type = value
        else:
            raise TypeError("field 'destination_service_type' should be type str")

    def get_destination_service_type(self) -> str:

        return self.destination_service_type

    def set_payload(self, value: str) -> None:

        if isinstance(value, str):
            self.payload = value
        else:
            raise TypeError("field 'payload' should be type str")

    def get_payload(self) -> str:

        return self.payload

    def serialize(self) -> None:
        return json.dumps(self, default=default_json_serialize)
