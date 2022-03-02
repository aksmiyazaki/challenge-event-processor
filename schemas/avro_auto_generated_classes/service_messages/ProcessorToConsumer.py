# -*- coding: utf-8 -*-

""" avro python class for file: ProcessorToConsumer """

import json
from schemas.avro_auto_generated_classes.helpers import default_json_serialize, todict
from typing import Union


class ProcessorToConsumer(object):

    schema = """
    {
        "name": "ProcessorToConsumer",
        "namespace": "service_messages",
        "type": "record",
        "doc": "Schema that defines messages exchanged between the event processor and downstream consumers.",
        "fields": [
            {
                "name": "producer_service_id",
                "type": "string",
                "doc": "First service on the pipeline that produced this event.."
            },
            {
                "name": "processor_service_id",
                "type": "string",
                "doc": "Service Id of the processor."
            },
            {
                "name": "destination_type",
                "type": "string",
                "doc": "Service type after the event_processor that should receive this message."
            },
            {
                "name": "producer_event_timestamp",
                "type": "long",
                "logicalType": "timestamp-millis",
                "doc": "Event closest to event production. Difference with it gives you total lag of the platform."
            },
            {
                "name": "processor_event_timestamp",
                "type": "long",
                "logicalType": "timestamp-millis",
                "doc": "Event after processing and validation from event processor."
            },
            {
                "name": "payload",
                "type": "string",
                "doc": "The content of the event."
            }
        ]
    }
    """

    def __init__(self, obj: Union[str, dict, 'ProcessorToConsumer']) -> None:
        if isinstance(obj, str):
            obj = json.loads(obj)

        elif isinstance(obj, type(self)):
            obj = obj.__dict__

        elif not isinstance(obj, dict):
            raise TypeError(
                f"{type(obj)} is not in ('str', 'dict', 'ProcessorToConsumer')"
            )

        self.set_producer_service_id(obj.get('producer_service_id', None))

        self.set_processor_service_id(obj.get('processor_service_id', None))

        self.set_destination_type(obj.get('destination_type', None))

        self.set_producer_event_timestamp(obj.get('producer_event_timestamp', None))

        self.set_processor_event_timestamp(obj.get('processor_event_timestamp', None))

        self.set_payload(obj.get('payload', None))

    def dict(self):
        return todict(self)

    def set_producer_service_id(self, value: str) -> None:

        if isinstance(value, str):
            self.producer_service_id = value
        else:
            raise TypeError("field 'producer_service_id' should be type str")

    def get_producer_service_id(self) -> str:

        return self.producer_service_id

    def set_processor_service_id(self, value: str) -> None:

        if isinstance(value, str):
            self.processor_service_id = value
        else:
            raise TypeError("field 'processor_service_id' should be type str")

    def get_processor_service_id(self) -> str:

        return self.processor_service_id

    def set_destination_type(self, value: str) -> None:

        if isinstance(value, str):
            self.destination_type = value
        else:
            raise TypeError("field 'destination_type' should be type str")

    def get_destination_type(self) -> str:

        return self.destination_type

    def set_producer_event_timestamp(self, value: int) -> None:

        if isinstance(value, int):
            self.producer_event_timestamp = value
        else:
            raise TypeError("field 'producer_event_timestamp' should be type int")

    def get_producer_event_timestamp(self) -> int:

        return self.producer_event_timestamp

    def set_processor_event_timestamp(self, value: int) -> None:

        if isinstance(value, int):
            self.processor_event_timestamp = value
        else:
            raise TypeError("field 'processor_event_timestamp' should be type int")

    def get_processor_event_timestamp(self) -> int:

        return self.processor_event_timestamp

    def set_payload(self, value: str) -> None:

        if isinstance(value, str):
            self.payload = value
        else:
            raise TypeError("field 'payload' should be type str")

    def get_payload(self) -> str:

        return self.payload

    def serialize(self) -> None:
        return json.dumps(self, default=default_json_serialize)
