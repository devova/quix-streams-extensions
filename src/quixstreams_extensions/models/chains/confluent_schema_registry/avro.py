from typing import Optional

from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField
from quixstreams.models import SerializationContext

from quixstreams_extensions.models.core import Chainable


class FromDict(Chainable):
    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        writer_schema_str: str,
        *args,
        conf: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._serializer = AvroSerializer(schema_registry_client, writer_schema_str, conf=conf)

    def __call__(self, value: dict, ctx: SerializationContext) -> bytes:
        return super(FromDict, self).__call__(
            self._serializer(value, ctx.to_confluent_ctx(MessageField.VALUE)),
            ctx,
        )


class ToDict(Chainable):
    def __init__(
        self, schema_registry_client: SchemaRegistryClient, *args, reader_schema_str: Optional[str] = None, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._deserializer = AvroDeserializer(schema_registry_client, schema_str=reader_schema_str)

    def __call__(self, value: bytes, ctx: SerializationContext) -> dict:
        return super(ToDict, self).__call__(
            self._deserializer(value, ctx.to_confluent_ctx(MessageField.VALUE)),
            ctx,
        )
