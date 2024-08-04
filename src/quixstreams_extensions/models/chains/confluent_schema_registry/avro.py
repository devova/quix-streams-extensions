from typing import Dict

from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField
from quixstreams.models import SerializationContext

from quixstreams_extensions.models.core import Chainable


class FromDict(Chainable):
    def __init__(self, schema_registry_client: SchemaRegistryClient, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._schema_registry_client = schema_registry_client
        self._serializers: Dict[str, AvroSerializer] = {}

    def __call__(self, value: dict, ctx: SerializationContext) -> bytes:
        confluent_ctx = ctx.to_confluent_ctx(MessageField.VALUE)
        schema_name = topic_subject_name_strategy(confluent_ctx, ctx.topic)
        if schema_name not in self._serializers:
            schema = self._schema_registry_client.get_latest_version(schema_name)
            self._serializers[schema_name] = AvroSerializer(self._schema_registry_client, schema.schema)
        return super(FromDict, self).__call__(
            self._serializers[schema_name](value, confluent_ctx),
            ctx,
        )


class ToDict(Chainable):
    def __init__(self, schema_registry_client: SchemaRegistryClient, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._deserializer = AvroDeserializer(schema_registry_client)

    def __call__(self, value: bytes, ctx: SerializationContext) -> dict:
        return super(ToDict, self).__call__(
            self._deserializer(value, ctx.to_confluent_ctx(MessageField.VALUE)),
            ctx,
        )
