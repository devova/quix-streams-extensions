from typing import Callable, Any, Optional, Union

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField
from orjson import orjson
from quixstreams.models import SerializationContext


def to_avro(
    schema_registry_client: SchemaRegistryClient, writer_schema: Union[dict, str, Schema], conf: Optional[dict] = None
) -> Callable[[dict[str, Any], SerializationContext], bytes]:
    """
    A factory wrapper around `confluent_kafka.schema_registry.avro.AvroSerializer`
    """
    if isinstance(writer_schema, dict):
        writer_schema = orjson.dumps(writer_schema).decode("utf-8")
    serializer = AvroSerializer(schema_registry_client, writer_schema, conf=conf)

    def wrapper(data: dict[str, Any], ctx: SerializationContext) -> bytes:
        return serializer(data, ctx.to_confluent_ctx(MessageField.VALUE))

    return wrapper


def to_dict(
    schema_registry_client: SchemaRegistryClient, reader_schema: Optional[str] = None
) -> Callable[[bytes, SerializationContext], dict[str, Any]]:
    """
    A factory wrapper around `confluent_kafka.schema_registry.avro.AvroDeserializer`
    """
    deserializer = AvroDeserializer(schema_registry_client, reader_schema)

    def wrapper(data: bytes, ctx: SerializationContext) -> dict[str, Any]:
        return deserializer(data, ctx.to_confluent_ctx(MessageField.VALUE))

    return wrapper
