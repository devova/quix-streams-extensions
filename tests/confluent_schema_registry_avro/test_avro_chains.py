from quixstreams.models import BytesSerializer

from quixstreams_extensions.models.chains.confluent_schema_registry import avro


def test_from(mocked_schema_registry_during_serialization, schema_registry_client, ctx):
    class From(avro.FromDict, BytesSerializer):
        ...

    serializer = From(schema_registry_client)
    assert serializer({"it": "works"}, ctx) == b"\x00\x00\x00\x00\x01\x00"


def test_to(mocked_schema_registry_during_deserialization, schema_registry_client, ctx):
    class To(avro.ToDict, BytesSerializer):
        ...

    serializer = To(schema_registry_client)
    assert serializer(b"\x00\x00\x00\x00\x01\x00", ctx) == {"it": "works"}


def test_from_to(mocked_schema_registry_during_serialization, mocked_schema_registry_during_deserialization, schema_registry_client, ctx):
    class FromTo(avro.FromDict, avro.ToDict, BytesSerializer):
        ...

    serializer = FromTo(schema_registry_client, schema_registry_client)
    assert serializer({"it": "works"}, ctx) == {"it": "works"}
