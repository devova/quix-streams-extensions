import json

import pytest
import responses
from confluent_kafka.schema_registry import SchemaRegistryClient

from quixstreams_extensions.serializers.compositions import confluent


SCHEMA_REGISTRY_URL = "http://schema-registry.url"


SCHEMA_ID = 1


@pytest.fixture
def subject(topic):
    return f"{topic}-value"


@pytest.fixture
def schema_registry_client():
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


@pytest.fixture
def schema():
    return json.dumps(
        {
            "type": "record",
            "name": "ExampleRecord",
            "fields": [
                {
                    "name": "it",
                    "type": {"type": "enum", "name": "ItEnum", "symbols": ["works"]},
                }
            ],
        }
    )


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@pytest.fixture
def mocked_schema_registry_during_serialization(mocked_responses, subject, schema):
    mocked_responses.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest",
        status=200,
        json={
            "id": SCHEMA_ID,
            "schema": schema,
            "subject": subject,
            "version": 1,
        },
    )
    mocked_responses.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions?normalize=False",
        status=200,
        json={
            "id": SCHEMA_ID,
            "schema": schema,
            "subject": subject,
            "version": 1,
        },
    )


@pytest.fixture
def mocked_schema_registry_during_deserialization(mocked_responses, subject, schema):
    mocked_responses.get(
        f"{SCHEMA_REGISTRY_URL}/schemas/ids/{SCHEMA_ID}",
        status=200,
        json={
            "id": SCHEMA_ID,
            "schema": schema,
            "subject": subject,
            "version": 1,
        },
    )


def test_to_avro(mocked_schema_registry_during_serialization, schema_registry_client, schema, ctx):
    serializer = confluent.to_avro(schema_registry_client, schema)
    assert serializer({"it": "works"}, ctx) == b"\x00\x00\x00\x00\x01\x00"


def test_to_dict(mocked_schema_registry_during_deserialization, schema_registry_client, ctx):
    serializer = confluent.to_dict(schema_registry_client)
    assert serializer(b"\x00\x00\x00\x00\x01\x00", ctx) == {"it": "works"}
