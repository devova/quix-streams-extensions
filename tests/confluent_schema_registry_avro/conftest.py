import json

import pytest
import responses
from confluent_kafka.schema_registry import SchemaRegistryClient

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


#
# @pytest.fixture
# def mock_finished_transaction_serializer_requests(mocked_responses, overridden_kafka_settings):
#     mocked_responses.post(
#         f"{overridden_kafka_settings.schema_registry.url}/subjects/{FINISHED_TRANSACTIONS_TOPIC}-value?normalize=False",
#         status=200,
#         json={
#             "id": TOPIC_TO_ID_MAP[FINISHED_TRANSACTIONS_FILE_NAME],
#             "schema": get_avro_schema(FINISHED_TRANSACTIONS_FILE_NAME),
#             "subject": FINISHED_TRANSACTIONS_TOPIC,
#             "version": 1,
#         },
#     )
#
#     mocked_responses.get(
#         f"{overridden_kafka_settings.schema_registry.url}/schemas/ids/{TOPIC_TO_ID_MAP[FINISHED_TRANSACTIONS_FILE_NAME]}",
#         status=200,
#         json={
#             "schema": get_avro_schema("finished_transactions"),
#         },
#     )
