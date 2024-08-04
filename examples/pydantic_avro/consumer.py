import json
from typing import Type, Literal

from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic_avro import AvroBase
from quixstreams import Application
from quixstreams.models import BytesDeserializer

from quixstreams_extensions.models.chains import loggers, pydantic
from quixstreams_extensions.models.chains.confluent_schema_registry import avro
from quixstreams_extensions.models.serializers.confluent_schema_registry.avro import AVROSerializer

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="pydantic_avro")

# Configure the Schema Registry client
schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})


class User(AvroBase):
    age: int


class EnhancedUser(AvroBase):
    age: int
    prefer: Literal["quix-streaming", "sleeping", "hiking"]


class AVROPydanticDeserializer(avro.ToDict, pydantic.FromDict, BytesDeserializer):
    """
    Takes AVRO payload form input topic and returns a pydantic model (may fail during pydantic validation)
    """


class PydanticAVROSerializer(pydantic.ToDict, loggers.Logged, AVROSerializer):
    """
    Takes Pydantic model and convert into AVRO, to be ready for publishing
    """

    def __init__(self, schema_registry_client: SchemaRegistryClient, model_class: Type[AvroBase]):
        super().__init__(schema_registry_client, json.dumps(model_class.avro_schema()), model_class)


# Define the input topic
input = app.topic(
    "input",
    value_deserializer=AVROPydanticDeserializer(schema_registry_client, User),
)

# Define the output topics
output = app.topic(
    "output",
    value_serializer=PydanticAVROSerializer(schema_registry_client, EnhancedUser),
)


def adults_only(user: User):
    return user.age > 18


def enhance(user: User):
    return EnhancedUser(age=user.age, prefer="quix-streaming" if user.age < 99 else "sleeping")


sdf = app.dataframe(input)
sdf = sdf.filter(adults_only)
sdf = sdf.apply(enhance)
sdf = sdf.to_topic(output)

if __name__ == "__main__":
    app.run(sdf)
