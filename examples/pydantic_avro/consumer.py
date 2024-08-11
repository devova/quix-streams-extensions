from typing import Literal

from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic_avro import AvroBase
from quixstreams import Application
from quixstreams.models import Deserializer, Serializer

from quixstreams_extensions.serializers.composer import composed
from quixstreams_extensions.serializers.compositions import pydantic, confluent

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="pydantic_avro")

# Configure the Schema Registry client
schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})


class User(AvroBase):
    age: int


class EnhancedUser(AvroBase):
    age: int
    prefer: Literal["quix-streaming", "sleeping", "hiking"]


# Define the input topic
input = app.topic(
    "input",
    value_deserializer=composed(
        Deserializer, confluent.to_dict(schema_registry_client), pydantic.to_instance_of(User)
    ),  # Takes AVRO payload and returns a pydantic model (may fail during pydantic validation)
)

# Define the output topics
output = app.topic(
    "output",
    value_serializer=composed(
        Serializer,
        pydantic.to_dict,
        confluent.to_avro(
            schema_registry_client, EnhancedUser.avro_schema()
        ),  # Takes Pydantic model and convert into AVRO, to be ready for publishing
    ),
)


def adults_only(user: User):
    return user.age > 18


def enhance(user: User) -> EnhancedUser:
    return EnhancedUser(age=user.age, prefer="quix-streaming" if user.age < 99 else "sleeping")


sdf = app.dataframe(input)
sdf = sdf.filter(adults_only)
sdf = sdf.apply(enhance)
sdf = sdf.print()
sdf = sdf.to_topic(output)

if __name__ == "__main__":
    app.run(sdf)
