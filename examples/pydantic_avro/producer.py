import json
from typing import Type

from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic_avro import AvroBase
from quixstreams import Application

from quixstreams_extensions.models.chains import pydantic
from quixstreams_extensions.models.serializers.confluent_schema_registry.avro import AVROSerializer

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="pydantic_avro")

# Configure the Schema Registry client
schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})


class PydanticAVROSerializer(pydantic.ToDict, AVROSerializer):
    """
    Takes Pydantic model and convert into AVRO, to be ready for publishing
    """

    def __init__(self, schema_registry_client: SchemaRegistryClient, model_class: Type[AvroBase]):
        super().__init__(schema_registry_client, json.dumps(model_class.avro_schema()), model_class)


class User(AvroBase):
    age: int
    name: str


messages_topic = app.topic(
    "input",
    value_serializer=PydanticAVROSerializer(schema_registry_client, User),
)

messages = [
    User(age=4, name="Volodymyr"),
    User(age=23, name="Sneha"),
    User(age=84, name="John"),
    User(age=13, name="Rin"),
    User(age=104, name="Abdullah"),
]


def main():
    with app.get_producer() as producer:
        for message in messages:
            kafka_msg = messages_topic.serialize(value=message)

            # Produce chat message to the topic
            print(f'Produce event with key="{kafka_msg.key}" value="{kafka_msg.value}"')
            producer.produce(
                topic=messages_topic.name,
                value=kafka_msg.value,
            )


if __name__ == "__main__":
    main()
