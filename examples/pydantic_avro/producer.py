from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic_avro import AvroBase
from quixstreams import Application
from quixstreams.models import Serializer

from quixstreams_extensions.serializers.composer import composed
from quixstreams_extensions.serializers.compositions import confluent, pydantic

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="pydantic_avro")

# Configure the Schema Registry client
schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})


class User(AvroBase):
    age: int
    name: str


messages_topic = app.topic(
    "input",
    value_serializer=composed(
        Serializer, pydantic.to_dict, confluent.to_avro(schema_registry_client, User.avro_schema())
    ),
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
