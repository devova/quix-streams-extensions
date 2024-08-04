# QuixStreams Extensions

Holds chainable serializers and utils for railway-oriented programming.

# QuixStreams Extension Package

This Python package is an extension for the popular [QuixStreams](https://quix.io/docs/get-started/welcome.html) package, 
providing enhanced functionality with chainable serializers. 
These serializers allow you to chain different types to each other seamlessly.

## Key Features

- **Chainable Serializers**: Easily chain different types of serializers to each other.
  - **Pydantic Serializers**: Chain serializers for Pydantic models.
  - **AVRO Serializers**: Integrate Confluent Schema Registry AVRO serializers into your chains.
  - **Rail-Well-Oriented Programming Serializes**: Use serializers designed for railway-oriented Programming. Based on [returns](https://returns.readthedocs.io/en/latest/index.html)

## Installation

To install this package, you can use pip:

```shell
pip install quixstreams-extension[avro,pydantic]
```

## Usage
Here's an example of how to use the chainable serializers with QuixStreams:

First let’s define our serializers:
```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic_avro import AvroBase
from quixstreams.models import (
    BytesDeserializer,
)
from quixstreams_extensions.models.serializers.confluent_schema_registry.avro import (
    AVROSerializer,
)
from quixstreams_extensions.models.chains import pydantic
from quixstreams_extensions.models.chains.confluent_schema_registry import avro



class AVROPydanticDeserializer(avro.ToDict, pydantic.FromDict, BytesDeserializer):
    """
    Takes AVRO payload form input topic and returns a pydantic model (may fail during pydantic validation)
    """


class PydanticAVROSerializer(pydantic.ToDict, AVROSerializer):
    """
    Takes Pydantic model and convert into AVRO, to be ready for publishing
    """

    def __init__(self, schema_registry_client: SchemaRegistryClient, model_class: Type[BaseModel]):
        super().__init__(schema_registry_client, json.dumps(model_class.avro_schema()), model_class)
```

Then we can use them in the app:
```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic_avro import AvroBase
from quixstreams import Application

# Create an Application - the main configuration entry point
app = Application(...)

# Configure the Schema Registry client
schema_registry_client = SchemaRegistryClient(...)

class User(AvroBase):
  age: int

class EnhancedUser(AvroBase):
  age: int
  prefer: Literal["quix-streaming", "sleeping", "hiking"]

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
sdf = sdf.filter(adults_only).print()
sdf = sdf.apply(enhance)
sdf = sdf.to_topic(output).print()

if __name__ == "__main__":
    app.run(sdf)

```
Please discover `examples/` folder for more information.