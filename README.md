# QuixStreams Extensions

Composable Serializers and non-official Sinks.

It is an extension for the popular [QuixStreams](https://quix.io/docs/get-started/welcome.html) package, 
providing enhanced functionality that doesn't suit the main stream branch. 

## Key Features

### Chainable Serializers
Easily chain different types of serializers to each other. Including:

  - **Pydantic Serializers**: Converts back-and-forth Pydantic models and dataclasses. Helps writing type safe code.
  - **Confluent AVRO Serializers**: Integrate Confluent Schema Registry.

 
## Installation

To install this package, you can use pip:

```shell
pip install quixstreams-extension[avro,pydantic]
```

## Quick start
Here's an example of using composable serializers with [QuixStreams](https://quix.io/docs/get-started/welcome.html):

Imagine you want to write a type safe code and forget asking your self "what was the input schema? let me check examples or logs...".
Also, you want to process a topic that contains [AVRO](https://avro.apache.org/) messages serialised with a help of [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

So first let’s define our input topic schema, as pydantic model:
```python
from pydantic import BaseModel

class User(BaseModel):
    age: int
```

Now, let's define an input topic with its deserializer:

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from quixstreams.models import Deserializer
from quixstreams_extensions.serializers.composer import composed
from quixstreams_extensions.serializers.compositions import pydantic, confluent

# Configure the Schema Registry client
schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})

composed_deserializer = (
    composed(Deserializer, confluent.to_dict(schema_registry_client), pydantic.to_instance_of(User)),
)  # Takes AVRO payload and returns a pydantic model (may fail during pydantic validation)

input = app.topic("input", value_deserializer=composed_deserializer)
```
Take a look closer to `composed_deserializer`. The main entry point is `composed(SerialiserClass, *functions)` function, 
which accept either base `quixstreams.models.Deserializer` class or `quixstreams.models.Serializer` class (subclasses are allowed), 
then it accept a series of composed callable which will be called sequentially to achieve a final result. As we can see in our example above it:
- creates composable Deserializer, which first
- take AVRO payload and convert it to python dictionary, with the help of `SchemaRegistryClient`, then it
- take python dict and convert it to `User` instance, which now can to used in pipeline.

Now we can use them in the app that defines business logic:
```python
from pydantic_avro import AvroBase


class EnhancedUser(AvroBase):  # output data model 
  age: int
  prefer: Literal["quix-streaming", "sleeping", "hiking"]


def adults_only(user: User):
    return user.age > 18


def enhance(user: User) -> EnhancedUser:
    return EnhancedUser(age=user.age, prefer="quix-streaming" if user.age < 99 else "sleeping")


sdf = app.dataframe(input)
sdf = sdf.filter(adults_only).print()
sdf = sdf.apply(enhance)
```
The pipeline has two processing functions, both of them are type safe, with help of pydantic (we could achieve the same with dataclasses of course).
The `EnhancedUser` is our output data model, inherits from `AvroBase(pydantic.BaseModel)`, it leverages [pydantic-avro](https://github.com/godatadriven/pydantic-avro) that will be useful later.

Finally let's push our enhanced data into another AVRO topic:
```python
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

sdf = sdf.to_topic(output).print()

if __name__ == "__main__":
    app.run(sdf)
```
We we've got a composed serializer that:
- `pydantic.to_dict` takes pydantic model and converts it to python dict
- `confluent.to_avro` takes the dict and converts it to AVRO with help of Confluent `SchemaRegistryClient` and generated AVRO schema by `EnhancedUser.avro_schema()`
  - by default `schema_registry_client` will try to register AVRO schema in its registry; 
    with time being and schema evolving it may crash due to migration [policy](https://docs.confluent.io/platform/current/schema-registry/index.html#compatibility-and-schema-evolution)

Please discover `examples/` folder for more information.