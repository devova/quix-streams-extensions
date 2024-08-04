from quixstreams.models import BytesDeserializer, BytesSerializer

from quixstreams_extensions.models.chains.confluent_schema_registry import avro


class AVROSerializer(avro.FromDict, BytesSerializer):
    ...


class AVRODeserializer(avro.ToDict, BytesDeserializer):
    ...
