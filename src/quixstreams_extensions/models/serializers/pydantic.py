from quixstreams.models import BytesDeserializer, JSONSerializer

from ..chains import json, pydantic


class PydanticJSONSerializer(pydantic.ToDict, JSONSerializer):
    ...


class PydanticJSONDeserializer(json.ToDict, pydantic.FromDict, BytesDeserializer):
    ...
