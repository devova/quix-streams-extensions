from typing import Literal

from pydantic import BaseModel
from quixstreams.models import BytesSerializer
from quixstreams_extensions.models.chains import pydantic


class Model(BaseModel):
    it: Literal["works"]


def test_from(ctx):
    class From(pydantic.FromDict, BytesSerializer): ...

    serializer = From(model_class=Model)
    assert serializer({"it": "works"}, ctx) == Model(it="works")


def test_to(ctx):
    class To(pydantic.ToDict, BytesSerializer): ...

    serializer = To()
    assert serializer(Model(it="works"), ctx) == {"it": "works"}


def test_from_to(ctx):
    class FromTo(pydantic.FromDict, pydantic.ToDict, BytesSerializer): ...

    serializer = FromTo(model_class=Model)
    assert serializer({"it": "works"}, ctx) == {"it": "works"}
