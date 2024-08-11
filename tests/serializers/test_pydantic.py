from dataclasses import dataclass
from typing import Literal, Union

import pytest
from pydantic import BaseModel
from quixstreams.models import Serializer

from quixstreams_extensions.serializers.composer import composed
from quixstreams_extensions.serializers.compositions import pydantic


class Model(BaseModel):
    it: Literal["works"]


@dataclass
class DataClass:
    it: Literal["also works"]


@pytest.mark.parametrize(
    "model_class, payload, expected",
    (
        (Model, {"it": "works"}, Model(it="works")),
        (DataClass, {"it": "also works"}, DataClass(it="also works")),
        (Union[Model, DataClass], {"it": "works"}, Model(it="works")),
        (Union[Model, DataClass], {"it": "also works"}, DataClass(it="also works")),
    ),
)
def test_to_instance_of(model_class, payload, expected):
    serializer = pydantic.to_instance_of(model_class)
    assert serializer(payload) == expected


@pytest.mark.parametrize(
    "obj, expected",
    (
        (Model(it="works"), {"it": "works"}),
        (DataClass(it="also works"), {"it": "also works"}),
    ),
)
def test_to_dict(obj, expected):
    assert pydantic.to_dict(obj) == expected


def test_chain():
    serializer = composed(Serializer, pydantic.to_instance_of(Model), pydantic.to_dict)
    assert serializer({"it": "works"}) == {"it": "works"}
