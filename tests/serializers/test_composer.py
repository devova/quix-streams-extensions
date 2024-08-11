from functools import partial

import pytest
from typing import List, Tuple

from quixstreams.models import SerializationContext, Serializer

from quixstreams_extensions.serializers.composer import composed as original_composed

composed = partial(original_composed, Serializer)


# Type alias for headers
MessageHeadersTuples = List[Tuple[str, bytes]]


# Test functions
def add_one(x: int) -> int:
    return x + 1


def multiply_by_two(x: int) -> int:
    return x * 2


def add_topic_length(x: int, context: SerializationContext) -> int:
    return x + len(context.topic)


def test_compose_single_arg_functions():
    serializer = composed(add_one, multiply_by_two, add_one)
    assert serializer(3) == 9


def test_compose_two_arg_functions():
    context = SerializationContext("test_topic")
    serializer = composed(add_topic_length, add_topic_length)
    assert serializer(3, context) == 23  # 3 + 10 + 10 (len("test_topic") is 10)


def test_compose_mixed_functions():
    context = SerializationContext("example")
    serializer = composed(add_one, add_topic_length, multiply_by_two)
    assert serializer(3, context) == 22  # (3 + 1 + 7) * 2


def test_compose_no_functions():
    serializer = composed()
    assert serializer(5) == 5
    context = SerializationContext("topic")
    assert serializer(5, context) == 5


def test_compose_single_function():
    serializer = composed(add_one)
    assert serializer(5) == 6


def test_compose_with_lambda():
    serializer = composed(lambda x: x * 3, add_one)
    assert serializer(2) == 7


def test_compose_error_wrong_arity():
    def three_args(x: int, y: int, z: int) -> int:
        return x + y + z

    serializer = composed(add_one, three_args)
    with pytest.raises(ValueError):
        serializer(5)


def test_compose_type_hints():
    def str_to_int(s: str) -> int:
        return len(s)

    def int_to_bool(i: int) -> bool:
        return i > 5

    serializer = composed(str_to_int, int_to_bool)
    assert serializer("hello") is False
    assert serializer("hello world") is True
