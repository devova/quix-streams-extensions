import pytest
from quixstreams.models import SerializationContext


@pytest.fixture
def topic():
    return "any-topic"


@pytest.fixture
def ctx(topic):
    return SerializationContext(topic)
