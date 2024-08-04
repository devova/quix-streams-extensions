from typing import Any, Callable, Iterable, Mapping, Union

from confluent_kafka.serialization import SerializationContext
from quixstreams.models import SerializationError
from quixstreams.utils import json

from quixstreams_extensions.models.core import Chainable


class ToDict(Chainable):
    def __init__(
        self,
        *args,
        loads: Callable[[Union[bytes, bytearray]], Any] = json.loads,
        **kwargs,
    ):
        """
        Parses data from JSON
        :param loads: function to parse json from bytes.
            Default - :py:func:`quixstreams.utils.json.loads`.
        """
        super().__init__(*args, **kwargs)
        self._loads = loads

    def __call__(self, value: bytes, ctx: SerializationContext) -> Union[Iterable[Mapping], Mapping]:
        try:
            return super(ToDict, self).__call__(self._loads(value), ctx)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc


class FromDict(Chainable):
    def __init__(
        self,
        *args,
        dumps: Callable[[Any], Union[str, bytes]] = json.dumps,
        **kwargs,
    ):
        """
        Returns data in json format.
        :param dumps: a function to serialize objects to json.
            Default - :py:func:`quixstreams.utils.json.dumps`
        """
        super().__init__(*args, **kwargs)
        self._dumps = dumps

    def __call__(self, value: Any, ctx: SerializationContext) -> Union[str, bytes]:
        try:
            return super(FromDict, self).__call__(self._dumps(value), ctx)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc
