from typing import Any, TypeVar

from confluent_kafka.serialization import SerializationContext
from returns.pipeline import is_successful
from returns.result import Failure, Result, ResultE, Success, safe

from quixstreams_extensions.models.core import Chainable


class DontPublishMessageException(Exception):
    ...


def producer_error_handler(exc, *args, **kwargs) -> bool:
    """
    Decides whether to suppress producer errors.
    """
    return isinstance(exc, DontPublishMessageException)


class Mapped(Chainable):
    """
    Reveals the successful container only.
    It is meant to be used with Application(..., on_producer_error=producer_error_handler) callback
    """

    def __call__(self, value: Result[Any, Any], ctx: SerializationContext) -> Any:
        if is_successful(value):
            return super(Mapped, self).__call__(value.unwrap(), ctx)
        else:
            raise DontPublishMessageException()


class Altered(Chainable):
    """
    Reveals the failed container only.
    It is meant to be used with Application(..., on_producer_error=producer_error_handler) callback
    """

    def __call__(self, value: Result[Any, Any], ctx: SerializationContext) -> Any:
        if not is_successful(value):
            return super(Altered, self).__call__(value.failure(), ctx)
        else:
            raise DontPublishMessageException()


class Safety(Chainable):
    """
    Converts a regular serialization pipeline that can throw exceptions to one that return Result type.
    The Failure branch will contain a raised exception.
    """

    def __call__(self, value: Any, ctx: SerializationContext) -> ResultE[Any]:
        return safe(super(Safety, self).__call__)(value, ctx)


T = TypeVar("T")


class Attempted(Chainable):
    """
    Converts a regular serialization pipeline that can throw exceptions to one that return Result type.
    The Failure branch reveals the argument of a failed attempt, which allows getting an original input message
    that failed to be serialized.
    """

    def __call__(self, value: T, ctx: SerializationContext) -> Result[Any, T]:
        try:
            return Success(super(Attempted, self).__call__(value, ctx))
        except Exception:
            return Failure(value)
