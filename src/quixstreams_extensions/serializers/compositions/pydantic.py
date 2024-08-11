from dataclasses import asdict, is_dataclass
from typing import Type, TypeVar, Any, Callable, get_origin, Optional, Union, Protocol

from pydantic import BaseModel, TypeAdapter
from quixstreams.models import SerializationContext

T = TypeVar("T")


class _DataclassProtocol(Protocol):
    __dataclass_fields__: dict


def _as_own_context(ctx: Optional[SerializationContext]) -> Optional[dict[str, Any]]:
    if ctx is not None:
        return {
            "topic": ctx.topic,
            "headers": ctx.headers,
        }


def to_instance_of(model_class: Type[T]) -> Callable[[dict, Optional[SerializationContext]], T]:
    """
    Tries to map an input dict to an instance of a given type. Works well with Pydantic models, Dataclasses, Unions.
    May raise `ValidationError` If the object could not be validated.
    :param model_class: A type
    :return: Instance of `model_class`
    :raises: ValidationError: If the object could not be validated.
    """
    if get_origin(model_class) is not None or not issubclass(model_class, BaseModel):
        model_class = TypeAdapter[T](model_class)

    def validate(data: dict, ctx: Optional[SerializationContext] = None) -> T:
        if isinstance(model_class, TypeAdapter):
            return model_class.validate_python(data, context=_as_own_context(ctx))
        else:
            return model_class.model_validate(data)

    return validate


def to_dict(obj: Union[BaseModel, _DataclassProtocol], ctx: Optional[SerializationContext] = None) -> dict[str, Any]:
    """
    Converts a Pydantic or Dataclass instance to a dictionary.
    """
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json", by_alias=True, context=_as_own_context(ctx))
    elif is_dataclass(obj):
        return asdict(obj)
    else:
        return dict(obj)


def to_json(obj: BaseModel, ctx: Optional[SerializationContext] = None) -> bytes:
    """
    Converts a Pydantic instance to json string. A shortcut to `composed(..., to_dict, orjson.dumps)`
    """
    return obj.model_dump_json(by_alias=True, context=_as_own_context(ctx)).encode()
