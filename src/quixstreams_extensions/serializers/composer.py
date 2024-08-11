from functools import reduce
from inspect import signature
from typing import Callable, Union, Optional, Any, TypeVar, overload, Type

from quixstreams.models import SerializationContext, Deserializer, Serializer

Composable = Union[Callable[[Any], Any], Callable[[Any, SerializationContext], Any]]
Composed = Callable[[Any, SerializationContext], Any]

T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")

S = TypeVar("S", bound=Serializer)
D = TypeVar("D", bound=Deserializer)
ST = Union[S, D]


@overload
def composed(serializer_type: Type[ST]) -> ST:
    ...


@overload
def composed(
    serializer_type: Type[ST],
    fn1: Union[Callable[[T1], T2], Callable[[T1, SerializationContext], T2]],
) -> ST:
    ...


@overload
def composed(
    serializer_type: Type[ST],
    fn1: Union[Callable[[T1], T2], Callable[[T1, SerializationContext], T2]],
    fn2: Union[Callable[[T2], T3], Callable[[T2, SerializationContext], T3]],
) -> ST:
    ...


@overload
def composed(
    serializer_type: Type[ST],
    fn1: Union[Callable[[T1], T2], Callable[[T1, SerializationContext], T2]],
    fn2: Union[Callable[[T2], T3], Callable[[T2, SerializationContext], T3]],
    fn3: Union[Callable[[T3], T4], Callable[[T3, SerializationContext], T4]],
) -> ST:
    ...


@overload
def composed(
    serializer_type: Type[ST],
    fn1: Union[Callable[[T1], T2], Callable[[T1, SerializationContext], T2]],
    fn2: Union[Callable[[T2], T3], Callable[[T2, SerializationContext], T3]],
    fn3: Union[Callable[[T3], T4], Callable[[T3, SerializationContext], T4]],
    fn4: Union[Callable[[T4], T5], Callable[[T4, SerializationContext], T5]],
) -> ST:
    ...


@overload
def composed(
    serializer_type: Type[ST],
    fn1: Union[Callable[[T1], T2], Callable[[T1, SerializationContext], T2]],
    fn2: Union[Callable[[T2], T3], Callable[[T2, SerializationContext], T3]],
    fn3: Union[Callable[[T3], T4], Callable[[T3, SerializationContext], T4]],
    fn4: Union[Callable[[T4], T5], Callable[[T4, SerializationContext], T5]],
    fn5: Union[Callable[[T5], T6], Callable[[T5, SerializationContext], T6]],
) -> ST:
    ...


@overload
def composed(
    serializer_type: Type[ST],
    fn1: Union[Callable[[T1], Any], Callable[[T1, SerializationContext], Any]],
    *,
    last: Union[Callable[[Any], T2], Callable[[Any, SerializationContext], T2]],
) -> Union[Callable[[T1], T2], Callable[[T1, SerializationContext], T2]]:
    ...


def composed(serializer_type: Type[ST], *functions: Composable) -> ST:
    """
    Compose multiple functions into a composed Serializer. Provides IO type checks across the chain.
    :param serializer_type: Should be either `Serializer` or `Deserializer` type.
    :param functions: A series of composed callables which will be called sequentially to achieve a final result.

    **Example**:

    `composed(Deserializer, orjson.loads, pydantic.to_instance_of(Model))` --
    Converts bytes into dict and then into pydantic object.
    """

    class ComposedSerializer(serializer_type):
        def __call__(self, value: Any, ctx: Optional[SerializationContext] = None) -> Any:
            def apply(acc: T1, f: Composable) -> T2:
                sig = signature(f)
                if len(sig.parameters) == 1:
                    return f(acc)
                elif len(sig.parameters) == 2:
                    return f(acc, ctx)
                else:
                    raise ValueError(f"Function {f} has an unexpected number of parameters")

            return reduce(apply, functions, value)

    return ComposedSerializer()
