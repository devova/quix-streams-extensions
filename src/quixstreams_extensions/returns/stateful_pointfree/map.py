from typing import Callable, TypeVar

from quixstreams import State
from returns.curry import partial
from returns.interfaces.mappable import MappableN
from returns.primitives.hkt import Kinded, KindN, kinded

_FirstType = TypeVar("_FirstType")
_SecondType = TypeVar("_SecondType")
_ThirdType = TypeVar("_ThirdType")
_UpdatedType = TypeVar("_UpdatedType")

_MappableKind = TypeVar("_MappableKind", bound=MappableN)


def map_(
    function: Callable[[_FirstType, State], _UpdatedType],
) -> Kinded[
    Callable[
        [KindN[_MappableKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_MappableKind, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Lifts function to be wrapped in a container for better composition.

    In other words, it modifies the function's
    signature from:
    ``a, state: State -> b, state: State``
    to: ``Container[a], state: State -> Container[b], state: State``

    This is how it should be used:

    .. code:: python

        >>> from returns.io import IO
        >>> from quixstreams_extensions.returns.stateful_pointfree import map_
        >>> from quixstreams import State

        >>> def example(argument: int, state: State) -> float:
        ...     return argument / 2

        >>> assert map_(example)(IO(1)) == IO(0.5)

    Note, that this function works for all containers with ``.map`` method.
    See :class:`returns.primitives.interfaces.mappable.MappableN` for more info.

    See also:
        - https://wiki.haskell.org/Lifting

    """

    @kinded
    def factory(
        container: KindN[_MappableKind, _FirstType, _SecondType, _ThirdType],
        state: State,
    ) -> KindN[_MappableKind, _UpdatedType, _SecondType, _ThirdType]:
        return container.map(partial(function, state=state))

    return factory
