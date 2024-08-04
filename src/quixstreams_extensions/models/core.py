from typing import Any, Protocol

from quixstreams.models import SerializationContext


class Chainable(Protocol):
    def __call__(self, value: Any, ctx: SerializationContext) -> Any:
        return super().__call__(value, ctx)
