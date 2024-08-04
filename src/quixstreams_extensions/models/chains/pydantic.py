from typing import Any, Dict, Optional, Type, Union

from pydantic import BaseModel
from quixstreams.models import SerializationContext

from quixstreams_extensions.models.core import Chainable


class FromDict(Chainable):
    def __init__(
        self,
        model_class: Type[BaseModel],
        *args,
        strict: Optional[bool] = None,
        from_attributes: Optional[bool] = None,
        context: Union[Dict[str, Any], None] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._model_class = model_class
        self._validation_kwargs = {
            "strict": strict,
            "from_attributes": from_attributes,
            "context": context,
        }

    def __call__(self, value: dict, ctx: SerializationContext) -> BaseModel:
        return super(FromDict, self).__call__(self._model_class.model_validate(value, **self._validation_kwargs), ctx)


class ToDict(Chainable):
    def __call__(self, value: BaseModel, ctx: SerializationContext) -> dict:
        return super(ToDict, self).__call__(value.model_dump(mode="json"), ctx)
