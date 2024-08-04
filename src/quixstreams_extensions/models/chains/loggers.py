import logging
from typing import Any

from confluent_kafka.serialization import SerializationContext

from quixstreams_extensions.models.core import Chainable


class Printed(Chainable):
    def __call__(self, value: Any, ctx: SerializationContext) -> Any:
        print(value)
        return super(Printed, self).__call__(value, ctx)


default_logger = logging.getLogger("quixstreams")


class Logged(Chainable):
    def __init__(self, *args, logger: logging.Logger = default_logger, log_level: int = logging.INFO, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger
        self._log_level = log_level

    def __call__(self, value: Any, ctx: SerializationContext) -> Any:
        self._logger.log(self._log_level, value)
        return super(Logged, self).__call__(value, ctx)
