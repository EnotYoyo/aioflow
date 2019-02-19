import abc
import asyncio
import logging
from enum import Enum, auto
from typing import Dict
from functools import update_wrapper

__author__ = "a.lemets"

logger = logging.getLogger(__name__)


class AioFlowBadStatus(RuntimeError):
    """Service not finished"""


def service_payload(func):
    async def wrapper(self: "Service", **kwargs):
        logger.debug(f"Start [{self.name}] payload with {kwargs}")

        self.status = ServiceStatus.PROCESSING
        self.number = kwargs.pop("__service_number", None)
        try:
            result = await func(self, **kwargs)
        except Exception as e:
            logger.exception(f"Failed [{self.name}]")
            self.status = ServiceStatus.FAILED
            if not self.allow_failure:
                raise
        else:
            logger.debug(f"Success [{self.name}] with {result}")
            self.result = result
            return result

    return update_wrapper(wrapper, func)


class ServiceStatus(Enum):
    PENDING = auto()
    PROCESSING = auto()
    DONE = auto()
    FAILED = auto()


class Service:
    def __init__(self,
                 pipeline: "Pipeline",
                 *,
                 result_of: Dict = None,
                 allow_failure: bool = False,
                 timeout: int = None):
        self.pipeline = pipeline
        self.pipeline.register_service(self, result_of=result_of)
        self.number = None

        self.allow_failure = allow_failure
        self.timeout = timeout

        # service instance
        self.status = ServiceStatus.PENDING
        self._result = None
        self._loop = asyncio.get_event_loop()

    async def message(self, *args, **kwargs):
        logger.debug(f"Send message [{self.name}]")
        kwargs.update({"__service_name": self.name})
        await self.pipeline.message(*args, **kwargs)

    @property
    def config(self):
        return self.pipeline.config.get(self.name, {})

    @property
    def name(self) -> str:
        return type(self).__name__.lower()

    def __repr__(self):
        return self.name

    @property
    def id(self) -> str:
        return f"{type(self).__name__}__{id(self)}"

    @property
    def is_finished(self) -> bool:
        return self.status is ServiceStatus.DONE or self.status is ServiceStatus.FAILED

    @property
    def loop(self):
        return self._loop

    @property
    def result(self):
        if not self.is_finished:
            logger.debug(f"Service [{self.name}] is {self.status}")
            raise AioFlowBadStatus("Service instance is not done")
        return self._result

    @result.setter
    def result(self, value):
        logger.debug(f"Set service [{self.name}] result {value}")
        self.status = ServiceStatus.DONE
        self._result = value

    @abc.abstractmethod
    async def payload(self, **kwargs) -> Dict or None:
        ...

    @service_payload
    async def __call__(self, **kwargs) -> Dict or None:
        return await self.payload(**kwargs)
