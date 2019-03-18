import abc
import asyncio
import logging
from enum import Enum
from typing import Dict

from aioflow.helpers import cached_property

try:
    import ujson as json
except ImportError:
    import json

__author__ = "a.lemets"

logger = logging.getLogger(__name__)


class AioFlowBadStatus(RuntimeError):
    """Service not finished"""


class ServiceStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class Service:
    def __init__(self, pipeline: "Pipeline"):
        self._id = None
        self._pipeline = pipeline
        self.number = None

        self.allow_failure = self.config.get("allow_failure", False)
        self.timeout = self.config.get("timeout", None)

        # service instance
        self.status = ServiceStatus.PENDING
        self._result = None
        self._loop = asyncio.get_event_loop()

    async def message(self, *args, **kwargs):
        logger.debug(f"Send message [{self.name}]")
        return await self._pipeline._call_middleware("service_message", self, **kwargs)

    @cached_property
    def config(self):
        _config = {}
        _config.update(self._pipeline.config.get("__global", {}))
        _config.update(self._pipeline.config.get(self.name, {}))
        return _config

    @property
    def name(self) -> str:
        return type(self).__name__.lower()

    def __repr__(self):
        return self.name

    @property
    def id(self) -> str:
        return self._id or f"{type(self).__name__}__{id(self)}"

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

    @property
    def json_result(self):
        return json.dumps(self.result)

    @abc.abstractmethod
    async def payload(self, **kwargs) -> Dict or None:
        ...

    async def __call__(self, **kwargs) -> Dict or None:
        return await self.payload(**kwargs)


def service_deco(_payload=None, *, bind=False, payload_name="payload", base_service_cls=Service):
    def decorator(func):
        payload = func
        if not bind:
            payload = staticmethod(func)

        attrs = {
            payload_name: payload,
            '__doc__': func.__doc__,
            '__module__': func.__module__,
            '__wrapped__': func
        }
        cls = type(func.__name__, (base_service_cls,), attrs)

        return cls

    if _payload:
        return decorator(_payload)

    return decorator
