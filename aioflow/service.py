import abc
from enum import Enum, auto
from typing import Dict
from functools import update_wrapper

__author__ = "a.lemets"


class AioFlowBadStatus(RuntimeError):
    """Service not finished"""


def service_payload(func):
    async def wrapper(self: "Service", **kwargs):
        self.status = ServiceStatus.PROCESSING
        try:
            result = await func(self, **kwargs)
        except Exception:
            self.status = ServiceStatus.FAILED
            if not self.allow_failure:
                raise
        else:
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

        self.allow_failure = allow_failure
        self.timeout = timeout

        # service instance
        self.status = ServiceStatus.PENDING
        self._result = None

    async def message(self, *args, **kwargs):
        kwargs.update({"__service_name": self.name})
        await self.pipeline.message(*args, **kwargs)

    @property
    def name(self) -> str:
        return type(self).__name__

    def __repr__(self):
        return self.name

    @property
    def id(self) -> str:
        return f"{self.name}__{id(self)}"

    @property
    def is_finished(self) -> bool:
        return self.status is ServiceStatus.DONE or self.status is ServiceStatus.FAILED

    @property
    def result(self):
        if not self.is_finished:
            raise AioFlowBadStatus("Service Instance is not done")
        return self._result

    @result.setter
    def result(self, value):
        self.status = ServiceStatus.DONE
        self._result = value

    @abc.abstractmethod
    async def payload(self, **kwargs) -> Dict or None:
        ...

    @service_payload
    async def __call__(self, **kwargs) -> Dict or None:
        return await self.payload(**kwargs)
