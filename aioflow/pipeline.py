import asyncio
import logging
from enum import Enum
from itertools import count
from typing import Dict, List, Iterator, Type
from uuid import uuid4

from aioflow.helpers import load_config, merge_dict
from aioflow.middlewareabc import MiddlewareABC
from aioflow.service import Service, ServiceStatus

__author__ = 'a.lemets'

logger = logging.getLogger(__name__)


class AioFlowRuntimeError(RuntimeError):
    ...


class AioFlowKeyError(KeyError):
    ...


class PipelineStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class Pipeline:
    @classmethod
    async def create(cls,
                     name: str,
                     *,
                     config: Dict or str = None,
                     middleware: List[MiddlewareABC] or MiddlewareABC = None) -> "Pipeline":
        self = cls(name, config=config, middleware=middleware)
        await self._call_middleware("pipeline_create", self)
        return self

    def __init__(self,
                 name: str,
                 *,
                 config: Dict or str = None,
                 middleware: List[MiddlewareABC] or MiddlewareABC = None):
        """

        :param name: name of pipeline
        :param config: path or config object
        :param middleware: list of middleware
        """
        self.name = name
        self.config = config
        self._id = None
        if isinstance(middleware, MiddlewareABC):
            middleware = [middleware]
        self._middleware = middleware or []
        self._services = {}
        self._depends_on = {}

    @property
    def id(self):
        if not self._id:
            self._id = str(uuid4())
        return self._id

    @property
    def config(self) -> Dict:
        return self._config

    @config.setter
    def config(self, value: Dict or None):
        """
        Set config file
        :param value:
        :return:
        """
        if value is None:
            self._config = {}
        elif isinstance(value, Dict):
            self._config = value
        elif isinstance(value, str):
            self._config = load_config(value)
        else:
            raise AioFlowRuntimeError("Bad value for config")

    def update_config(self, dct: Dict):
        merge_dict(self._config, dct)

    async def _call_middleware(self, func, *args, **kwargs):
        kwargs.update(self.config.get("__{}_kwargs".format(func), {}))
        for m in self._middleware:
            await getattr(m, func)(*args, **kwargs)

    async def message(self, **kwargs) -> None:
        """
        Try call message callback

        :param kwargs:
        :return:
        """
        return await self._call_middleware("pipeline_message", self, **kwargs)

    async def register(self, service_cls: Type[Service], *, depends_on: Dict = None):
        """
        Register new service in pipeline

        :param service_cls: cls of service
        :param depends_on: dependence from another servic
        Ex.:
        depends_on = {
            service_cls1: "res.key1",
            service_cls2: ["key1", "res.key_another"]
        }

        kwargs for service: **{
            f"{service_cls1.name}.res.key1": value1,
            f"{service_cls2.name}.key1": value2,
            f"{service_cls2.name}.res.key_another": value3,
        }

        :return: None
        """
        service = service_cls(self)
        await self._call_middleware("service_create", service)
        self._register_service(service, depends_on)
        return self

    def _register_service(self, service: Service, depends_on: Dict = None) -> None:
        logger.debug(f"Try register {service.name}")

        # transform
        # {service_cls: [str, ...]} into {service: [str, ...]}
        # {service_cls: str} into {service: [str]}
        transformed_depends_on = {}
        if depends_on:
            for srv_cls in depends_on:
                # isinstance(srv_cls, type) - check srv_cls is class
                if not isinstance(srv_cls, type) or not issubclass(srv_cls, Service):
                    msg = "Use {service_cls: str} or {service_cls: [str, str, ...]} format for depends_on"
                    raise AioFlowRuntimeError(msg)

                for srv in self.services:
                    if isinstance(srv, srv_cls):
                        if isinstance(depends_on[srv_cls], str):
                            transformed_depends_on[srv] = [depends_on[srv_cls]]
                        else:
                            transformed_depends_on[srv] = depends_on[srv_cls]
                        break
                else:
                    raise AioFlowRuntimeError(f"Service {srv_cls} not registered")

        logger.debug(f"Service {service.name} needs result {depends_on}")
        self._services[service.id] = service
        self._depends_on[service.id] = transformed_depends_on

    @property
    def services(self):
        return self._services.values()

    def ready_services(self) -> Iterator[List]:
        """
        Generator for getting services for running

        :yield: List of service
        """
        dependencies = {}
        for service_id in self._depends_on:
            dependencies[service_id] = {service.id for service in self._depends_on[service_id]}

        service_number = count(start=1)
        already_done = set()
        while True:
            scheduled = set()
            scheduled_tasks = []
            for service_id in self._depends_on:
                if service_id not in already_done and dependencies[service_id] <= already_done:
                    scheduled.add(service_id)

                    task = self.service_wrapper(service_id, next(service_number))
                    scheduled_tasks.append(task)

            if not scheduled_tasks:
                break

            yield scheduled_tasks
            already_done |= scheduled

    def build_service_kwargs(self, service: Service, service_number: int) -> dict:
        logger.debug(f"Building service [{service.name}] kwargs")
        kwargs = {"__service_number": service_number}
        kwargs.update(service.config.get("__kwargs", {}))  # maybe service.__init__ with kwargs?

        for srv in self._depends_on[service.id]:
            keys = self._depends_on[service.id][srv]
            for key in keys:
                res = srv.result

                try:
                    for k in key.split("."):
                        res = res[k]
                except KeyError:
                    logger.error(f"Key {key} not found in {srv.name}")
                    raise AioFlowKeyError(f"{key} not found in result of {srv.name}")
                kwargs[f"{srv.name}.{key}"] = res

        return kwargs

    async def service_wrapper(self, service_id: int, service_number):
        service = self._services[service_id]
        kwargs = self.build_service_kwargs(service, service_number)

        logger.debug(f"Start [{service.name}] payload with {kwargs}")

        service.status = ServiceStatus.PROCESSING
        service.number = kwargs.pop("__service_number", None)
        await self._call_middleware("service_start", service)
        try:
            result = await asyncio.wait_for(service(**kwargs), timeout=service.timeout)
        except asyncio.TimeoutError as exp:
            logger.error(f"Timeout [{service.name}]")
            service.status = ServiceStatus.FAILED
            await self._call_middleware("service_failed", service, exp)
            if not service.allow_failure:
                raise
        except Exception as exp:
            logger.exception(f"Failed [{service.name}]")
            service.status = ServiceStatus.FAILED
            await self._call_middleware("service_failed", service, exp)
            if not service.allow_failure:
                raise
        else:
            logger.debug(f"Success [{service.name}] with {result}")
            service.result = result
            await self._call_middleware("service_done", service)
            return result

    async def run(self):
        await self._call_middleware("pipeline_start", self)

        for services in self.ready_services():
            try:
                await asyncio.gather(*services, )
            except Exception as exp:
                await self._call_middleware("pipeline_failed", self, exp)
                raise

        await self._call_middleware("pipeline_done", self)
