import asyncio
import logging
from itertools import count
from typing import Dict, Callable, List, Iterator, Type
from uuid import uuid4

from aioflow.helpers import try_call, load_config, merge_dict
from aioflow.service import Service, ServiceStatus

__author__ = 'a.lemets'

logger = logging.getLogger(__name__)


class AioFlowRuntimeError(RuntimeError):
    ...


class AioFlowKeyError(KeyError):
    ...


class Pipeline:
    def __init__(self,
                 name: str,
                 *,
                 config: Dict or str = None,
                 pre_callback: Callable = None,
                 post_callback: Callable = None,
                 on_message: Callable = None):
        """

        :param name: name of pipeline
        :param config: path or config object
        :param pre_callback: call, when pipeline is started
        :param post_callback: call, when pipeline is ended
        :param on_message: callback for messages (call from service)
        """
        self.name = name
        self.config = config
        self._uuid = str(uuid4())
        self._pre_callback = pre_callback
        self._post_callback = post_callback
        self._on_message = on_message
        self._services = {}
        self._depends_on = {}

    @property
    def id(self):
        return self._uuid

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

    async def message(self, **kwargs) -> None:
        """
        Try call message callback

        :param kwargs:
        :return:
        """
        return await try_call(self._on_message, **kwargs)

    async def pre_callback(self, **kwargs) -> None:
        """
        Try call pipeline callback

        :param kwargs:
        :return:
        """

        logger.debug("Trying call pre_callback")
        return await try_call(self._pre_callback, **kwargs)

    async def post_callback(self, **kwargs) -> None:
        """
        Try call pipeline callback

        :param kwargs:
        :return:
        """

        logger.debug("Trying call post_callback")
        return await try_call(self._post_callback, **kwargs)

    def register(self, service_cls: Type[Service], *, depends_on: Dict = None):
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

                    task = self.prepare_service(service_id, next(service_number))
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

    async def prepare_service(self, service_id: int, service_number):
        service = self._services[service_id]
        kwargs = self.build_service_kwargs(service, service_number)

        logger.debug(f"Start [{service.name}] payload with {kwargs}")

        service.status = ServiceStatus.PROCESSING
        service.number = kwargs.pop("__service_number", None)
        try:
            result = await asyncio.wait_for(service(**kwargs), timeout=service.timeout)
        except asyncio.TimeoutError:
            logger.error(f"Timeout [{service.name}]")
            service.status = ServiceStatus.FAILED
            if not service.allow_failure:
                raise
        except Exception:
            logger.exception(f"Failed [{service.name}]")
            service.status = ServiceStatus.FAILED
            if not service.allow_failure:
                raise
        else:
            logger.debug(f"Success [{service.name}] with {result}")
            service.result = result
            return result

    async def run(self):
        await self.pre_callback(**self.config.get("__pre_callback_kwargs", {}))
        for services in self.ready_services():
            await asyncio.gather(*services, )
        await self.post_callback(**self.config.get("__post_callback_kwargs", {}))
